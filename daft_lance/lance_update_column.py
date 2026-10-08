from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

import lance

import daft
import daft.pickle
from daft.datatype import DataType
from daft.dependencies import pa
from daft.runners import get_or_create_runner
from daft.udf import method
from daft_lance._metadata import _is_lance_blob

if TYPE_CHECKING:
    from daft_lance.namespace import DatasetOpenContext


UpdateTransform = dict[str, str] | lance.udf.BatchUDF | Callable[[pa.RecordBatch], pa.RecordBatch]


_ROW_ADDRESS = "_rowaddr"
_FRAGMENT_ID = "fragment_id"
_METADATA_COLUMNS = {_ROW_ADDRESS, "_rowid", _FRAGMENT_ID}
_FRAGMENT_UPDATE_RESULT_DTYPE = DataType.struct(
    {
        "fragment_meta": DataType.binary(),
        "rows_updated": DataType.int64(),
        "fields_modified": DataType.list(DataType.int64()),
    }
)


@dataclass(frozen=True)
class UpdateColumnsResult:
    """Result of a distributed Lance column update.

    For ``update_columns``, ``rows_updated`` is the number of live rows matching
    its predicate. For ``update_columns_df``, it counts the rows submitted by the
    source, which can differ from the number Lance matches when addresses are
    stale or repeated.
    """

    version: int
    rows_updated: int


@dataclass(frozen=True)
class _FragmentUpdateBatch:
    """Arrow inputs for one fragment update."""

    fragment_id: int
    row_addresses: pa.Array[Any]
    values: pa.Table


def validate_update_arguments(columns: Sequence[str], max_concurrency: int | None) -> list[str]:
    """Check the arguments that do not depend on the target dataset.

    Called before the dataset is opened so a bad argument does not first cost a
    namespace round trip and a manifest read.
    """
    if max_concurrency is not None and max_concurrency <= 0:
        raise ValueError("max_concurrency must be a positive integer.")

    if isinstance(columns, str):
        raise TypeError(f"'columns' must be a sequence of column names, not a bare string. Did you mean ['{columns}']?")

    resolved_columns = list(columns)
    if not resolved_columns:
        raise ValueError("'columns' must name at least one existing column to update.")

    seen: set[str] = set()
    for name in resolved_columns:
        if not isinstance(name, str):
            raise TypeError(f"'columns' entries must be strings, got {type(name).__name__}.")
        if name in seen:
            raise ValueError(f"Duplicate column {name!r} in 'columns'.")
        seen.add(name)
        if name == _FRAGMENT_ID:
            raise ValueError(
                f"Cannot update {name!r}; it is the grouping key update_columns_df injects, "
                "not a column of the target dataset."
            )
        if name in _METADATA_COLUMNS:
            raise ValueError(f"Cannot update metadata column {name!r}.")
        if "." in name:
            raise ValueError(f"Nested field path {name!r} is not supported; only top-level columns can be updated.")

    return resolved_columns


def _validate_update_target_columns(
    lance_ds: lance.LanceDataset,
    columns: Sequence[str],
    *,
    operation_name: str,
) -> list[str]:
    resolved_columns = validate_update_arguments(columns, None)
    target_names = set(lance_ds.schema.names)
    for name in resolved_columns:
        if name not in target_names:
            raise ValueError(
                f"Cannot update non-existent column {name!r}; {operation_name} only overwrites existing columns."
            )
        arrow_field = lance_ds.schema.field(name)
        if pa.types.is_struct(arrow_field.type):
            raise ValueError(f"Struct column {name!r} is not supported by {operation_name}.")
        if _is_lance_blob(arrow_field):
            raise ValueError(f"Blob column {name!r} cannot be updated by {operation_name}.")
    return resolved_columns


def _validate_update_columns(
    df: daft.DataFrame,
    lance_ds: lance.LanceDataset,
    columns: Sequence[str],
    max_concurrency: int | None = None,
) -> list[str]:
    validate_update_arguments(columns, max_concurrency)
    resolved_columns = _validate_update_target_columns(lance_ds, columns, operation_name="update_columns_df")

    source_names = df.column_names
    for required in [_ROW_ADDRESS, _FRAGMENT_ID, *resolved_columns]:
        count = source_names.count(required)
        if count == 0:
            raise ValueError(f"DataFrame must contain column {required!r}.")
        if count > 1:
            raise ValueError(f"DataFrame column {required!r} is ambiguous because it appears {count} times.")

    return resolved_columns


def _to_arrow_array(series: Any) -> pa.Array[Any]:
    array = series.to_arrow()
    if isinstance(array, pa.ChunkedArray):
        return array.combine_chunks()
    return cast("pa.Array[Any]", array)


def _prepare_fragment_update(columns: list[str], series: tuple[Any, ...]) -> _FragmentUpdateBatch:
    """Convert one Daft fragment group into Arrow inputs.

    Only nulls are rejected. Repeated addresses are left to Lance, which picks
    one of the matching rows without specifying which.
    """
    *update_series, row_address_series, fragment_id_series = series
    fragment_id_scalar = _to_arrow_array(fragment_id_series)[0]
    if not fragment_id_scalar.is_valid:
        raise ValueError("fragment_id cannot contain nulls.")
    fragment_id = int(fragment_id_scalar.cast(pa.int64(), safe=True).as_py())

    row_addresses = _to_arrow_array(row_address_series)
    if row_addresses.null_count:
        raise ValueError("_rowaddr cannot contain nulls.")
    row_addresses = row_addresses.cast(pa.uint64(), safe=True)

    values = pa.Table.from_arrays([_to_arrow_array(value) for value in update_series], names=columns)
    return _FragmentUpdateBatch(fragment_id, row_addresses, values)


def _rewrite_fragment(
    lance_ds: lance.LanceDataset,
    batch: _FragmentUpdateBatch,
    *,
    columns: list[str],
) -> dict[str, Any]:
    """Rewrite one fragment, returning a minimal driver commit message.

    Row addresses are not checked against the fragment. ``update_columns`` is a
    left-outer join, so an address that does not identify a live row of this
    fragment updates nothing and raises nothing.
    """
    fragment = lance_ds.get_fragment(batch.fragment_id)
    if fragment is None:
        raise ValueError(f"Fragment {batch.fragment_id} does not exist in target snapshot version {lance_ds.version}.")

    target_schema = pa.schema([lance_ds.schema.field(name) for name in columns])
    for field in target_schema:
        if not field.nullable and batch.values.column(field.name).null_count:
            raise ValueError(f"Update produced nulls for non-nullable column {field.name!r}.")
    try:
        values = batch.values.cast(target_schema, safe=True)
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError) as exc:
        raise ValueError(f"Update columns cannot be safely cast to the target Lance schema: {exc}") from exc

    update_table = values.append_column(_ROW_ADDRESS, batch.row_addresses)
    fragment_meta, fields_modified = fragment.update_columns(
        update_table,
        left_on=_ROW_ADDRESS,
        right_on=_ROW_ADDRESS,
    )
    if int(fragment_meta.id) != batch.fragment_id:
        raise ValueError(f"Fragment rewrite changed fragment id: expected {batch.fragment_id}, got {fragment_meta.id}.")

    return {
        "fragment_meta": daft.pickle.dumps(fragment_meta),
        "rows_updated": len(batch.row_addresses),
        "fields_modified": [int(field_id) for field_id in fields_modified],
    }


class _FragmentUpdateHandler:
    """Rewrite existing columns for one pinned Lance fragment."""

    def __init__(
        self,
        open_context: DatasetOpenContext,
        update_columns: list[str],
    ) -> None:
        self.open_context = open_context
        self.update_columns = update_columns
        self._lance_ds: lance.LanceDataset | None = None

    def _dataset(self) -> lance.LanceDataset:
        if self._lance_ds is None:
            self._lance_ds = self.open_context.open_pinned()
        return self._lance_ds

    @method.batch(return_dtype=_FRAGMENT_UPDATE_RESULT_DTYPE)
    def __call__(self, *series: Any) -> list[dict[str, Any]]:
        if not series or len(series[0]) == 0:
            return []

        batch = _prepare_fragment_update(self.update_columns, series)
        return [
            _rewrite_fragment(
                self._dataset(),
                batch,
                columns=self.update_columns,
            )
        ]


def _resolve_transform_columns(
    transform: UpdateTransform,
    columns: Sequence[str] | None,
    max_concurrency: int | None,
) -> list[str]:
    if max_concurrency is not None and max_concurrency <= 0:
        raise ValueError("max_concurrency must be a positive integer.")

    inferred_columns: Sequence[str] | None = None
    if isinstance(transform, dict):
        for name, expression in transform.items():
            if not isinstance(name, str):
                raise TypeError(f"Transform column names must be strings, got {type(name).__name__}.")
            if not isinstance(expression, str):
                raise TypeError(f"Transform expressions must be strings, got {type(expression).__name__}.")
        inferred_columns = list(transform)
    elif isinstance(transform, lance.udf.BatchUDF):
        if transform.cache is not None:
            raise ValueError("BatchUDF checkpoint files are not supported by update_columns fragment workers.")
        if transform.output_schema is not None:
            inferred_columns = transform.output_schema.names
    elif not callable(transform):
        raise TypeError("'transform' must be a dict of Lance SQL expressions, a BatchUDF, or a callable.")

    if columns is None:
        if inferred_columns is None:
            raise ValueError("'columns' is required for a callable transform without an output schema.")
        return validate_update_arguments(inferred_columns, max_concurrency)

    resolved_columns = validate_update_arguments(columns, max_concurrency)
    if inferred_columns is not None and set(resolved_columns) != set(inferred_columns):
        raise ValueError("'columns' must name exactly the columns produced by 'transform'.")
    return resolved_columns


def _validate_read_columns(lance_ds: lance.LanceDataset, read_columns: Sequence[str] | None) -> list[str] | None:
    if read_columns is None:
        return None
    if isinstance(read_columns, str):
        raise TypeError("'read_columns' must be a sequence of column names, not a bare string.")

    resolved = list(read_columns)
    seen: set[str] = set()
    valid_names = set(lance_ds.schema.names) | {_ROW_ADDRESS, "_rowid"}
    for name in resolved:
        if not isinstance(name, str):
            raise TypeError(f"'read_columns' entries must be strings, got {type(name).__name__}.")
        if name in seen:
            raise ValueError(f"Duplicate column {name!r} in 'read_columns'.")
        if name not in valid_names:
            raise ValueError(f"Read column {name!r} does not exist in the target dataset.")
        seen.add(name)
    return resolved


def validate_transform_update_arguments(
    lance_ds: lance.LanceDataset,
    transform: UpdateTransform,
    *,
    columns: Sequence[str] | None,
    read_columns: Sequence[str] | None,
    where: str | None,
    batch_size: int | None,
    max_concurrency: int | None,
) -> tuple[list[str], list[str] | None, str | None]:
    """Validate a transform-driven update before any fragment writes."""
    if batch_size is not None and batch_size <= 0:
        raise ValueError("batch_size must be a positive integer.")
    resolved_columns = _resolve_transform_columns(transform, columns, max_concurrency)
    resolved_columns = _validate_update_target_columns(lance_ds, resolved_columns, operation_name="update_columns")
    resolved_read_columns = _validate_read_columns(lance_ds, read_columns)

    if where is not None:
        if not isinstance(where, str):
            raise TypeError("'where' must be a Lance SQL predicate string or None.")
        where = where.strip()
        if not where:
            raise ValueError("'where' must be a non-empty Lance SQL predicate when provided.")

    projection: dict[str, str] | list[str]
    if isinstance(transform, dict):
        projection = {name: transform[name] for name in resolved_columns}
    else:
        projection = []
    try:
        lance_ds.scanner(columns=projection, filter=where, limit=1).explain_plan(True)
    except Exception as exc:
        raise ValueError(f"Transform or where predicate is not valid for the target Lance dataset: {exc}") from exc

    return resolved_columns, resolved_read_columns, where


def _cast_transform_output(
    output: pa.RecordBatch,
    row_addresses: pa.Array[Any],
    *,
    columns: list[str],
    target_schema: pa.Schema,
    expected_rows: int,
) -> pa.RecordBatch:
    if not isinstance(output, pa.RecordBatch):
        raise TypeError(f"Transform must return a pyarrow.RecordBatch, got {type(output).__name__}.")
    if output.num_rows != expected_rows:
        raise ValueError(
            f"Transform changed the row count from {expected_rows} to {output.num_rows}; row-preserving output is required."
        )
    if len(set(output.schema.names)) != len(output.schema.names):
        raise ValueError("Transform output contains duplicate column names.")
    if set(output.schema.names) != set(columns):
        raise ValueError(f"Transform must return exactly the update columns {columns!r}, got {output.schema.names!r}.")

    arrays: list[pa.Array[Any]] = []
    try:
        for field in target_schema:
            array = output.column(output.schema.get_field_index(field.name))
            if not field.nullable and array.null_count:
                raise ValueError(f"Update produced nulls for non-nullable column {field.name!r}.")
            arrays.append(array.cast(field.type, safe=True))
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError) as exc:
        raise ValueError(f"Update columns cannot be safely cast to the target Lance schema: {exc}") from exc

    output_schema = pa.schema([*target_schema, pa.field(_ROW_ADDRESS, pa.uint64(), nullable=False)])
    return pa.RecordBatch.from_arrays([*arrays, row_addresses.cast(pa.uint64(), safe=True)], schema=output_schema)


def _transform_fragment(
    lance_ds: lance.LanceDataset,
    fragment_id: int,
    *,
    transform: UpdateTransform,
    columns: list[str],
    read_columns: list[str] | None,
    where: str | None,
    batch_size: int | None,
) -> dict[str, Any]:
    fragment = lance_ds.get_fragment(fragment_id)
    if fragment is None:
        raise ValueError(f"Fragment {fragment_id} does not exist in target snapshot version {lance_ds.version}.")

    target_schema = pa.schema([lance_ds.schema.field(name) for name in columns])
    if isinstance(transform, dict):
        source_batches = fragment.to_batches(
            columns={name: transform[name] for name in columns},
            filter=where,
            with_row_address=True,
            batch_size=batch_size,
        )

        def transformed_batches() -> Iterator[pa.RecordBatch]:
            for batch in source_batches:
                row_addresses = batch.column(batch.schema.get_field_index(_ROW_ADDRESS))
                output = batch.select(columns)
                yield _cast_transform_output(
                    output,
                    row_addresses,
                    columns=columns,
                    target_schema=target_schema,
                    expected_rows=batch.num_rows,
                )

    else:
        requested = list(lance_ds.schema.names) if read_columns is None else read_columns
        physical_columns = [name for name in requested if name not in {_ROW_ADDRESS, "_rowid"}]
        source_batches = fragment.to_batches(
            columns=physical_columns,
            filter=where,
            with_row_address=True,
            with_row_id="_rowid" in requested,
            batch_size=batch_size,
        )

        def transformed_batches() -> Iterator[pa.RecordBatch]:
            for batch in source_batches:
                row_addresses = batch.column(batch.schema.get_field_index(_ROW_ADDRESS))
                transform_input = batch.select(requested)
                output = transform(transform_input)
                yield _cast_transform_output(
                    output,
                    row_addresses,
                    columns=columns,
                    target_schema=target_schema,
                    expected_rows=batch.num_rows,
                )

    iterator = iter(transformed_batches())
    first = next(iterator, None)
    if first is None:
        return {"fragment_meta": None, "rows_updated": 0, "fields_modified": []}

    rows_updated = first.num_rows

    def counted_batches() -> Iterator[pa.RecordBatch]:
        nonlocal rows_updated
        yield first
        for batch in iterator:
            rows_updated += batch.num_rows
            yield batch

    reader = pa.RecordBatchReader.from_batches(first.schema, counted_batches())
    fragment_meta, fields_modified = fragment.update_columns(
        reader,
        left_on=_ROW_ADDRESS,
        right_on=_ROW_ADDRESS,
    )
    if int(fragment_meta.id) != fragment_id:
        raise ValueError(f"Fragment rewrite changed fragment id: expected {fragment_id}, got {fragment_meta.id}.")
    return {
        "fragment_meta": daft.pickle.dumps(fragment_meta),
        "rows_updated": rows_updated,
        "fields_modified": [int(field_id) for field_id in fields_modified],
    }


class _FragmentTransformUpdateHandler:
    """Apply a row-preserving transform to matching rows in pinned fragments."""

    def __init__(
        self,
        open_context: DatasetOpenContext,
        transform: UpdateTransform,
        columns: list[str],
        read_columns: list[str] | None,
        where: str | None,
        batch_size: int | None,
    ) -> None:
        self.open_context = open_context
        self.transform = transform
        self.columns = columns
        self.read_columns = read_columns
        self.where = where
        self.batch_size = batch_size
        self._lance_ds: lance.LanceDataset | None = None

    def _dataset(self) -> lance.LanceDataset:
        if self._lance_ds is None:
            self._lance_ds = self.open_context.open_pinned()
        return self._lance_ds

    @method.batch(return_dtype=_FRAGMENT_UPDATE_RESULT_DTYPE)
    def __call__(self, fragment_ids: Any) -> list[dict[str, Any]]:
        results = []
        for scalar in _to_arrow_array(fragment_ids):
            if not scalar.is_valid:
                raise ValueError("fragment_id cannot be null.")
            fragment_id = int(scalar.cast(pa.int64(), safe=True).as_py())
            results.append(
                _transform_fragment(
                    self._dataset(),
                    fragment_id,
                    transform=self.transform,
                    columns=self.columns,
                    read_columns=self.read_columns,
                    where=self.where,
                    batch_size=self.batch_size,
                )
            )
        return results


def _commit_update_messages(
    commit_messages: list[dict[str, Any]],
    lance_ds: lance.LanceDataset,
    open_context: DatasetOpenContext,
    *,
    commit_lock: Any | None,
) -> UpdateColumnsResult:
    commit_messages = [message for message in commit_messages if message["fragment_meta"] is not None]
    if not commit_messages:
        return UpdateColumnsResult(version=lance_ds.version, rows_updated=0)

    updated_fragments = []
    seen_fragment_ids: set[int] = set()
    fields_modified: set[int] = set()
    rows_updated = 0
    for message in commit_messages:
        fragment_meta = daft.pickle.loads(message["fragment_meta"])
        fragment_id = int(fragment_meta.id)
        if fragment_id in seen_fragment_ids:
            raise ValueError(f"Duplicate update result for fragment {fragment_id}.")
        seen_fragment_ids.add(fragment_id)
        updated_fragments.append(fragment_meta)
        fields_modified.update(int(field_id) for field_id in message["fields_modified"])
        rows_updated += int(message["rows_updated"])

    operation = lance.LanceOperation.Update(
        updated_fragments=updated_fragments,
        fields_modified=sorted(fields_modified),
        fields_for_preserving_frag_bitmap=[],
        update_mode="rewrite_columns",
    )
    committed = lance.LanceDataset.commit(
        open_context.uri,
        operation,
        read_version=lance_ds.version,
        commit_lock=commit_lock,
        storage_options=open_context.storage_options,
        **open_context.commit_kwargs,
    )
    return UpdateColumnsResult(version=committed.version, rows_updated=rows_updated)


def update_columns_with_transform(
    lance_ds: lance.LanceDataset,
    open_context: DatasetOpenContext,
    *,
    transform: UpdateTransform,
    columns: Sequence[str] | None,
    read_columns: Sequence[str] | None,
    where: str | None,
    batch_size: int | None,
    commit_lock: Any | None,
    max_concurrency: int | None,
) -> UpdateColumnsResult:
    resolved_columns, resolved_read_columns, resolved_where = validate_transform_update_arguments(
        lance_ds,
        transform,
        columns=columns,
        read_columns=read_columns,
        where=where,
        batch_size=batch_size,
        max_concurrency=max_concurrency,
    )
    fragment_ids = [fragment.fragment_id for fragment in lance_ds.get_fragments()]
    if not fragment_ids:
        return UpdateColumnsResult(version=lance_ds.version, rows_updated=0)

    # The control DataFrame contains only fragment ids. Split it without a
    # hash shuffle so distributed runners can schedule fragments independently.
    source = daft.from_pydict({_FRAGMENT_ID: fragment_ids})
    if len(fragment_ids) > 1 and get_or_create_runner().name != "native":
        source = source.into_partitions(len(fragment_ids))
    handler_cls = daft.cls(_FragmentTransformUpdateHandler, max_concurrency=max_concurrency)
    handler = handler_cls(
        open_context,
        transform,
        resolved_columns,
        resolved_read_columns,
        resolved_where,
        batch_size,
    )
    results = source.with_column("commit_message", handler(source[_FRAGMENT_ID]))
    commit_messages = results.collect().to_pydict()["commit_message"]
    return _commit_update_messages(commit_messages, lance_ds, open_context, commit_lock=commit_lock)


def update_columns_from_df(
    df: daft.DataFrame,
    lance_ds: lance.LanceDataset,
    open_context: DatasetOpenContext,
    *,
    columns: Sequence[str],
    commit_lock: Any | None = None,
    max_concurrency: int | None = None,
) -> UpdateColumnsResult:
    """Execute a distributed, DataFrame-driven RewriteColumns transaction."""
    resolved_columns = _validate_update_columns(df, lance_ds, columns, max_concurrency)
    source = df.select(*resolved_columns, _ROW_ADDRESS, _FRAGMENT_ID)

    handler_cls = daft.cls(_FragmentUpdateHandler, max_concurrency=max_concurrency)
    handler = handler_cls(open_context, resolved_columns)
    grouped = source.groupby(_FRAGMENT_ID).map_groups(
        handler(
            *(source[name] for name in resolved_columns),
            source[_ROW_ADDRESS],
            source[_FRAGMENT_ID],
        ).alias("commit_message")
    )
    commit_messages = grouped.collect().to_pydict()["commit_message"]
    return _commit_update_messages(commit_messages, lance_ds, open_context, commit_lock=commit_lock)
