from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from functools import cache
from typing import TYPE_CHECKING, Any, cast

import lance

import daft
import daft.pickle
from daft.datatype import DataType
from daft.dependencies import pa
from daft.udf import method
from daft_lance._blob import is_blob_v2_field

if TYPE_CHECKING:
    from daft_lance.namespace import DatasetOpenContext


_ROW_ADDRESS = "_rowaddr"
_FRAGMENT_ID = "fragment_id"
_METADATA_COLUMNS = {_ROW_ADDRESS, "_rowid", _FRAGMENT_ID}
_FRAGMENT_UPDATE_RESULT_DTYPE = DataType.struct(
    {
        "fragment_meta": DataType.binary(),
        "rows_updated": DataType.int64(),
    }
)


@dataclass(frozen=True)
class UpdateColumnsResult:
    """Result of a distributed Lance column update."""

    version: int
    rows_updated: int


@dataclass(frozen=True)
class _FragmentUpdateBatch:
    """Arrow inputs for one fragment update."""

    fragment_id: int
    row_addresses: pa.Array[Any]
    values: pa.Table


def _is_blob_field(field: pa.Field[Any]) -> bool:
    if is_blob_v2_field(field):
        return True
    metadata = field.metadata or {}
    return metadata.get(b"lance-encoding:blob") == b"true"


def _leaf_field_ids(field: Any) -> list[int]:
    children = field.children()
    if not children:
        return [field.id()]
    field_ids: list[int] = []
    for child in children:
        field_ids.extend(_leaf_field_ids(child))
    return field_ids


def _validate_update_columns(
    df: daft.DataFrame,
    lance_ds: lance.LanceDataset,
    columns: Sequence[str],
) -> tuple[list[str], list[int]]:
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
        if name in _METADATA_COLUMNS:
            raise ValueError(f"Cannot update metadata column {name!r}.")
        if "." in name:
            raise ValueError(f"Nested field path {name!r} is not supported; only top-level columns can be updated.")

    target_names = set(lance_ds.schema.names)
    field_ids: list[int] = []
    for name in resolved_columns:
        if name not in target_names:
            raise ValueError(
                f"Cannot update non-existent column {name!r}; update_columns_df only overwrites existing columns."
            )
        arrow_field = lance_ds.schema.field(name)
        if arrow_field is None:
            raise ValueError(f"Column {name!r} has no Arrow field in the target schema.")
        if pa.types.is_struct(arrow_field.type):
            raise ValueError(f"Struct column {name!r} is not supported by update_columns_df.")
        if _is_blob_field(arrow_field):
            raise ValueError(f"Blob column {name!r} cannot be updated by update_columns_df.")

        lance_field = lance_ds.lance_schema.field(name)  # type: ignore[attr-defined]
        if lance_field is None:
            raise ValueError(f"Column {name!r} has no Lance field id.")
        field_ids.extend(_leaf_field_ids(lance_field))

    source_names = df.column_names
    for required in [_ROW_ADDRESS, _FRAGMENT_ID, *resolved_columns]:
        count = source_names.count(required)
        if count == 0:
            raise ValueError(f"DataFrame must contain column {required!r}.")
        if count > 1:
            raise ValueError(f"DataFrame column {required!r} is ambiguous because it appears {count} times.")

    return resolved_columns, sorted(field_ids)


def _to_arrow_array(series: Any) -> pa.Array[Any]:
    from daft.dependencies import pa

    array = series.to_arrow()
    if isinstance(array, pa.ChunkedArray):
        return array.combine_chunks()
    return cast("pa.Array[Any]", array)


def _prepare_fragment_update(columns: list[str], series: tuple[Any, ...]) -> _FragmentUpdateBatch:
    """Validate and convert one Daft fragment group into Arrow inputs."""
    *update_series, row_address_series, fragment_id_series = series
    fragment_id_scalar = _to_arrow_array(fragment_id_series)[0]
    if not fragment_id_scalar.is_valid:
        raise ValueError("fragment_id cannot contain nulls.")
    fragment_id = int(fragment_id_scalar.cast(pa.int64(), safe=True).as_py())

    row_addresses = _to_arrow_array(row_address_series)
    if row_addresses.null_count:
        raise ValueError("_rowaddr cannot contain nulls.")
    row_addresses = row_addresses.cast(pa.uint64(), safe=True)
    if pa.compute.count_distinct(row_addresses).as_py() != len(row_addresses):
        raise ValueError(f"Duplicate _rowaddr values found for fragment {fragment_id}.")

    values = pa.Table.from_arrays([_to_arrow_array(value) for value in update_series], names=columns)
    return _FragmentUpdateBatch(fragment_id, row_addresses, values)


def _validate_live_row_addresses(
    fragment: Any,
    row_addresses: pa.Array[Any],
    *,
    fragment_id: int,
    version: int,
) -> None:
    """Require every requested address to identify a live row in the pinned fragment."""
    live_row_addresses = (
        fragment.scanner(columns=[], with_row_address=True).to_table().column(_ROW_ADDRESS).combine_chunks()
    )
    addresses_are_live = pa.compute.is_in(row_addresses, value_set=live_row_addresses)
    if bool(pa.compute.all(addresses_are_live).as_py()):
        return

    invalid = row_addresses.filter(pa.compute.invert(addresses_are_live)).to_pylist()
    preview = invalid[:10]
    suffix = "..." if len(invalid) > len(preview) else ""
    raise ValueError(
        f"Source contains _rowaddr values that are not live rows in fragment {fragment_id} "
        f"at version {version}: {preview}{suffix}"
    )


def _rewrite_fragment(
    lance_ds: lance.LanceDataset,
    batch: _FragmentUpdateBatch,
    *,
    columns: list[str],
) -> dict[str, Any]:
    """Validate and rewrite one fragment, returning a minimal driver commit message."""
    fragment = lance_ds.get_fragment(batch.fragment_id)
    if fragment is None:
        raise ValueError(f"Fragment {batch.fragment_id} does not exist in target snapshot version {lance_ds.version}.")

    _validate_live_row_addresses(
        fragment,
        batch.row_addresses,
        fragment_id=batch.fragment_id,
        version=lance_ds.version,
    )

    target_schema = pa.schema([lance_ds.schema.field(name) for name in columns])
    for field in target_schema:
        if not field.nullable and batch.values.column(field.name).null_count:
            raise ValueError(f"Update produced nulls for non-nullable column {field.name!r}.")
    try:
        values = batch.values.cast(target_schema, safe=True)
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError) as exc:
        raise ValueError(f"Update columns cannot be safely cast to the target Lance schema: {exc}") from exc

    update_table = values.append_column(_ROW_ADDRESS, batch.row_addresses)
    fragment_meta, _ = fragment.update_columns(
        update_table,
        left_on=_ROW_ADDRESS,
        right_on=_ROW_ADDRESS,
    )
    if int(fragment_meta.id) != batch.fragment_id:
        raise ValueError(f"Fragment rewrite changed fragment id: expected {batch.fragment_id}, got {fragment_meta.id}.")

    return {
        "fragment_meta": daft.pickle.dumps(fragment_meta),
        "rows_updated": len(batch.row_addresses),
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


@cache
def _fragment_update_handler_cls(max_concurrency: int | None) -> type:
    """Create each resource-configured Daft class once per process."""
    return daft.cls(_FragmentUpdateHandler, max_concurrency=max_concurrency)


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
    if max_concurrency is not None and max_concurrency <= 0:
        raise ValueError("max_concurrency must be a positive integer.")

    resolved_columns, expected_field_ids = _validate_update_columns(df, lance_ds, columns)
    source = df.select(*resolved_columns, _ROW_ADDRESS, _FRAGMENT_ID)

    handler_cls = _fragment_update_handler_cls(max_concurrency)
    handler = handler_cls(open_context, resolved_columns)
    grouped = source.groupby(_FRAGMENT_ID).map_groups(
        handler(
            *(source[name] for name in resolved_columns),
            source[_ROW_ADDRESS],
            source[_FRAGMENT_ID],
        ).alias("commit_message")
    )
    commit_messages = grouped.collect().to_pydict()["commit_message"]
    if not commit_messages:
        return UpdateColumnsResult(version=lance_ds.version, rows_updated=0)

    updated_fragments = []
    seen_fragment_ids: set[int] = set()
    rows_updated = 0
    for message in commit_messages:
        fragment_meta = daft.pickle.loads(message["fragment_meta"])
        fragment_id = int(fragment_meta.id)
        if fragment_id in seen_fragment_ids:
            raise ValueError(f"Duplicate update result for fragment {fragment_id}.")
        seen_fragment_ids.add(fragment_id)

        updated_fragments.append(fragment_meta)
        rows_updated += int(message["rows_updated"])

    operation = lance.LanceOperation.Update(
        updated_fragments=updated_fragments,
        fields_modified=expected_field_ids,
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
