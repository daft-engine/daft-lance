from __future__ import annotations

import json
import os
import uuid
from typing import TYPE_CHECKING, Any

import lance
from lance.schema import LanceSchema

import daft.pickle
from daft import from_pylist
from daft.datatype import DataType
from daft.udf import cls as daft_cls
from daft.udf import method

from .namespace import namespace_kwargs_for_dataset

if TYPE_CHECKING:
    import pathlib
    from collections.abc import Callable

    from daft.dependencies import pa


_FRAGMENT_HANDLER_RETURN_DTYPE = DataType.struct({"fragment_meta": DataType.binary(), "schema": DataType.binary()})


@daft_cls
class FragmentHandler:
    def __init__(
        self,
        lance_ds: lance.LanceDataset,
        transform: dict[str, str] | lance.udf.BatchUDF | Callable[[pa.lib.RecordBatch], pa.lib.RecordBatch],
        read_columns: list[str] | None,
        reader_schema: pa.Schema | None = None,
    ):
        self.lance_ds = lance_ds
        self.transform = transform
        self.read_columns = read_columns
        self.reader_schema = reader_schema

    @method.batch(return_dtype=_FRAGMENT_HANDLER_RETURN_DTYPE)
    def __call__(self, fragment_ids: Any) -> list[dict[str, bytes]]:
        results = []
        for fragment_id in fragment_ids:
            fragment = self.lance_ds.get_fragment(fragment_id)
            if fragment is None:
                raise ValueError(f"Fragment {fragment_id} not found in dataset")
            fragment_meta, schema = fragment.merge_columns(self.transform, self.read_columns, None, self.reader_schema)
            results.append({"fragment_meta": daft.pickle.dumps(fragment_meta), "schema": daft.pickle.dumps(schema)})
        return results


def merge_columns_internal(
    lance_ds: lance.LanceDataset,
    url: str | pathlib.Path,
    *,
    transform: dict[str, str] | lance.udf.BatchUDF | Callable[[pa.RecordBatch], pa.RecordBatch],
    read_columns: list[str] | None = None,
    reader_schema: pa.Schema | None = None,
    storage_options: dict[str, Any] | None = None,
    daft_remote_args: dict[str, Any] | None = None,
    concurrency: int | None = None,
) -> lance.LanceDataset:
    # NOTE: Legacy remote args (num_cpus/num_gpus/memory_bytes/batch_size) were
    # only used for resource hints on the old @udf path. The new daft.cls
    # interface does not expose these; functional behavior does not depend on
    # them, so we ignore them here to keep the API simple.
    fragment_ids = [f.metadata.id for f in lance_ds.get_fragments()]
    fragment_data = [{"fragment_id": fid} for fid in fragment_ids]

    df = from_pylist(fragment_data)

    # Instantiate the Daft class with Lance-specific state and apply the
    # batch method over the fragment_id column.
    handler = FragmentHandler(lance_ds, transform, read_columns, reader_schema)
    df = df.with_column("commit_message", handler(df["fragment_id"]))  # type: ignore[arg-type]

    commit_messages = df.collect().to_pydict()["commit_message"]
    new_schema = None
    fragment_metas = []
    for commit_message in commit_messages:
        fragment_meta = commit_message["fragment_meta"]
        schema = commit_message["schema"]
        fragment_metas.append(daft.pickle.loads(fragment_meta))
        if new_schema is None:
            new_schema = daft.pickle.loads(schema)
            continue
    if new_schema is None:
        raise ValueError("No schema for new fragment found")
    op = lance.LanceOperation.Merge(fragment_metas, new_schema)
    return lance_ds.commit(
        url,
        op,
        read_version=lance_ds.version,
        storage_options=storage_options,
        **namespace_kwargs_for_dataset(lance_ds),
    )


@daft_cls
class GroupFragmentMergeUDF:
    def __init__(
        self,
        lance_ds: lance.LanceDataset,
        left_on: str | None = "_rowaddr",
        right_on: str | None = None,
        read_columns: list[str] | None = None,
        reader_schema: pa.Schema | None = None,
        batch_size: int | None = 9223372036854775807,
    ):
        """Per-group merge handler that directly invokes Lance fragment.merge with keyed join.

        Args:
            lance_ds: Target Lance dataset.
            left_on: Key column on the Lance fragment (default "_rowaddr").
            right_on: Key column name present in the provided reader data (defaults to left_on).
            read_columns: Names for columns provided to the handler via map_groups (must include right_on).
            reader_schema: Optional Arrow schema for the reader.
            batch_size: Optional batch size when building RecordBatchReader from the provided data.
        """
        self.lance_ds = lance_ds
        self.left_on = left_on or "_rowaddr"
        self.right_on = right_on or self.left_on
        self.read_columns = read_columns or []
        self.reader_schema = reader_schema
        self.batch_size = batch_size

    @method.batch(return_dtype=_FRAGMENT_HANDLER_RETURN_DTYPE)
    def __call__(self, *cols: Any) -> list[dict[str, bytes]]:
        from daft.dependencies import pa as _pa

        if len(cols) == 0:
            return []
        # Last argument is the fragment_id series, preceding args are data columns as per read_columns
        *data_cols, fragment_ids = cols
        ids = fragment_ids.to_pylist() if hasattr(fragment_ids, "to_pylist") else list(fragment_ids)
        if len(ids) == 0:
            return []
        frag_id = ids[0]

        if len(self.read_columns) != len(data_cols):
            raise ValueError(
                f"GroupFragmentMergeUDF expected {len(self.read_columns)} data columns, received {len(data_cols)}."
            )

        arrays: list[_pa.Array[Any]] = []

        for col_name, s in zip(self.read_columns, data_cols):
            pylist = s.to_pylist() if hasattr(s, "to_pylist") else list(s)

            if col_name == self.right_on:
                key_arr: _pa.Array[Any]
                if self.right_on == "_rowaddr":
                    key_arr = _pa.array(pylist, type=_pa.uint64())
                else:
                    pylist_int = [None if v is None else int(v) for v in pylist]
                    key_arr = _pa.array(pylist_int, type=_pa.int64())

                # Convert all arrays to a consistent type to avoid mypy errors
                arrays.append(key_arr.cast(_pa.int64()))
            else:
                arr = _pa.array(pylist)
                if _pa.types.is_floating(arr.type):
                    arrays.append(arr)
                elif _pa.types.is_integer(arr.type):
                    arrays.append(arr.cast(_pa.int64()))
                else:
                    arrays.append(arr)

        tbl = _pa.Table.from_arrays(arrays, names=self.read_columns)

        # Ensure the join key exists in the reader data
        if self.right_on not in tbl.schema.names:
            raise ValueError(
                f"Reader data missing join key '{self.right_on}'. Ensure the DataFrame includes this column (e.g., read with default_scan_options={'with_rowaddr': True} to expose '_rowaddr'). Hint: join key must be Int64; will be coerced automatically."
            )

        # After building the table, ensure the join key field is the correct type; cast if necessary
        join_idx = tbl.schema.get_field_index(self.right_on)
        if join_idx != -1:
            join_field = tbl.schema.field(join_idx)
            # Use appropriate type based on the join key name
            expected_type = _pa.uint64() if self.right_on == "_rowaddr" else _pa.int64()
            if join_field.type != expected_type:
                fields = []
                for i, name in enumerate(tbl.schema.names):
                    if name == self.right_on:
                        fields.append(_pa.field(name, expected_type))
                    else:
                        fields.append(tbl.schema.field(i))
                coerced_schema = _pa.schema(fields)
                tbl = tbl.cast(coerced_schema)

        # Enforce that reader stream contains only join key + new columns (exclude existing dataset fields)
        df_schema = tbl.schema
        existing_fields: set[str] = set()
        try:
            existing_fields = {getattr(f, "name", str(f)) for f in self.lance_ds.schema}
        except Exception:
            names = []
            try:
                names = list(getattr(self.lance_ds.schema, "names", []))
            except Exception:
                try:
                    names = [getattr(f, "name", str(f)) for f in getattr(self.lance_ds.schema, "fields", [])]
                except Exception:
                    names = []
            existing_fields = set(names)

        new_column_names = [name for name in df_schema.names if name not in existing_fields and name != self.right_on]
        if len(new_column_names) == 0:
            # No new columns to merge; return early
            return [{"fragment_meta": b"", "schema": b""}]  # Return empty bytes instead of None

        # Filter table to only include join key + new columns
        filtered_names = [name for name in df_schema.names if name == self.right_on or name in new_column_names]
        tbl = tbl.select(filtered_names)

        # Build RecordBatchReader from table batches
        batches = tbl.to_batches(max_chunksize=self.batch_size) if self.batch_size is not None else tbl.to_batches()
        reader = _pa.RecordBatchReader.from_batches(tbl.schema, batches)

        fragment = self.lance_ds.get_fragment(frag_id)
        if fragment is None:
            raise ValueError(f"Fragment {frag_id} not found in dataset")
        # Build schema argument: use the table's schema (including join key and new columns) unless an explicit reader_schema is provided
        schema_arg = tbl.schema if self.reader_schema is None else self.reader_schema
        fragment_meta, schema = fragment.merge(reader, left_on=self.left_on, right_on=self.right_on, schema=schema_arg)
        return [{"fragment_meta": daft.pickle.dumps(fragment_meta), "schema": daft.pickle.dumps(schema)}]


@daft_cls
class FastPathFragmentWriter:
    """Writes new columns as raw .lance files and stitches them into fragment metadata.

    This avoids rewriting existing data — only the new column values are written.
    Requires rows to be positionally aligned with the fragment (sorted by _rowaddr,
    complete row count).
    """

    def __init__(
        self,
        lance_ds: lance.LanceDataset,
        uri: str,
        new_column_names: list[str],
        storage_options: dict[str, str] | None = None,
    ):
        self.lance_ds = lance_ds
        self.uri = str(uri)
        self.new_column_names = new_column_names
        self.storage_options = storage_options

    @method.batch(return_dtype=_FRAGMENT_HANDLER_RETURN_DTYPE)
    def __call__(self, *cols: Any) -> list[dict[str, bytes]]:
        from lance.file import LanceFileWriter
        from lance.fragment import FragmentMetadata

        from daft.dependencies import pa as _pa

        if len(cols) == 0:
            return []

        *data_cols, rowaddr_col, fragment_ids = cols
        ids = fragment_ids.to_pylist() if hasattr(fragment_ids, "to_pylist") else list(fragment_ids)
        if len(ids) == 0:
            return []
        frag_id = ids[0]

        rowaddrs = rowaddr_col.to_pylist() if hasattr(rowaddr_col, "to_pylist") else list(rowaddr_col)

        # Build table of new columns, preserving the Arrow type from the daft Series.
        # pa.array(s.to_pylist()) loses type information: fixed_size_list<float32>[N]
        # becomes list<double> because Python floats are float64 and list structure is
        # inferred from Python lists. Using s.to_arrow() avoids this type erasure.
        arrays = []
        for s in data_cols:
            if hasattr(s, "to_arrow"):
                arr = s.to_arrow()
                if isinstance(arr, _pa.ChunkedArray):
                    arr = arr.combine_chunks()
            else:
                arr = _pa.array(list(s))
            arrays.append(arr)
        tbl = _pa.table({name: arr for name, arr in zip(self.new_column_names, arrays)})

        # Sort by _rowaddr to restore positional order
        sort_indices = _pa.compute.sort_indices(
            _pa.table({"_rowaddr": _pa.array(rowaddrs, type=_pa.uint64())}),
            sort_keys=[("_rowaddr", "ascending")],
        )
        tbl = tbl.take(sort_indices)

        # Determine the existing file format version so the new file matches.
        # Lance commit rejects fragments whose files mix major/minor versions.
        fragment = self.lance_ds.get_fragment(frag_id)
        if fragment is None:
            raise ValueError(f"Fragment {frag_id} not found in dataset")
        meta = dict(fragment.metadata.to_json())
        existing_files = list(meta["files"])
        if not existing_files:
            raise ValueError(f"Fragment {frag_id} has no data files; cannot infer version for fast-path write")
        file_major = int(existing_files[0]["file_major_version"])
        file_minor = int(existing_files[0]["file_minor_version"])

        # Write raw .lance file with only new columns
        filename = uuid.uuid4().hex + ".lance"
        filepath = os.path.join(self.uri, "data", filename)
        with LanceFileWriter(
            filepath,
            tbl.schema,
            version=f"{file_major}.{file_minor}",
            storage_options=self.storage_options,
        ) as writer:
            for b in tbl.to_batches():
                writer.write_batch(b)
        file_size = os.path.getsize(filepath)

        # Determine field IDs for the new columns. Lance's manifest-level
        # max_field_id includes nested child fields and field IDs from dropped
        # columns, so it is the correct high-water mark for dataset evolution.
        next_fid = self.lance_ds.max_field_id + 1

        # Stitch new data file into fragment metadata
        new_file_entry = {
            "path": filename,
            "fields": list(range(next_fid, next_fid + len(self.new_column_names))),
            "column_indices": list(range(len(self.new_column_names))),
            "file_major_version": file_major,
            "file_minor_version": file_minor,
            "file_size_bytes": file_size,
            "base_id": None,
        }
        meta["files"] = list(meta["files"]) + [new_file_entry]
        new_frag_meta = FragmentMetadata.from_json(json.dumps(meta))

        # Build new schema (original + new columns)
        new_schema = self.lance_ds.schema
        for col_name in self.new_column_names:
            col_idx = tbl.schema.get_field_index(col_name)
            new_schema = new_schema.append(_pa.field(col_name, tbl.schema.field(col_idx).type))

        return [{"fragment_meta": daft.pickle.dumps(new_frag_meta), "schema": daft.pickle.dumps(new_schema)}]


def _can_use_fast_path(
    df: daft.DataFrame,
    lance_ds: lance.LanceDataset,
    join_key: str,
) -> bool:
    if join_key != "_rowaddr":
        return False
    if "_rowaddr" not in df.column_names:
        return False
    if "fragment_id" not in df.column_names:
        return False
    # Use count_rows() rather than collect() to avoid caching one-shot Python
    # objects (e.g. BlobFile) into df._result_cache. collect() caches its result
    # so a subsequent groupby().map_groups() would receive the same exhausted
    # Python objects instead of fresh ones.
    df_row_count = df.count_rows()
    ds_row_count = lance_ds.count_rows()
    return df_row_count == ds_row_count


def merge_columns_from_df(
    df: daft.DataFrame,
    lance_ds: lance.LanceDataset,
    uri: str | pathlib.Path,
    *,
    read_columns: list[str] | None = None,
    reader_schema: pa.Schema | None = None,
    storage_options: dict[str, Any] | None = None,
    daft_remote_args: dict[str, Any] | None = None,
    concurrency: int | None = None,
    left_on: str | None = "_rowaddr",
    right_on: str | None = None,
    batch_size: int | None = 9223372036854775807,
) -> lance.LanceDataset:
    # Validate required keys
    if "fragment_id" not in df.column_names:
        raise ValueError("DataFrame must contain 'fragment_id' column for row-level merge workflow")
    join_key = right_on or left_on
    if join_key not in df.column_names:
        raise ValueError(
            f"DataFrame must contain join key column '{join_key}'. If missing, read with default_scan_options={{'with_row_address': True}} to expose '_rowaddr', or include the key explicitly."
        )

    # Compute existing field names
    existing_fields: set[str] = set()
    try:
        existing_fields = {getattr(f, "name", str(f)) for f in lance_ds.schema}
    except Exception:
        names: list[str] = []
        try:
            names = list(getattr(lance_ds.schema, "names", []))
        except Exception:
            try:
                names = [getattr(f, "name", str(f)) for f in getattr(lance_ds.schema, "fields", [])]
            except Exception:
                names = []
        existing_fields = set(names)

    new_cols = [c for c in df.column_names if c not in existing_fields and c not in ("fragment_id", join_key)]
    if len(new_cols) == 0:
        raise ValueError(
            "No new columns to merge; Lance requires the reader stream to include only the join key and new columns not present in the dataset."
        )

    # Derive read_columns if not provided
    if read_columns is None:
        read_columns = [join_key] + new_cols

    # Decide: fast path (raw file write) or slow path (keyed join)
    use_fast_path = _can_use_fast_path(df, lance_ds, join_key)

    if use_fast_path:
        return _merge_fast_path(df, lance_ds, uri, new_cols, storage_options=storage_options)
    else:
        return _merge_slow_path(
            df,
            lance_ds,
            uri,
            read_columns,
            left_on,
            right_on,
            reader_schema,
            batch_size,
            storage_options=storage_options,
        )


def _merge_fast_path(
    df: daft.DataFrame,
    lance_ds: lance.LanceDataset,
    uri: str | pathlib.Path,
    new_column_names: list[str],
    storage_options: dict[str, Any] | None = None,
) -> lance.LanceDataset:
    """Metadata-only add_columns: write raw .lance files and stitch into fragment metadata."""
    handler = FastPathFragmentWriter(lance_ds, str(uri), new_column_names, storage_options=storage_options)

    grouped = df.groupby("fragment_id").map_groups(
        handler(*(df[c] for c in new_column_names), df["_rowaddr"], df["fragment_id"]).alias("commit_message")  # type: ignore[attr-defined]
    )

    commit_messages = grouped.collect().to_pydict()["commit_message"]
    new_schema = None
    fragment_metas: list[Any] = []
    enriched_frag_ids: set[int] = set()

    for commit_message in commit_messages:
        fragment_meta_bytes = commit_message["fragment_meta"]
        schema_bytes = commit_message["schema"]
        if not fragment_meta_bytes or not schema_bytes:
            continue
        fmeta = daft.pickle.loads(fragment_meta_bytes)
        fragment_metas.append(fmeta)
        # pylance 6.0.0 FragmentMetadata exposes the id only via to_json()
        enriched_frag_ids.add(int(fmeta.to_json()["id"]))
        if new_schema is None:
            new_schema = daft.pickle.loads(schema_bytes)

    if new_schema is None:
        raise ValueError("Fast path produced no fragment metadata")

    # Include untouched fragments (they'll get NULLs for new columns)
    for frag in lance_ds.get_fragments():
        if frag.fragment_id not in enriched_frag_ids:
            fragment_metas.append(frag.metadata)

    op = lance.LanceOperation.Merge(fragment_metas, LanceSchema.from_pyarrow(new_schema))
    return lance.LanceDataset.commit(
        str(uri),
        op,
        read_version=lance_ds.version,
        storage_options=storage_options,
        **namespace_kwargs_for_dataset(lance_ds),
    )


def _merge_slow_path(
    df: daft.DataFrame,
    lance_ds: lance.LanceDataset,
    uri: str | pathlib.Path,
    read_columns: list[str],
    left_on: str | None,
    right_on: str | None,
    reader_schema: pa.Schema | None,
    batch_size: int | None,
    storage_options: dict[str, Any] | None = None,
) -> lance.LanceDataset:
    """Original keyed-join merge path: rewrites fragment data."""
    handler_udf = GroupFragmentMergeUDF(
        lance_ds,
        left_on,
        right_on,
        read_columns,
        reader_schema,
        batch_size,
    )

    grouped = df.groupby("fragment_id").map_groups(
        handler_udf(*(df[c] for c in read_columns), df["fragment_id"]).alias("commit_message")  # type: ignore[attr-defined]
    )

    commit_messages = grouped.collect().to_pydict()["commit_message"]
    new_schema = None
    fragment_metas = []
    for commit_message in commit_messages:
        fragment_meta = commit_message["fragment_meta"]
        schema = commit_message["schema"]
        if not fragment_meta or not schema:
            continue
        fragment_metas.append(daft.pickle.loads(fragment_meta))
        if new_schema is None:
            new_schema = daft.pickle.loads(schema)
            continue
    if new_schema is None:
        return lance_ds
    op = lance.LanceOperation.Merge(fragment_metas, new_schema)
    return lance_ds.commit(
        uri,
        op,
        read_version=lance_ds.version,
        storage_options=storage_options,
        **namespace_kwargs_for_dataset(lance_ds),
    )
