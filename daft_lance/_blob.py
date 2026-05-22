from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, cast

import lance
import lance.blob  # registers lance.blob.v2 extension type
import pyarrow.compute as pc

import daft
from daft.dataframe import DataFrame
from daft.datatype import DataType
from daft.dependencies import pa
from daft.series import Series
from daft_lance._metadata import LANCE_BLOB_DESCRIPTOR_TYPE

if TYPE_CHECKING:
    from collections.abc import Iterable

LanceStorageVersion = Literal["legacy", "0.1", "stable", "2.0", "2.1", "2.2", "2.3", "next"]

# Lance Blob V2 requires the 2.2 storage version; lower versions cannot encode
# the logical lance.blob.v2 extension as descriptors.
LANCE_BLOB_V2_STORAGE_VERSION: LanceStorageVersion = "2.2"


def is_blob_v2_field(field: pa.Field[Any]) -> bool:
    """True if the field carries the logical lance.blob.v2 extension type."""
    if isinstance(field.type, pa.ExtensionType) and field.type.extension_name == "lance.blob.v2":
        return True
    md = field.metadata or {}
    return md.get(b"ARROW:extension:name") == b"lance.blob.v2"


def detect_blob_v2_columns(schema: pa.Schema) -> list[str]:
    """Return the names of fields in `schema` whose type is lance.blob.v2."""
    return [f.name for f in schema if is_blob_v2_field(f)]


def binary_to_blob_v2_array(col: pa.ChunkedArray[Any] | pa.Array[Any]) -> pa.ExtensionArray[Any]:
    """Promote an Arrow binary / large_binary column to a logical lance.blob.v2 ExtensionArray.

    Builds the storage struct directly from the Arrow buffer to avoid the
    list[bytes] round-trip that ``lance.blob.blob_array`` requires.
    """
    combined: pa.Array[Any] = col.combine_chunks() if isinstance(col, pa.ChunkedArray) else col
    if not pa.types.is_large_binary(combined.type):
        if not pa.types.is_binary(combined.type):
            raise TypeError(f"expected binary or large_binary, got {combined.type}")
        combined = combined.cast(pa.large_binary())
    arr = cast(pa.LargeBinaryArray, combined)
    n = len(arr)
    sizes = pc.binary_length(arr).cast(pa.uint64())
    storage_fields: list[pa.Field[Any]] = [
        pa.field("data", pa.large_binary()),
        pa.field("uri", pa.string()),
        pa.field("position", pa.uint64()),
        pa.field("size", pa.uint64()),
    ]
    storage = pa.StructArray.from_arrays(
        [
            arr,
            pa.nulls(n, type=pa.string()),
            pa.array([0] * n, type=pa.uint64()),
            sizes,
        ],
        fields=storage_fields,
    )
    return pa.ExtensionArray.from_storage(lance.blob.BlobType(), storage)


def blob_aware_schema_for_validation(candidate: pa.Schema, reference: pa.Schema) -> pa.Schema:
    """Normalize lance.blob.v2 fields to large_binary for schema castability checks.

    For each field in `candidate`, if the same-named field in `reference` is a
    lance.blob.v2 extension (or the candidate field itself is one), demote the
    field type to large_binary. Symmetric demotion lets the castability check
    succeed when the only difference is binary vs blob.v2 at the same position.
    """
    ref_by_name = {f.name: f for f in reference}
    new_fields: list[pa.Field[Any]] = []
    for f in candidate:
        ref = ref_by_name.get(f.name)
        ref_is_blob = ref is not None and is_blob_v2_field(ref)
        self_is_blob = is_blob_v2_field(f)
        if ref_is_blob or self_is_blob:
            new_fields.append(pa.field(f.name, pa.large_binary(), nullable=f.nullable))
        else:
            new_fields.append(f)
    metadata = cast("dict[bytes | str, bytes | str] | None", candidate.metadata)
    return pa.schema(new_fields, metadata=metadata)


def _pyarrow_schema_castable(src: pa.Schema, dst: pa.Schema) -> bool:
    """True if every field in `src` can be cast to the same-positioned field in `dst`."""
    if len(src) != len(dst):
        return False
    for src_field, dst_field in zip(src, dst):
        empty_array = pa.array([], type=src_field.type)
        try:
            empty_array.cast(dst_field.type)
        except Exception:
            return False
    return True


def resolve_storage_version(
    requested: LanceStorageVersion | None,
    existing: LanceStorageVersion | None,
    mode: Literal["create", "append", "overwrite"],
) -> LanceStorageVersion | None:
    """Resolve the storage version to use for a write, with mismatch detection.

    - For non-append modes, returns `requested` as-is.
    - For append: inherits `existing` when `requested is None`; raises
      ValueError when both differ.
    """
    if mode != "append":
        return requested
    if existing is None:
        return requested
    if requested is None:
        return existing
    if requested != existing:
        raise ValueError(
            f"data_storage_version={requested!r} does not match "
            f"existing dataset version {existing!r}. Version changes are "
            f"dataset-wide and must be performed as an explicit migration step."
        )
    return requested


class BlobV2WritePolicy:
    """Owns Blob V2 write-side concerns for a fixed set of columns.

    A single instance encapsulates: input validation, storage-version
    defaulting, effective-schema derivation, per-table column wrapping, and
    cast-target adjustment. An empty column set makes every method a no-op
    fast-path so callers can use the policy unconditionally.
    """

    def __init__(self, blob_columns: list[str] | None) -> None:
        cols = self._normalize(blob_columns)
        self._columns: list[str] = cols
        self._column_set: set[str] = set(cols)

    @staticmethod
    def _normalize(value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, str):
            raise TypeError(f"blob_columns must be a list of strings, got {type(value).__name__}")
        try:
            cols = list(value)
        except TypeError:
            raise TypeError(f"blob_columns must be a list of strings, got {type(value).__name__}")
        if any(not isinstance(c, str) for c in cols):
            raise TypeError("blob_columns entries must be strings")
        seen: set[str] = set()
        result: list[str] = []
        for c in cols:
            if c not in seen:
                seen.add(c)
                result.append(c)
        return result

    @property
    def enabled(self) -> bool:
        return bool(self._columns)

    @property
    def columns(self) -> list[str]:
        return list(self._columns)

    def add_columns(self, names: Iterable[str]) -> None:
        """Add additional blob columns (e.g., auto-detected from an existing dataset)."""
        for name in names:
            if name not in self._column_set:
                self._columns.append(name)
                self._column_set.add(name)

    def apply_blob_v2_default(self, current: LanceStorageVersion | None) -> LanceStorageVersion | None:
        """Default `current` to Blob V2's storage version when needed.

        If blob columns are present and the caller did not specify a version,
        return ``LANCE_BLOB_V2_STORAGE_VERSION``. Otherwise leave `current`
        alone — Lance will reject at write time if the chosen version cannot
        encode Blob V2.
        """
        if self.enabled and current is None:
            return LANCE_BLOB_V2_STORAGE_VERSION
        return current

    def validate_columns_present(self, schema: pa.Schema) -> None:
        """Each declared blob column must exist in `schema` and be binary-like."""
        if not self.enabled:
            return
        fields_by_name = {f.name: f for f in schema}
        for name in self._columns:
            field = fields_by_name.get(name)
            if field is None:
                raise ValueError(f"blob_columns references unknown column {name!r}")
            if is_blob_v2_field(field) or pa.types.is_binary(field.type) or pa.types.is_large_binary(field.type):
                continue
            raise ValueError(f"blob_columns[{name!r}] must be binary or large_binary, got {field.type}")

    def build_effective_schema(self, schema: pa.Schema) -> pa.Schema:
        """Retype declared blob columns to the lance.blob.v2 extension type."""
        if not self.enabled:
            return schema
        new_fields = [
            (
                pa.field(f.name, lance.blob.BlobType(), nullable=f.nullable, metadata=f.metadata)
                if f.name in self._column_set and not is_blob_v2_field(f)
                else f
            )
            for f in schema
        ]
        metadata = cast("dict[bytes | str, bytes | str] | None", schema.metadata)
        return pa.schema(new_fields, metadata=metadata)

    def wrap_table(self, table: pa.Table) -> pa.Table:
        """Replace declared binary columns with logical lance.blob.v2 arrays."""
        if not self.enabled:
            return table
        new_arrays: list[pa.ChunkedArray[Any] | pa.Array[Any]] = []
        new_fields: list[pa.Field[Any]] = []
        for i, field in enumerate(table.schema):
            col = table.column(i)
            if field.name in self._column_set and not is_blob_v2_field(field):
                blob_arr = binary_to_blob_v2_array(col)
                new_arrays.append(blob_arr)
                new_fields.append(pa.field(field.name, blob_arr.type, nullable=field.nullable, metadata=field.metadata))
            else:
                new_arrays.append(col)
                new_fields.append(field)
        metadata = cast("dict[bytes | str, bytes | str] | None", table.schema.metadata)
        return pa.Table.from_arrays(new_arrays, schema=pa.schema(new_fields, metadata=metadata))

    def cast_target_schema(self, reference: pa.Schema) -> pa.Schema:
        """Build a cast target where blob columns are kept as binary.

        The cast does not have to produce a lance.blob.v2 extension array;
        ``wrap_table`` handles that conversion explicitly.
        """
        if not self.enabled:
            return reference
        new_fields = []
        for f in reference:
            if f.name in self._column_set and is_blob_v2_field(f):
                storage = f.type.storage_type if isinstance(f.type, pa.ExtensionType) else pa.large_binary()
                inner = (
                    storage if pa.types.is_binary(storage) or pa.types.is_large_binary(storage) else pa.large_binary()
                )
                new_fields.append(pa.field(f.name, inner, nullable=f.nullable, metadata=f.metadata))
            else:
                new_fields.append(f)
        metadata = cast("dict[bytes | str, bytes | str] | None", reference.metadata)
        return pa.schema(new_fields, metadata=metadata)


def take_blobs(df: DataFrame, ds: lance.LanceDataset, column: str) -> DataFrame:
    """Materialize blobs from the Lance dataset into the dataframe."""
    # (1) Validate that we can actually materialize the blobs
    schema = df.schema()
    columns = set(schema.column_names())
    if column not in columns:
        raise ValueError(f"Column {column} is not in the schema")
    if "_rowid" not in columns:
        raise ValueError(
            "Row ids are not available in the schema.",
            'Please use daft.read_lance(uri, default_scan_options={"with_row_id": True}) to read the dataset with row ids.',
        )
    if schema[column].dtype != LANCE_BLOB_DESCRIPTOR_TYPE:
        raise ValueError(f"Column {column} is not a Lance blob column")

    # 2. Create a UDF closure over the lance dataset so we can take the blobs.
    @daft.func.batch(return_dtype=daft.DataType.python())
    def take_blobs_udf(row_ids: Series):  # type: ignore[no-untyped-def]
        blobs = ds.take_blobs(column, ids=row_ids.to_pylist())
        return Series.from_pylist(blobs, name=column, dtype=DataType.python(), pyobj="force")

    # 3. Return the new DataFrame with the column replaced by the logical lance blob type (python objects).
    return df.with_column(column, take_blobs_udf(df["_rowid"]))
