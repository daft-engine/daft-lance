"""Direct unit tests for `daft_lance._blob`.

These exercise `BlobV2WritePolicy`, the module-level helpers
(`is_blob_v2_field`, `detect_blob_v2_columns`, `blob_aware_schema_for_validation`,
`binary_to_blob_v2_array`), and the pure helper `resolve_storage_version`.
The end-to-end black-box tests in `test_lance_blob_v2_*.py` remain unchanged.
"""

from __future__ import annotations

import lance.blob
import pyarrow as pa
import pytest

from daft_lance._blob import (
    LANCE_BLOB_V2_STORAGE_VERSION,
    BlobV2WritePolicy,
    binary_to_blob_v2_array,
    blob_aware_schema_for_validation,
    detect_blob_v2_columns,
    is_blob_v2_field,
    resolve_storage_version,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _blob_field(name: str, nullable: bool = True) -> pa.Field:
    return pa.field(name, lance.blob.BlobType(), nullable=nullable)


# ---------------------------------------------------------------------------
# BlobV2WritePolicy: construction
# ---------------------------------------------------------------------------


def test_policy_none_is_disabled() -> None:
    p = BlobV2WritePolicy(None)
    assert p.enabled is False
    assert p.columns == []


def test_policy_disabled_methods_are_noops() -> None:
    p = BlobV2WritePolicy(None)
    schema = pa.schema([("a", pa.int64()), ("b", pa.binary())])
    assert p.build_effective_schema(schema) is schema
    assert p.cast_target_schema(schema) is schema
    table = pa.table({"a": [1], "b": [b"x"]})
    assert p.wrap_table(table) is table
    # validate is a no-op even if "blob" columns don't exist
    p.validate_columns_present(pa.schema([]))


def test_policy_dedups_columns() -> None:
    p = BlobV2WritePolicy(["a", "a", "b"])
    assert p.columns == ["a", "b"]
    assert p.enabled is True


def test_policy_rejects_non_list() -> None:
    with pytest.raises(TypeError):
        BlobV2WritePolicy("not a list")  # type: ignore[arg-type]


def test_policy_rejects_non_string_entries() -> None:
    with pytest.raises(TypeError):
        BlobV2WritePolicy([1, 2])  # type: ignore[list-item]


def test_policy_add_columns_dedup_preserves_order() -> None:
    p = BlobV2WritePolicy(["a"])
    p.add_columns(["c", "a"])
    assert p.columns == ["a", "c"]


# ---------------------------------------------------------------------------
# BlobV2WritePolicy: storage version defaulting
# ---------------------------------------------------------------------------


def test_apply_blob_v2_default_with_blob_and_no_current() -> None:
    p = BlobV2WritePolicy(["b"])
    assert p.apply_blob_v2_default(None) == LANCE_BLOB_V2_STORAGE_VERSION


def test_apply_blob_v2_default_caller_wins() -> None:
    p = BlobV2WritePolicy(["b"])
    assert p.apply_blob_v2_default("2.1") == "2.1"


def test_apply_blob_v2_default_no_blob_passthrough() -> None:
    p = BlobV2WritePolicy(None)
    assert p.apply_blob_v2_default(None) is None
    assert p.apply_blob_v2_default("2.0") == "2.0"


# ---------------------------------------------------------------------------
# BlobV2WritePolicy: validate_columns_present
# ---------------------------------------------------------------------------


def test_validate_columns_rejects_unknown_column() -> None:
    p = BlobV2WritePolicy(["missing"])
    schema = pa.schema([("a", pa.binary())])
    with pytest.raises(ValueError, match="unknown column"):
        p.validate_columns_present(schema)


def test_validate_columns_rejects_int64_column() -> None:
    p = BlobV2WritePolicy(["a"])
    schema = pa.schema([("a", pa.int64())])
    with pytest.raises(ValueError, match="must be binary or large_binary"):
        p.validate_columns_present(schema)


def test_validate_columns_accepts_binary() -> None:
    p = BlobV2WritePolicy(["a"])
    p.validate_columns_present(pa.schema([("a", pa.binary())]))


def test_validate_columns_accepts_large_binary() -> None:
    p = BlobV2WritePolicy(["a"])
    p.validate_columns_present(pa.schema([("a", pa.large_binary())]))


def test_validate_columns_accepts_already_blob() -> None:
    p = BlobV2WritePolicy(["a"])
    p.validate_columns_present(pa.schema([_blob_field("a")]))


# ---------------------------------------------------------------------------
# BlobV2WritePolicy: build_effective_schema
# ---------------------------------------------------------------------------


def test_build_effective_schema_retypes_binary_to_blob() -> None:
    p = BlobV2WritePolicy(["payload"])
    schema = pa.schema([("id", pa.int64()), ("payload", pa.binary())])
    eff = p.build_effective_schema(schema)
    assert eff.field("id").type == pa.int64()
    assert is_blob_v2_field(eff.field("payload"))


def test_build_effective_schema_leaves_non_blob_alone() -> None:
    p = BlobV2WritePolicy(["payload"])
    schema = pa.schema([("a", pa.int64()), ("payload", pa.large_binary())])
    eff = p.build_effective_schema(schema)
    assert eff.field("a").type == pa.int64()
    assert is_blob_v2_field(eff.field("payload"))


# ---------------------------------------------------------------------------
# BlobV2WritePolicy: wrap_table
# ---------------------------------------------------------------------------


def test_wrap_table_converts_binary_column_only() -> None:
    p = BlobV2WritePolicy(["payload"])
    table = pa.table({"id": pa.array([1, 2], type=pa.int64()), "payload": pa.array([b"x", b"y"], type=pa.binary())})
    wrapped = p.wrap_table(table)
    assert wrapped.schema.field("id").type == pa.int64()
    assert is_blob_v2_field(wrapped.schema.field("payload"))


def test_wrap_table_leaves_already_blob_columns_alone() -> None:
    p = BlobV2WritePolicy(["payload"])
    arr = binary_to_blob_v2_array(pa.array([b"x"], type=pa.binary()))
    table = pa.table({"payload": arr})
    wrapped = p.wrap_table(table)
    assert is_blob_v2_field(wrapped.schema.field("payload"))
    assert wrapped.num_rows == 1


# ---------------------------------------------------------------------------
# BlobV2WritePolicy: cast_target_schema
# ---------------------------------------------------------------------------


def test_cast_target_schema_demotes_blob_to_large_binary() -> None:
    p = BlobV2WritePolicy(["payload"])
    schema = pa.schema([_blob_field("payload"), ("id", pa.int64())])
    target = p.cast_target_schema(schema)
    assert target.field("payload").type == pa.large_binary()
    assert target.field("id").type == pa.int64()


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def test_is_blob_v2_field_true_for_blob() -> None:
    assert is_blob_v2_field(_blob_field("a")) is True


def test_is_blob_v2_field_false_for_binary() -> None:
    assert is_blob_v2_field(pa.field("a", pa.binary())) is False


def test_detect_blob_v2_columns_returns_names() -> None:
    schema = pa.schema([_blob_field("b1"), ("x", pa.int64()), _blob_field("b2")])
    assert detect_blob_v2_columns(schema) == ["b1", "b2"]


def test_blob_aware_schema_for_validation_demotes_candidate_only_blob() -> None:
    candidate = pa.schema([_blob_field("p"), ("a", pa.int64())])
    reference = pa.schema([("p", pa.large_binary()), ("a", pa.int64())])
    out = blob_aware_schema_for_validation(candidate, reference)
    assert out.field("p").type == pa.large_binary()


def test_blob_aware_schema_for_validation_demotes_reference_only_blob() -> None:
    candidate = pa.schema([("p", pa.large_binary()), ("a", pa.int64())])
    reference = pa.schema([_blob_field("p"), ("a", pa.int64())])
    out = blob_aware_schema_for_validation(candidate, reference)
    assert out.field("p").type == pa.large_binary()


# ---------------------------------------------------------------------------
# binary_to_blob_v2_array
# ---------------------------------------------------------------------------


def test_binary_to_blob_v2_array_empty() -> None:
    arr = binary_to_blob_v2_array(pa.array([], type=pa.binary()))
    assert isinstance(arr, pa.ExtensionArray)
    assert len(arr) == 0


def test_binary_to_blob_v2_array_single_row() -> None:
    arr = binary_to_blob_v2_array(pa.array([b"abc"], type=pa.binary()))
    assert isinstance(arr, pa.ExtensionArray)
    assert len(arr) == 1
    storage = arr.storage  # type: ignore[attr-defined]
    assert storage.field("data").to_pylist() == [b"abc"]
    assert storage.field("size").to_pylist() == [3]


def test_binary_to_blob_v2_array_multi_chunk_via_chunked_array() -> None:
    chunked = pa.chunked_array([pa.array([b"a"], type=pa.binary()), pa.array([b"bb", b"ccc"], type=pa.binary())])
    arr = binary_to_blob_v2_array(chunked)
    assert len(arr) == 3
    storage = arr.storage  # type: ignore[attr-defined]
    assert storage.field("data").to_pylist() == [b"a", b"bb", b"ccc"]
    assert storage.field("size").to_pylist() == [1, 2, 3]


def test_binary_to_blob_v2_array_rejects_int64() -> None:
    with pytest.raises(TypeError):
        binary_to_blob_v2_array(pa.array([1, 2], type=pa.int64()))  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# resolve_storage_version
# ---------------------------------------------------------------------------


def test_resolve_storage_version_non_append_passthrough() -> None:
    assert resolve_storage_version("2.1", "2.2", "create") == "2.1"
    assert resolve_storage_version(None, "2.2", "create") is None
    assert resolve_storage_version("2.0", None, "overwrite") == "2.0"


def test_resolve_storage_version_append_empty_dataset_passthrough() -> None:
    assert resolve_storage_version("2.1", None, "append") == "2.1"
    assert resolve_storage_version(None, None, "append") is None


def test_resolve_storage_version_append_inherits_when_requested_is_none() -> None:
    assert resolve_storage_version(None, "2.1", "append") == "2.1"


def test_resolve_storage_version_append_mismatch_raises() -> None:
    with pytest.raises(ValueError, match="does not match existing dataset version"):
        resolve_storage_version("2.2", "2.1", "append")


def test_resolve_storage_version_append_match_passes() -> None:
    assert resolve_storage_version("2.1", "2.1", "append") == "2.1"
