from __future__ import annotations

import os

import lance
import lance.blob
import pyarrow as pa
import pytest
from lance import Blob
from lance.blob import BlobType
from pytest import TempPathFactory

import daft

KIND_INLINE = 0
KIND_PACKED = 1
KIND_DEDICATED = 2
KIND_EXTERNAL = 3


@pytest.fixture(scope="function")
def lance_path(tmp_path_factory: TempPathFactory) -> str:
    return str(tmp_path_factory.mktemp("dataset"))


def _descriptors(uri: str, column: str = "data") -> list[dict]:
    return daft.read_lance(uri).to_arrow().column(column).to_pylist()


def _kinds(uri: str, column: str = "data") -> list[int]:
    return [d["kind"] for d in _descriptors(uri, column)]


def _assert_logical_blob_field(uri: str, column: str) -> None:
    ds = lance.dataset(uri)
    field = ds.schema.field(column)
    assert isinstance(field.type, BlobType), f"expected lance.blob.v2 field, got {field.type}"
    assert field.type.extension_name == "lance.blob.v2"


def test_blob_columns_inline(lance_path: str) -> None:
    df = daft.from_pydict({"id": [1, 2, 3], "data": [b"a", b"bb", b"ccc"]})
    df.write_lance(lance_path, blob_columns=["data"]).collect()

    _assert_logical_blob_field(lance_path, "data")
    assert _kinds(lance_path) == [KIND_INLINE, KIND_INLINE, KIND_INLINE]


def test_blob_columns_packed(lance_path: str) -> None:
    df = daft.from_pydict({"id": [1, 2, 3], "data": [b"x" * 100_000] * 3})
    df.write_lance(lance_path, blob_columns=["data"]).collect()

    _assert_logical_blob_field(lance_path, "data")
    # Lance heuristic may place 100KB in packed; tolerate inline if the heuristic shifts.
    assert set(_kinds(lance_path)).issubset({KIND_INLINE, KIND_PACKED})


def test_blob_columns_dedicated(lance_path: str) -> None:
    # >4MB hits the dedicated path.
    df = daft.from_pydict({"id": [1, 2], "data": [b"y" * 5_000_000, b"z" * 5_000_000]})
    df.write_lance(lance_path, blob_columns=["data"]).collect()

    _assert_logical_blob_field(lance_path, "data")
    assert _kinds(lance_path) == [KIND_DEDICATED, KIND_DEDICATED]


def test_blob_columns_default_storage_version(lance_path: str) -> None:
    """No explicit data_storage_version should still produce a 2.2 dataset."""
    df = daft.from_pydict({"id": [1], "data": [b"hi"]})
    df.write_lance(lance_path, blob_columns=["data"]).collect()

    ds = lance.dataset(lance_path)
    assert ds.data_storage_version == "2.2"


def test_blob_columns_unknown_column(lance_path: str) -> None:
    df = daft.from_pydict({"id": [1], "data": [b"hi"]})
    with pytest.raises(ValueError, match="unknown column 'missing'"):
        df.write_lance(lance_path, blob_columns=["missing"]).collect()


def test_blob_columns_non_binary(lance_path: str) -> None:
    df = daft.from_pydict({"id": [1], "label": ["text"]})
    with pytest.raises(ValueError, match="binary or large_binary"):
        df.write_lance(lance_path, blob_columns=["label"]).collect()


def test_blob_columns_large_binary(lance_path: str) -> None:
    """Daft binary columns may materialize as pa.large_binary; opt-in must accept both."""
    # Force large_binary at the pyarrow layer.
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("data", pa.large_binary()),
        ]
    )
    arrow_table = pa.Table.from_pydict({"id": [1, 2], "data": [b"foo", b"barbaz"]}, schema=schema)
    df = daft.from_arrow(arrow_table)
    df.write_lance(lance_path, blob_columns=["data"]).collect()

    _assert_logical_blob_field(lance_path, "data")
    assert _kinds(lance_path) == [KIND_INLINE, KIND_INLINE]


def test_blob_columns_append(lance_path: str) -> None:
    """Blob columns are auto-detected on append to existing dataset.

    Append into an existing Blob V2 dataset: blob_columns from prior write should
    be honored even if the user does not repeat them on append (auto-detected).
    """
    daft.from_pydict({"id": [1, 2], "data": [b"first", b"second"]}).write_lance(
        lance_path, blob_columns=["data"]
    ).collect()

    # Second write does NOT re-declare blob_columns — sink should auto-detect.
    daft.from_pydict({"id": [3, 4], "data": [b"third", b"fourth"]}).write_lance(lance_path, mode="append").collect()

    ds = lance.dataset(lance_path)
    assert ds.count_rows() == 4
    _assert_logical_blob_field(lance_path, "data")
    assert _kinds(lance_path) == [KIND_INLINE, KIND_INLINE, KIND_INLINE, KIND_INLINE]


def test_already_logical_blob_via_pyarrow_path(tmp_path_factory: TempPathFactory) -> None:
    """Logical blob columns pass through unchanged.

    An already-logical lance.blob.v2 column built via pa table + lance.blob_array
    must pass through the daft sink unchanged. blob_columns is not needed since the
    column is already logical. Uses inline payloads so the test does not need to
    register external base paths.
    """
    table = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "blob": lance.blob.blob_array([b"alpha", b"beta", b"gamma-payload"]),
        }
    )
    df = daft.from_arrow(table)

    out_path = str(tmp_path_factory.mktemp("dataset"))
    df.write_lance(out_path, data_storage_version="2.2").collect()

    _assert_logical_blob_field(out_path, "blob")
    descriptors = _descriptors(out_path, column="blob")
    assert [d["kind"] for d in descriptors] == [KIND_INLINE, KIND_INLINE, KIND_INLINE]
    assert [d["size"] for d in descriptors] == [5, 4, 13]


@pytest.mark.skip(
    reason=(
        "Writing kind=3 external-URI blobs from daft requires "
        "allow_external_blob_outside_bases (only on lance.write_dataset, not "
        "lance.fragment.write_fragments) or explicit DatasetBasePath registration. "
        "Out of scope for the current write opt-in; tracked separately."
    )
)
def test_external_blob_kind_3_write(tmp_path_factory: TempPathFactory) -> None:
    blob_dir = str(tmp_path_factory.mktemp("blobs"))
    external_path = os.path.join(blob_dir, "ref.bin")
    with open(external_path, "wb") as f:
        f.write(b"external-contents-larger-than-needed")
    external_uri = f"file://{external_path}"

    table = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "blob": lance.blob.blob_array(
                [
                    external_uri,
                    Blob.from_uri(external_uri, position=4, size=8),
                ]
            ),
        }
    )
    df = daft.from_arrow(table)
    out_path = str(tmp_path_factory.mktemp("dataset"))
    df.write_lance(out_path, data_storage_version="2.2").collect()
