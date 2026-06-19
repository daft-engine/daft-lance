from __future__ import annotations

import os

import lance
import pyarrow as pa
import pytest
from lance import Blob
from pytest import TempPathFactory

import daft
from daft import col
from daft_lance._blob import take_blobs
from daft_lance._metadata import LANCE_BLOB_DESCRIPTOR_TYPE

KIND_INLINE = 0
KIND_PACKED = 1
KIND_DEDICATED = 2
KIND_EXTERNAL = 3

# Large enough so the kind-3 slice (position=1024, size=4096) is in-range.
EXTERNAL_FILE_SIZE = 5120


@pytest.fixture(scope="module")
def lance_dataset(tmp_path_factory: TempPathFactory) -> lance.LanceDataset:
    """One row per storage kind: inline, packed, dedicated, external (full), external (slice)."""
    blob_dir = str(tmp_path_factory.mktemp("blobs"))
    external_path = os.path.join(blob_dir, "placeholder.mp4")
    with open(external_path, "wb") as f:
        f.write(b"\x00" * EXTERNAL_FILE_SIZE)

    values = [
        b"tiny-inline-data",  # kind 0
        b"x" * 100_000,  # kind 1
        b"y" * 5_000_000,  # kind 2
        f"file://{external_path}",  # kind 3 full
        Blob.from_uri(f"file://{external_path}", position=1024, size=4096),  # kind 3 slice
    ]
    table = pa.table(
        {
            "id": pa.array([1, 2, 3, 4, 5], type=pa.int64()),
            "blob": lance.blob_array(values),
        }
    )
    path = str(tmp_path_factory.mktemp("dataset"))
    return lance.write_dataset(
        table,
        path,
        data_storage_version="2.2",
        allow_external_blob_outside_bases=True,
    )


def test_descriptor_schema_shape(lance_dataset: lance.LanceDataset) -> None:
    """Tests that the blob column has the correct schema."""
    table = daft.read_lance(lance_dataset.uri).to_arrow()
    field = table.schema.field("blob")

    # Daft maps string() → large_utf8 in Arrow.
    expected_type = pa.struct(
        [
            pa.field("kind", pa.uint8()),
            pa.field("position", pa.uint64()),
            pa.field("size", pa.uint64()),
            pa.field("blob_id", pa.uint32()),
            pa.field("blob_uri", pa.large_utf8()),
        ]
    )

    assert pa.types.is_struct(field.type)
    assert field.type == expected_type, f"Expected {expected_type}, got {field.type}"


def test_descriptor_kinds(lance_dataset: lance.LanceDataset) -> None:
    """Tests all blob kinds are read correctly."""
    df = daft.read_lance(lance_dataset.uri)
    blobs = df.to_pydict()["blob"]

    # 0: inline
    assert blobs[0]["kind"] == KIND_INLINE
    assert blobs[0]["size"] == len(b"tiny-inline-data")

    # 1: packed
    assert blobs[1]["kind"] == KIND_PACKED
    assert blobs[1]["size"] == 100_000

    # 2: dedicated
    assert blobs[2]["kind"] == KIND_DEDICATED
    assert blobs[2]["size"] == 5_000_000

    # 3: external full
    assert blobs[3]["kind"] == KIND_EXTERNAL
    assert blobs[3]["blob_uri"].startswith("file://")
    assert blobs[3]["position"] == 0

    # 4: external slice
    assert blobs[4]["kind"] == KIND_EXTERNAL
    assert blobs[4]["position"] == 1024
    assert blobs[4]["size"] == 4096


def test_descriptor_blob_uri_is_large_utf8(lance_dataset: lance.LanceDataset) -> None:
    """Daft's normal arrow ingestion coerces utf8 (from lance) to large_utf8 (daft string)."""
    table = daft.read_lance(lance_dataset.uri).to_arrow()
    field = table.schema.field("blob")
    blob_uri_field = field.type.field("blob_uri")
    assert pa.types.is_large_string(blob_uri_field.type), f"expected large_utf8, got {blob_uri_field.type}"


def test_take_blobs_missing_column(lance_dataset: lance.LanceDataset) -> None:
    """take_blobs raises ValueError when the requested column is absent from the schema."""
    df = daft.read_lance(lance_dataset.uri, default_scan_options={"with_row_id": True})
    with pytest.raises(ValueError, match="nonexistent"):
        take_blobs(df, lance_dataset, "nonexistent")


def test_take_blobs_missing_rowid(lance_dataset: lance.LanceDataset) -> None:
    """take_blobs raises ValueError when _rowid is absent (dataset not read with row IDs)."""
    df = daft.read_lance(lance_dataset.uri)  # no with_row_id=True
    with pytest.raises(ValueError, match="Row ids"):
        take_blobs(df, lance_dataset, "blob")


def test_take_blobs_wrong_column_type(lance_dataset: lance.LanceDataset) -> None:
    """take_blobs raises ValueError when the target column is not a blob descriptor column."""
    df = daft.read_lance(lance_dataset.uri, default_scan_options={"with_row_id": True})
    with pytest.raises(ValueError, match="Lance blob"):
        take_blobs(df, lance_dataset, "id")


def test_take_blobs_returns_dataframe(lance_dataset: lance.LanceDataset) -> None:
    """take_blobs returns a Daft DataFrame."""
    df = daft.read_lance(lance_dataset.uri, default_scan_options={"with_row_id": True})
    assert isinstance(take_blobs(df, lance_dataset, "blob"), daft.DataFrame)


def test_take_blobs_no_extra_columns(lance_dataset: lance.LanceDataset) -> None:
    """take_blobs replaces the descriptor column in-place; column set is unchanged."""
    df = daft.read_lance(lance_dataset.uri, default_scan_options={"with_row_id": True})
    df = take_blobs(df, lance_dataset, "blob")
    assert set(df.schema().column_names()) == set(df.schema().column_names())


def test_take_blobs_other_columns_preserved(lance_dataset: lance.LanceDataset) -> None:
    """take_blobs preserves non-blob columns with their original values."""
    df = daft.read_lance(lance_dataset.uri, default_scan_options={"with_row_id": True})
    df = take_blobs(df, lance_dataset, "blob")
    ids = df.sort("id").select("id").to_pydict()["id"]
    assert ids == [1, 2, 3, 4, 5]


def test_take_blobs_column_dtype_replaced(lance_dataset: lance.LanceDataset) -> None:
    """After take_blobs, the blob column is no longer the descriptor struct type."""
    df = daft.read_lance(lance_dataset.uri, default_scan_options={"with_row_id": True})
    df = take_blobs(df, lance_dataset, "blob")
    assert df.schema()["blob"].dtype != LANCE_BLOB_DESCRIPTOR_TYPE


def test_take_blobs_kinds(lance_dataset: lance.LanceDataset) -> None:
    """Test all take_blobs storage kinds in one test for efficiency and parity with earlier type-combined tests."""
    ds = lance_dataset
    df = daft.read_lance(ds.uri, default_scan_options={"with_row_id": True})
    df = take_blobs(df, ds, "blob")
    blobs = df.to_pydict()["blob"]

    # 0: inline
    assert blobs[0].read() == b"tiny-inline-data"
    # 1: packed
    assert blobs[1].read() == b"x" * 100_000
    # 2: dedicated
    assert blobs[2].read() == b"y" * 5_000_000
    # 3: external full
    assert blobs[3].read() == b"\x00" * EXTERNAL_FILE_SIZE
    # 4: external slice
    assert blobs[4].read() == b"\x00" * 4096


def test_take_blobs_single_row(lance_dataset: lance.LanceDataset) -> None:
    """take_blobs works correctly when the DataFrame contains exactly one row."""
    df = daft.read_lance(lance_dataset.uri, default_scan_options={"with_row_id": True})
    df = df.where(col("id") == 1)
    df = take_blobs(df, lance_dataset, "blob")
    rows = df.to_pydict()
    assert len(rows["id"]) == 1
    assert rows["blob"][0].read() == b"tiny-inline-data"


def test_take_blobs_all_rows(lance_dataset: lance.LanceDataset) -> None:
    """take_blobs materializes all 5 rows and none are None."""
    df = daft.read_lance(lance_dataset.uri, default_scan_options={"with_row_id": True})
    df = take_blobs(df, lance_dataset, "blob")
    rows = df.to_pydict()
    assert len(rows["blob"]) == 5
    assert all(v is not None for v in rows["blob"])


def test_take_blobs_non_contiguous_rows(lance_dataset: lance.LanceDataset) -> None:
    """take_blobs works correctly when row IDs are non-contiguous (ids 1, 3, 5)."""
    df = daft.read_lance(lance_dataset.uri, default_scan_options={"with_row_id": True})
    df = df.where(col("id").is_in([1, 3, 5]))
    df = take_blobs(df, lance_dataset, "blob")
    rows = df.to_pydict()
    blobs = dict(zip(rows["id"], rows["blob"]))
    assert blobs[1].read() == b"tiny-inline-data"
    assert blobs[3].read() == b"y" * 5_000_000
    assert blobs[5].read() == b"\x00" * 4096
