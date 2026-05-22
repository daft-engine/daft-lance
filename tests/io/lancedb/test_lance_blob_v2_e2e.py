"""End-to-end Lance Blob V2 in daft: read descriptors, transform, write blobs back.

The read side returns physical descriptors (lance.blob_descriptor.v2), the write
side accepts daft binary columns via the ``blob_columns=[...]`` opt-in. These
tests stitch the two halves together without materializing blob bytes — proving
that a typical pipeline (read metadata, filter, derive, persist fresh blobs)
runs end-to-end in daft against Lance V2.
"""

from __future__ import annotations

import lance
import pyarrow as pa
import pytest
from lance.blob import BlobType
from pytest import TempPathFactory

import daft

KIND_INLINE = 0
KIND_PACKED = 1
KIND_DEDICATED = 2
KIND_EXTERNAL = 3


@pytest.fixture(scope="module")
def source_dataset(tmp_path_factory: TempPathFactory) -> str:
    """Create a source dataset with blob data.

    Five rows spanning kinds 0, 1, 2 (skip external since the e2e flow does not
    write kind=3 from daft).
    """
    values = [
        b"alpha",  # inline
        b"x" * 100_000,  # packed
        b"y" * 5_000_000,  # dedicated
        b"beta",  # inline
        b"z" * 6_000_000,  # dedicated
    ]
    table = pa.table(
        {
            "id": pa.array([1, 2, 3, 4, 5], type=pa.int64()),
            "blob": lance.blob_array(values),
        }
    )
    path = str(tmp_path_factory.mktemp("source"))
    lance.write_dataset(table, path, data_storage_version="2.2")
    return path


@daft.func.batch(return_dtype=daft.DataType.uint8())
def descriptor_kind(blob_series):  # type: ignore[no-untyped-def]
    """Extract the `kind` field from a lance.blob_descriptor.v2 column."""
    arr = blob_series.to_arrow()
    storage = arr.storage if hasattr(arr, "storage") else arr
    return storage.field("kind")


@daft.func.batch(return_dtype=daft.DataType.uint64())
def descriptor_size(blob_series):  # type: ignore[no-untyped-def]
    arr = blob_series.to_arrow()
    storage = arr.storage if hasattr(arr, "storage") else arr
    return storage.field("size")


@daft.func.batch(return_dtype=daft.DataType.binary())
def make_payload(ids):  # type: ignore[no-untyped-def]
    """Synthesize a per-row payload from the id column."""
    return pa.array([f"payload-{i}".encode() for i in ids.to_pylist()], type=pa.large_binary())


def test_e2e_descriptor_filter_and_writeback(source_dataset: str, tmp_path_factory: TempPathFactory) -> None:
    """Read descriptors → filter on kind → derive fresh bytes → write back as blobs.

    Verifies the full loop: descriptor columns can drive transformations, and the
    sink writes fresh daft binary columns as Lance Blob V2 in the output dataset.
    """
    out_path = str(tmp_path_factory.mktemp("out"))

    df = daft.read_lance(source_dataset)
    df = df.with_column("kind", descriptor_kind(df["blob"])).with_column("size", descriptor_size(df["blob"]))
    # Keep only inline (kind=0) and packed (kind=1) rows.
    df = df.where(daft.col("kind") < KIND_DEDICATED)

    # Drop the descriptor column (cannot round-trip as logical) and synthesize a
    # fresh binary payload from the id column. The new column is what we persist
    # as a Lance Blob V2.
    df = df.exclude("blob").with_column("payload", make_payload(df["id"]))

    df.write_lance(out_path, blob_columns=["payload"]).collect()

    out_ds = lance.dataset(out_path)
    assert out_ds.count_rows() == 3
    payload_field = out_ds.schema.field("payload")
    assert isinstance(payload_field.type, BlobType)
    assert payload_field.type.extension_name == "lance.blob.v2"
    assert out_ds.data_storage_version == "2.2"

    # Reading the new dataset back through daft surfaces descriptors again.
    # Daft converts lance.blob.v2 → a plain struct with the physical descriptor fields.
    round_trip = daft.read_lance(out_path).to_arrow()
    blob_field = round_trip.schema.field("payload")
    assert pa.types.is_struct(blob_field.type)
    assert {f.name for f in blob_field.type} == {"kind", "position", "size", "blob_id", "blob_uri"}

    # Synthesized payloads are short — every one should land as inline.
    assert all(d["kind"] == KIND_INLINE for d in round_trip.column("payload").to_pylist())
    ids = sorted(round_trip.column("id").to_pylist())
    assert ids == [1, 2, 4]  # rows with kind < 2


def test_e2e_non_blob_passthrough(source_dataset: str, tmp_path_factory: TempPathFactory) -> None:
    """Non-blob projections produce a normal Lance dataset.

    Reading a blob dataset and writing non-blob projections produces a normal
    Lance dataset with no blob columns and untouched id values.
    """
    out_path = str(tmp_path_factory.mktemp("out"))

    df = daft.read_lance(source_dataset).select("id").with_column("id_plus_one", daft.col("id") + 1)
    df.write_lance(out_path).collect()

    out_ds = lance.dataset(out_path)
    assert set(out_ds.schema.names) == {"id", "id_plus_one"}
    for f in out_ds.schema:
        assert not isinstance(f.type, BlobType)

    out_tbl = out_ds.to_table().to_pydict()
    rows = sorted(zip(out_tbl["id"], out_tbl["id_plus_one"]))
    assert rows == [(1, 2), (2, 3), (3, 4), (4, 5), (5, 6)]
