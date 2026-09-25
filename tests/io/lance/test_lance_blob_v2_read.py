from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import lance
import pyarrow as pa
import pytest
from lance import Blob
from pytest import TempPathFactory

import daft
from daft import col
from daft_lance._blob import read_blobs, take_blobs

KIND_INLINE = 0
KIND_PACKED = 1
KIND_DEDICATED = 2
KIND_EXTERNAL = 3

# Large enough so the kind-3 slice (position=1024, size=4096) is in-range.
EXTERNAL_FILE_SIZE = 5120


@pytest.fixture(scope="module")
def lance_dataset(tmp_path_factory: TempPathFactory) -> lance.LanceDataset:
    """One row per storage kind: inline, packed, dedicated, external (full), external (slice), null, empty."""
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
        None,  # null
        b"",  # empty
    ]
    table = pa.table(
        {
            "id": pa.array([1, 2, 3, 4, 5, 6, 7], type=pa.int64()),
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


ON_RAY = os.environ.get("DAFT_RUNNER") == "ray"
native_only = pytest.mark.skipif(ON_RAY, reason="take_blobs returns BlobFile, which cannot cross Ray workers")
BLOB_FUNCS = [pytest.param(take_blobs, marks=native_only, id="take_blobs"), pytest.param(read_blobs, id="read_blobs")]


@pytest.mark.parametrize("func", BLOB_FUNCS)
def test_blobs_missing_column(lance_dataset: lance.LanceDataset, func: Any) -> None:
    """Raises ValueError when the requested column is absent from the schema."""
    df = daft.read_lance(lance_dataset.uri, default_scan_options={"with_row_id": True})
    with pytest.raises(ValueError, match="nonexistent"):
        func(df, lance_dataset, "nonexistent")


@pytest.mark.parametrize("func", BLOB_FUNCS)
def test_blobs_missing_rowid(lance_dataset: lance.LanceDataset, func: Any) -> None:
    """Raises ValueError when _rowid is absent (dataset not read with row IDs)."""
    df = daft.read_lance(lance_dataset.uri)  # no with_row_id=True
    with pytest.raises(ValueError, match="Row ids"):
        func(df, lance_dataset, "blob")


@pytest.mark.parametrize("func", BLOB_FUNCS)
def test_blobs_wrong_column_type(lance_dataset: lance.LanceDataset, func: Any) -> None:
    """Raises ValueError when the target column is not a blob descriptor column."""
    df = daft.read_lance(lance_dataset.uri, default_scan_options={"with_row_id": True})
    with pytest.raises(ValueError, match="Lance blob"):
        func(df, lance_dataset, "id")


@pytest.mark.parametrize("func", BLOB_FUNCS)
def test_blobs_no_extra_columns(lance_dataset: lance.LanceDataset, func: Any) -> None:
    """The descriptor column is replaced in-place; column names are unchanged."""
    df = daft.read_lance(lance_dataset.uri, default_scan_options={"with_row_id": True})
    before = df.schema().column_names()
    df = func(df, lance_dataset, "blob")
    assert isinstance(df, daft.DataFrame)
    assert df.schema().column_names() == before


@pytest.mark.parametrize("func", BLOB_FUNCS)
def test_blobs_other_columns_preserved(lance_dataset: lance.LanceDataset, func: Any) -> None:
    """Non-blob columns keep their original values."""
    df = daft.read_lance(lance_dataset.uri, default_scan_options={"with_row_id": True})
    df = func(df, lance_dataset, "blob")
    ids = df.sort("id").select("id").to_pydict()["id"]
    assert ids == [1, 2, 3, 4, 5, 6, 7]


# take_blobs: lazy lance.BlobFile handles, native runner only.


@native_only
def test_take_blobs_column_dtype_replaced(lance_dataset: lance.LanceDataset) -> None:
    """After take_blobs, the blob column holds Python objects instead of the descriptor struct."""
    df = daft.read_lance(lance_dataset.uri, default_scan_options={"with_row_id": True})
    df = take_blobs(df, lance_dataset, "blob")
    assert df.schema()["blob"].dtype == daft.DataType.python()


@native_only
def test_take_blobs_kinds(lance_dataset: lance.LanceDataset) -> None:
    """take_blobs returns a readable BlobFile for every storage kind, and None for a null blob."""
    ds = lance_dataset
    df = daft.read_lance(ds.uri, default_scan_options={"with_row_id": True})
    df = take_blobs(df, ds, "blob")
    blobs = df.sort("id").to_pydict()["blob"]

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
    # 5: null
    assert blobs[5] is None
    # 6: empty
    assert blobs[6].read() == b""


@native_only
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


def test_take_blobs_rejects_inferred_ray_runner(
    lance_dataset: lance.LanceDataset, monkeypatch: pytest.MonkeyPatch
) -> None:
    """take_blobs fails fast, pointing at read_blobs, when the runner is (inferred to be) Ray."""
    monkeypatch.setattr(daft.runners, "get_or_infer_runner_type", lambda: "ray")
    df = daft.read_lance(lance_dataset.uri, default_scan_options={"with_row_id": True})
    with pytest.raises(ValueError, match="read_blobs"):
        take_blobs(df, lance_dataset, "blob")


@pytest.mark.skipif(not ON_RAY, reason="needs the Ray runner")
def test_take_blobs_rejects_ray_runner(lance_dataset: lance.LanceDataset) -> None:
    """On a real Ray runner, take_blobs fails fast instead of hitting a pickle error later."""
    df = daft.read_lance(lance_dataset.uri, default_scan_options={"with_row_id": True})
    with pytest.raises(ValueError, match="read_blobs"):
        take_blobs(df, lance_dataset, "blob")


# read_blobs: full payloads as binary, any runner.


def test_read_blobs_column_dtype_replaced(lance_dataset: lance.LanceDataset) -> None:
    """After read_blobs, the blob column is binary instead of the descriptor struct type."""
    df = daft.read_lance(lance_dataset.uri, default_scan_options={"with_row_id": True})
    df = read_blobs(df, lance_dataset, "blob")
    assert df.schema()["blob"].dtype == daft.DataType.binary()


def test_read_blobs_kinds(lance_dataset: lance.LanceDataset) -> None:
    """read_blobs returns the bytes for every storage kind, None for null and b"" for empty."""
    ds = lance_dataset
    df = daft.read_lance(ds.uri, default_scan_options={"with_row_id": True})
    df = read_blobs(df, ds, "blob")
    blobs = df.sort("id").to_pydict()["blob"]

    # 0: inline
    assert blobs[0] == b"tiny-inline-data"
    # 1: packed
    assert blobs[1] == b"x" * 100_000
    # 2: dedicated
    assert blobs[2] == b"y" * 5_000_000
    # 3: external full
    assert blobs[3] == b"\x00" * EXTERNAL_FILE_SIZE
    # 4: external slice
    assert blobs[4] == b"\x00" * 4096
    # 5: null
    assert blobs[5] is None
    # 6: empty
    assert blobs[6] == b""


def test_read_blobs_single_row(lance_dataset: lance.LanceDataset) -> None:
    """read_blobs works correctly when the DataFrame contains exactly one row."""
    df = daft.read_lance(lance_dataset.uri, default_scan_options={"with_row_id": True})
    df = df.where(col("id") == 1)
    df = read_blobs(df, lance_dataset, "blob")
    rows = df.to_pydict()
    assert len(rows["id"]) == 1
    assert rows["blob"][0] == b"tiny-inline-data"


def test_read_blobs_non_contiguous_rows(lance_dataset: lance.LanceDataset) -> None:
    """read_blobs works correctly when row IDs are non-contiguous (ids 1, 3, 5, 7)."""
    df = daft.read_lance(lance_dataset.uri, default_scan_options={"with_row_id": True})
    df = df.where(col("id").is_in([1, 3, 5, 7]))
    df = read_blobs(df, lance_dataset, "blob")
    rows = df.to_pydict()
    blobs = dict(zip(rows["id"], rows["blob"]))
    assert blobs[1] == b"tiny-inline-data"
    assert blobs[3] == b"y" * 5_000_000
    assert blobs[5] == b"\x00" * 4096
    assert blobs[7] == b""


def test_read_blobs_null_rowid(lance_dataset: lance.LanceDataset) -> None:
    """Rows whose _rowid is null (e.g. after an outer join) get a None blob."""
    df = daft.read_lance(lance_dataset.uri, default_scan_options={"with_row_id": True})
    df = daft.from_pydict({"id": [1, 99, 3]}).join(df, on="id", how="left")
    df = read_blobs(df, lance_dataset, "blob")
    rows = df.sort("id").to_pydict()
    assert rows["id"] == [1, 3, 99]
    assert rows["blob"] == [b"tiny-inline-data", b"y" * 5_000_000, None]


def test_read_blobs_duplicate_rowids(lance_dataset: lance.LanceDataset) -> None:
    """Duplicate _rowid values each get their own copy of the blob."""
    df = daft.read_lance(lance_dataset.uri, default_scan_options={"with_row_id": True})
    df = daft.from_pydict({"id": [2, 1, 2]}).join(df, on="id", how="inner")
    df = read_blobs(df, lance_dataset, "blob")
    rows = df.sort("id").to_pydict()
    assert rows["id"] == [1, 2, 2]
    assert rows["blob"] == [b"tiny-inline-data", b"x" * 100_000, b"x" * 100_000]


@pytest.mark.parametrize("batch_size", [0, -1])
def test_read_blobs_invalid_batch_size(lance_dataset: lance.LanceDataset, batch_size: int) -> None:
    """read_blobs rejects a batch_size below 1 before building the plan."""
    df = daft.read_lance(lance_dataset.uri, default_scan_options={"with_row_id": True})
    with pytest.raises(ValueError, match="batch_size"):
        read_blobs(df, lance_dataset, "blob", batch_size=batch_size)


@pytest.mark.skipif(ON_RAY, reason="monkeypatch does not reach Ray workers")
@pytest.mark.parametrize(("batch_size", "expected_max"), [(None, 16), (5, 5)])
def test_read_blobs_batch_size_bounds_reads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    batch_size: int | None,
    expected_max: int,
) -> None:
    """Each LanceDataset.read_blobs call gets at most batch_size ids, and io_buffer_size is forwarded."""
    values = [f"blob-{i}".encode() for i in range(40)]
    table = pa.table({"id": pa.array(range(40), type=pa.int64()), "blob": lance.blob_array(values)})
    ds = lance.write_dataset(table, str(tmp_path), data_storage_version="2.2")

    calls: list[tuple[int, int | None]] = []
    lance_read_blobs = lance.LanceDataset.read_blobs

    def recording_read_blobs(self: lance.LanceDataset, *args: Any, **kwargs: Any) -> Any:
        calls.append((len(kwargs["ids"]), kwargs["io_buffer_size"]))
        return lance_read_blobs(self, *args, **kwargs)

    monkeypatch.setattr(lance.LanceDataset, "read_blobs", recording_read_blobs)

    df = daft.read_lance(ds.uri, default_scan_options={"with_row_id": True})
    kwargs: dict[str, Any] = {"io_buffer_size": 1 << 20}
    if batch_size is not None:
        kwargs["batch_size"] = batch_size
    rows = read_blobs(df, ds, "blob", **kwargs).sort("id").to_pydict()

    assert rows["blob"] == values
    assert sum(n for n, _ in calls) == 40
    assert max(n for n, _ in calls) <= expected_max
    assert {buf for _, buf in calls} == {1 << 20}
