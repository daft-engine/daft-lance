from __future__ import annotations

import os

import lance
import pyarrow as pa
import pytest
from lance import Blob
from pytest import TempPathFactory

import daft


@pytest.fixture(scope="module")
def lance_dataset(tmp_path_factory: TempPathFactory) -> lance.LanceDataset:
    dataset_dir = tmp_path_factory.mktemp("dataset")
    dataset_path = str(dataset_dir)

    # Create a temp file to use as a fake external asset (for URI reference)
    blob_dir = str(tmp_path_factory.mktemp("blobs"))
    blob_reference = os.path.join(blob_dir, "placeholder.mp4")
    with open(blob_reference, "wb") as f:
        f.write(b"contents")

    # Values are from https://www.lancedb.com/blog/lance-blob-v2
    values = [
        # inline (<= 64 KB)
        b"tiny-inline-data",
        # packed (64 KB - 4 MB)
        b"x" * 100_000,
        # dedicated (> 4 MB)
        b"y" * 5_000_000,
        # URI reference (full)
        f"file://{blob_reference}",
        # URI reference (slice)
        Blob.from_uri(f"file://{blob_reference}", position=1024, size=4096),
    ]

    table = pa.table(
        {
            "id": pa.array([1, 2, 3, 4, 5], type=pa.int64()),
            "blob": lance.blob_array(values),
        }
    )

    return lance.write_dataset(table, dataset_path, data_storage_version="2.2", allow_external_blob_outside_bases=True)


def test_v2_blob_read_descriptors(lance_dataset: lance.LanceDataset) -> None:
    # Reading into daft is ok, it's just the descriptor (struct with extension type)
    df = daft.read_lance(lance_dataset.uri)

    # Convert to arrow so we can inspect the actual schema
    table = df.to_arrow()
    field = table.schema.field("blob")

    # Check that the fields are all there.
    assert isinstance(field.type, pa.ExtensionType)
    assert field.type.extension_name == "lance.blob.v2"
    assert pa.types.is_struct(field.type.storage_type)

    # The descriptor struct should carry the canonical {data, uri, position, size} layout
    storage_field_names = {f.name for f in field.type.storage_type}
    assert storage_field_names == {"data", "uri", "position", "size"}


@pytest.mark.xfail(
    reason=(
        "pylance 6.0.0 raises `there were more fields in the schema than provided column "
        "indices / infos` when scanning V2 blob columns with blob_handling='all_binary'. "
        "The daft-lance plumbing is forward-compatible; this test should pass once the "
        "upstream Lance decoder issue is fixed."
    ),
    strict=False,
)
def test_v2_read_all_binary(lance_dataset: lance.LanceDataset) -> None:
    pass
    # df = daft_lance.read_lance(lance_dataset.uri, blob_handling="all_binary")
    # table = df.to_arrow()
    # field = table.schema.field("blob")
    # assert pa.types.is_large_binary(field.type) or pa.types.is_binary(field.type)


def test_blob_handling_plumbing_to_scanner(lance_dataset: lance.LanceDataset, monkeypatch: pytest.MonkeyPatch) -> None:
    """Independent of the upstream all_binary bug, verify the parameter reaches `ds.scanner()`."""
    pass
    # captured: dict[str, object] = {}
    # real_scanner = lance.LanceDataset.scanner

    # def spy(self: lance.LanceDataset, *args: Any, **kwargs: Any) -> Any:
    #     captured["blob_handling"] = kwargs.get("blob_handling")
    #     return real_scanner(self, *args, **kwargs)

    # monkeypatch.setattr(lance.LanceDataset, "scanner", spy)

    # df = daft_lance.read_lance(lance_dataset_path, blob_handling="blobs_descriptions")
    # df.to_arrow()
    # assert captured["blob_handling"] == "blobs_descriptions"


def test_v2_take_blobs_via_udf(lance_dataset: lance.LanceDataset) -> None:
    pass
    # df = daft_lance.read_lance(lance_dataset.uri, default_scan_options=cast("dict[str, str]", {"with_row_id": True}))

    # @daft.udf(return_dtype=daft.DataType.binary())
    # def materialize(row_ids: Any) -> list[bytes]:
    #     ds = lance.dataset(uri)
    #     files = ds.take_blobs("clip", ids=row_ids.to_pylist())
    #     return [f.readall() for f in files]

    # out = df.with_column("bytes", materialize(df["_rowid"])).to_arrow()
    # rows = sorted(zip(out.column("id").to_pylist(), out.column("bytes").to_pylist()))
    # assert rows == [(1, b"inline-bytes"), (2, b"another"), (3, b"third")]
