"""Check worker-local Session identity and Blob serialization on the Ray runner."""

from __future__ import annotations

import os
from pathlib import Path

import lance
import pyarrow as pa
import pytest

import daft
from daft_lance._blob import read_blobs
from daft_lance.utils import construct_lance_dataset_handle

pytest.importorskip("ray")
pytestmark = pytest.mark.skipif(os.environ.get("DAFT_RUNNER") != "ray", reason="requires the Ray runner")


@pytest.mark.parametrize("sharing", [True, False])
def test_worker_opens_honor_session_sharing(tmp_path: Path, sharing: bool) -> None:
    uri = str(tmp_path / "table.lance")
    lance.write_dataset(pa.table({"i": [1, 2]}), uri)
    handle = construct_lance_dataset_handle(uri)
    context = handle.worker_open_context()
    open_kwargs = handle.open_kwargs
    driver_pid = os.getpid()
    # Advance latest so the serialized context must still select the old snapshot.
    lance.write_dataset(pa.table({"i": [3]}), uri, mode="append")

    @daft.func.batch(return_dtype=daft.DataType.bool())
    def check(column: daft.Series) -> list[bool]:
        from daft_lance.namespace import open_dataset_from_open_kwargs, reset_shared_session

        assert os.getpid() != driver_pid
        previous = os.environ.get("DAFT_LANCE_SHARE_SESSION")
        try:
            os.environ["DAFT_LANCE_SHARE_SESSION"] = "1" if sharing else "0"
            reset_shared_session()
            first = open_dataset_from_open_kwargs(uri, open_kwargs)
            second = open_dataset_from_open_kwargs(uri, open_kwargs)
            maintenance = context.open_pinned()
            # pylance exposes is_same_as at runtime but omits it from its stub.
            assert bool(first.session().is_same_as(second.session())) == sharing  # type: ignore[attr-defined]
            assert bool(first.session().is_same_as(maintenance.session())) == sharing  # type: ignore[attr-defined]
            assert first.to_table().to_pydict() == {"i": [1, 2]}
            assert maintenance.to_table().to_pydict() == {"i": [1, 2]}
            return [True] * len(column)
        finally:
            if previous is None:
                os.environ.pop("DAFT_LANCE_SHARE_SESSION", None)
            else:
                os.environ["DAFT_LANCE_SHARE_SESSION"] = previous
            reset_shared_session()

    result = daft.from_pydict({"x": [0]}).select(check(daft.col("x")).alias("ok")).to_pydict()
    assert result == {"ok": [True]}


def test_read_blobs_on_ray(tmp_path: Path) -> None:
    uri = str(tmp_path / "blobs.lance")
    payloads = [b"first", b"second"]
    table = pa.table({"id": [0, 1], "blob": lance.blob_array(payloads)})
    dataset = lance.write_dataset(table, uri, data_storage_version="2.2", max_rows_per_file=1)
    df = daft.read_lance(uri, default_scan_options={"with_row_id": True})
    assert read_blobs(df, dataset, "blob").sort("id").to_pydict()["blob"] == payloads
