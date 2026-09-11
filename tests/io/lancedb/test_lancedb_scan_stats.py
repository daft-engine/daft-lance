from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pytest

import daft
from daft.subscribers import Subscriber
from daft.subscribers.events import Stats
from daft_lance.lance_scan import _LanceBatchIterator, _lancedb_table_factory_function

lance = pytest.importorskip("lance")

NUM_FRAGMENTS = 3
ROWS_PER_FRAGMENT = 500


@pytest.fixture(scope="function")
def lance_dataset_path(tmp_path: Path) -> str:
    for frag_idx in range(NUM_FRAGMENTS):
        base = frag_idx * ROWS_PER_FRAGMENT
        tbl = pa.Table.from_pydict(
            {
                "big_int": list(range(base, base + ROWS_PER_FRAGMENT)),
                "payload": [f"row-{i:06d}" * 4 for i in range(base, base + ROWS_PER_FRAGMENT)],
            }
        )
        lance.write_dataset(tbl, tmp_path, mode="append" if frag_idx > 0 else None)
    return str(tmp_path)


def _drain_and_snapshot(it: _LanceBatchIterator) -> tuple[int, list[dict[str, int]]]:
    """Drain the iterator the way Daft does: poll ``stats()`` after every batch and once at the end."""
    snapshots: list[dict[str, int]] = []
    rows = 0
    for rb in it:
        rows += len(rb)
        snapshots.append(it.stats())
    snapshots.append(it.stats())
    return rows, snapshots


def _assert_monotonic(snapshots: list[dict[str, int]]) -> None:
    for prev, cur in zip(snapshots, snapshots[1:]):
        assert cur["bytes.read"] >= prev["bytes.read"]
        assert cur["requests"] >= prev["requests"]


# ---------------------------------------------------------------------------
# Unit tests: call the factory directly (no dependency on the Daft executor).
# ---------------------------------------------------------------------------


def test_factory_returns_iterator_with_stats(lance_dataset_path: str) -> None:
    it = _lancedb_table_factory_function(ds_uri=lance_dataset_path, fragment_ids=[0])
    assert isinstance(it, Iterator)
    assert iter(it) is it
    assert callable(it.stats)
    # Nothing has been read yet.
    assert it.stats() == {"bytes.read": 0, "requests": 0}


def test_factory_stats_per_fragment_path(lance_dataset_path: str) -> None:
    ds = lance.dataset(lance_dataset_path)
    frag_ids = [f.fragment_id for f in ds.get_fragments()]
    it = _lancedb_table_factory_function(ds_uri=lance_dataset_path, fragment_ids=frag_ids)
    rows, snapshots = _drain_and_snapshot(it)

    assert rows == NUM_FRAGMENTS * ROWS_PER_FRAGMENT
    final = snapshots[-1]
    assert set(final) == {"bytes.read", "requests"}
    assert final["bytes.read"] > 0
    assert final["requests"] > 0
    _assert_monotonic(snapshots)
    # Cumulative: repeated polling after exhaustion does not grow the counters.
    assert it.stats() == final
    assert it.stats() == final


def test_factory_stats_index_driven_path(lance_dataset_path: str) -> None:
    it = _lancedb_table_factory_function(ds_uri=lance_dataset_path, fragment_ids=None)
    rows, snapshots = _drain_and_snapshot(it)

    assert rows == NUM_FRAGMENTS * ROWS_PER_FRAGMENT
    final = snapshots[-1]
    assert final["bytes.read"] > 0
    assert final["requests"] > 0
    _assert_monotonic(snapshots)
    assert it.stats() == final


def test_factory_stats_index_driven_with_filter_and_limit(lance_dataset_path: str) -> None:
    it = _lancedb_table_factory_function(
        ds_uri=lance_dataset_path,
        fragment_ids=None,
        required_columns=["big_int"],
        filter=pc.greater_equal(pc.field("big_int"), pc.scalar(ROWS_PER_FRAGMENT)),
        limit=7,
    )
    rows, snapshots = _drain_and_snapshot(it)
    assert rows == 7
    assert snapshots[-1]["bytes.read"] > 0
    assert it.stats() == snapshots[-1]


def test_factory_stats_per_fragment_with_limit_stops_early(lance_dataset_path: str) -> None:
    """The limit stops iteration after the first fragment, so only that scanner contributes stats."""
    ds = lance.dataset(lance_dataset_path)
    frag_ids = [f.fragment_id for f in ds.get_fragments()]

    limited = _lancedb_table_factory_function(ds_uri=lance_dataset_path, fragment_ids=frag_ids, limit=10)
    rows, snapshots = _drain_and_snapshot(limited)
    assert rows == 10
    limited_final = snapshots[-1]
    assert limited_final["bytes.read"] > 0

    full = _lancedb_table_factory_function(ds_uri=lance_dataset_path, fragment_ids=frag_ids)
    _, full_snapshots = _drain_and_snapshot(full)
    assert full_snapshots[-1]["bytes.read"] > limited_final["bytes.read"]
    assert full_snapshots[-1]["requests"] > limited_final["requests"]


def test_factory_stats_include_fragment_id(lance_dataset_path: str) -> None:
    it = _lancedb_table_factory_function(ds_uri=lance_dataset_path, fragment_ids=[0, 1], include_fragment_id=True)
    batches = list(it)
    assert all("fragment_id" in rb.schema().names() for rb in batches)
    assert it.stats()["bytes.read"] > 0


def test_batch_iterator_accumulates_callback_values() -> None:
    """Pure unit test of the wrapper: callback values are summed, and stats() is cumulative."""

    class _FakeScanStatistics:
        def __init__(self, bytes_read: int, requests: int) -> None:
            self.bytes_read = bytes_read
            self.requests = requests
            self.iops = requests

    def _make_batches(record: Any) -> Iterator[Any]:
        record(_FakeScanStatistics(100, 2))
        yield "batch-1"
        record(_FakeScanStatistics(50, 1))
        yield "batch-2"
        record(_FakeScanStatistics(0, 0))

    it: Any = _LanceBatchIterator(_make_batches)
    assert it.stats() == {"bytes.read": 0, "requests": 0}
    assert next(it) == "batch-1"
    assert it.stats() == {"bytes.read": 100, "requests": 2}
    assert next(it) == "batch-2"
    assert it.stats() == {"bytes.read": 150, "requests": 3}
    with pytest.raises(StopIteration):
        next(it)
    assert it.stats() == {"bytes.read": 150, "requests": 3}
    assert it.stats() == {"bytes.read": 150, "requests": 3}


# ---------------------------------------------------------------------------
# End-to-end: Daft's executor polls ``stats()`` and surfaces it as ``bytes.read``
# on the scan node. Only supported by Daft builds that expose DataSourceTask.stats().
# ---------------------------------------------------------------------------

_DAFT_SUPPORTS_FACTORY_STATS = hasattr(getattr(daft.io, "DataSourceTask", None), "stats")


class _StatsRecorder(Subscriber):
    def __init__(self) -> None:
        self.stats_events: list[Stats] = []
        self.scan_node_ids: set[int] = set()

    def on_operator_start(self, event: Any) -> None:
        if "lance" in event.name.lower() or "scan" in event.name.lower():
            self.scan_node_ids.add(event.node_id)

    def on_stats(self, event: Stats) -> None:
        self.stats_events.append(event)

    def scan_bytes_read(self) -> list[int]:
        """``bytes.read`` reported for scan nodes, in the order the Stats events arrived."""
        values: list[int] = []
        for event in self.stats_events:
            for node_id, node_stats in event.stats.items():
                if node_id not in self.scan_node_ids:
                    continue
                if "bytes.read" in node_stats:
                    values.append(int(node_stats["bytes.read"][1]))
        return values


def _run_with_recorder(df: daft.DataFrame) -> _StatsRecorder:
    recorder = _StatsRecorder()
    alias = f"lance-scan-stats-{id(recorder)}"
    daft.attach_subscriber(alias, recorder)
    try:
        df.collect()
    finally:
        daft.detach_subscriber(alias)
    return recorder


@pytest.mark.skipif(not _DAFT_SUPPORTS_FACTORY_STATS, reason="Daft build does not support factory iterator stats()")
def test_scan_node_reports_bytes_read_per_fragment_path(lance_dataset_path: str) -> None:
    df = daft.read_lance(lance_dataset_path)
    recorder = _run_with_recorder(df)

    values = recorder.scan_bytes_read()
    assert values, "no bytes.read stat was emitted for the scan node"
    assert max(values) > 0
    # Stats are cumulative on the iterator, so Daft's delta folding must not re-add them:
    # every Stats event reports a value no larger than the final one, and the final value
    # matches what Lance reports for a full scan of the dataset.
    final = values[-1]
    assert all(v <= final for v in values)

    expected = 0

    def _record(st: Any) -> None:
        nonlocal expected
        expected += st.bytes_read

    ds = lance.dataset(lance_dataset_path)
    for fragment in ds.get_fragments():
        list(
            ds.scanner(
                fragments=[fragment], scan_stats_callback=_record, blob_handling="blobs_descriptions"
            ).to_batches()
        )
    assert final == expected


@pytest.mark.skipif(not _DAFT_SUPPORTS_FACTORY_STATS, reason="Daft build does not support factory iterator stats()")
def test_scan_node_reports_bytes_read_index_driven_path(lance_dataset_path: str) -> None:
    ds = lance.dataset(lance_dataset_path)
    ds.create_scalar_index("big_int", index_type="BTREE")

    df = daft.read_lance(lance_dataset_path).where(daft.col("big_int") == 42)
    recorder = _run_with_recorder(df)

    values = recorder.scan_bytes_read()
    assert values, "no bytes.read stat was emitted for the scan node"
    final = values[-1]
    assert final > 0
    assert all(v <= final for v in values)
