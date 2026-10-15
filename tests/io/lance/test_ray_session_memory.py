"""Under Ray, the dataset opens happen in a worker process, not in the driver.

``daft_lance`` hands one process-level ``lance.Session`` to every open, so a
table's fragment list is held once per process instead of once per scan task,
blob reader and writer. None of that is observable from the driver: a
monkeypatch or a spy only ever patches the driver, which is why the spy tests in
``test_shared_session.py`` are native-only.

So these ask the worker itself. A plain batch UDF is fused into the scan's own
``SwordfishTask``, so a UDF that reports ``Session.size_bytes()`` reports it from
the very process that opened the datasets. ``size_bytes()`` is used rather than
RSS on purpose: it is exact, it needs no large table, and it is zero for any open
that did not go through the process session -- which is exactly what the
pre-sharing behavior looks like. ``benchmarks/lance_session_memory.py`` is the
counterpart that puts megabytes on it.

The last two tests cover the maintenance path, which behaves differently enough
to be worth stating: its opens do not multiply by concurrency, because Daft runs
each ``daft.cls`` instance in a process of its own. They assert that, so the day
it stops being true is the day the tests say so.
"""

from __future__ import annotations

import json
import os
from typing import Any

import lance
import numpy as np
import pyarrow as pa
import pytest
from pytest import TempPathFactory

import daft
import daft_lance
from daft_lance._blob import read_blobs
from daft_lance.utils import construct_lance_dataset_handle

ray = pytest.importorskip("ray")

pytestmark = pytest.mark.skipif(
    os.environ.get("DAFT_RUNNER") != "ray",
    reason="a worker process's lance session is only observable under the Ray runner",
)

NUM_FRAGMENTS = 32
DRIVER_FLAG = os.environ.get("DAFT_LANCE_SHARE_SESSION")


def report_from_worker() -> dict[int, dict[str, Any]]:
    """Ask every worker that runs a task for its lance session state.

    The probe is defined here, not at module scope, so cloudpickle ships it by
    value and the worker does not have to import this test module. It must stay
    a plain batch UDF: ``concurrency=`` or ``use_process=True`` would move it
    into a process of its own, and it would then describe the wrong process.
    """

    @daft.func.batch(return_dtype=daft.DataType.string())
    def probe(column: daft.Series) -> list[str]:
        from daft_lance.namespace import get_shared_session

        session = get_shared_session()
        payload = json.dumps(
            {
                "pid": os.getpid(),
                "session_size": session.size_bytes(),
                "session_obj_id": id(session),
                # ``is_same_as`` exists at runtime; pylance 12's stub omits it.
                "same_session": bool(session.is_same_as(get_shared_session())),  # type: ignore[attr-defined]
                "flag": os.environ.get("DAFT_LANCE_SHARE_SESSION"),
            }
        )
        return [payload] * len(column)

    df = daft.from_pydict({"x": [0]}).with_column("p", probe(daft.col("x")))
    rows = [json.loads(value) for value in df.to_pydict()["p"]]
    return {row["pid"]: row for row in rows}


def one_worker(report: dict[int, dict[str, Any]]) -> dict[str, Any]:
    assert len(report) == 1, f"expected a single worker process, got pids {sorted(report)}"
    return next(iter(report.values()))


def session_growth(before: dict[int, dict[str, Any]], after: dict[int, dict[str, Any]]) -> int:
    """How much the worker's process-wide session grew between two reports."""
    assert sorted(before) == sorted(after), (
        f"the reports ran on different workers ({sorted(before)} then {sorted(after)}), so nothing can be compared"
    )
    pid = next(iter(before))
    return int(after[pid]["session_size"]) - int(before[pid]["session_size"])


def write_table(uri: str, fragments: int = NUM_FRAGMENTS) -> lance.LanceDataset:
    """One row per fragment, so the manifest is mostly its fragment list."""
    table = pa.table({"i": pa.array(range(fragments), type=pa.int64())})
    return lance.write_dataset(table, uri, mode="create", max_rows_per_file=1)


def test_worker_sees_the_drivers_share_session_flag() -> None:
    """Every other test here, and the A/B benchmark, rests on this.

    Ray actors do not inherit the driver's environment by contract, so if the
    kill switch stopped reaching the worker, the benchmark would quietly compare
    two identical runs and report a flattering number.
    """
    worker = one_worker(report_from_worker())

    assert worker["flag"] == DRIVER_FLAG


def test_scan_tasks_open_through_the_process_session(tmp_path_factory: TempPathFactory) -> None:
    uri = str(tmp_path_factory.mktemp("read") / "table.lance")
    dataset = write_table(uri)

    before = report_from_worker()
    # Never count_rows(): count pushdown collapses the plan to a single task
    # that opens no fragment at all.
    rows = daft_lance.read_lance(uri).to_arrow().num_rows
    after = report_from_worker()

    assert rows == NUM_FRAGMENTS
    growth = session_growth(before, after)
    assert growth >= len(dataset._ds.serialized_manifest()), (
        f"the worker's session grew by only {growth} bytes, which cannot hold this table's manifest: "
        "the scan tasks opened their datasets outside the process session"
    )
    assert one_worker(after)["same_session"]


def test_scan_tasks_reuse_one_session_per_process(tmp_path_factory: TempPathFactory) -> None:
    """Two queries in a row must land on the same session object, not two."""
    uri = str(tmp_path_factory.mktemp("reuse") / "table.lance")
    write_table(uri)

    daft_lance.read_lance(uri).to_arrow()
    first = one_worker(report_from_worker())
    daft_lance.read_lance(uri).to_arrow()
    second = one_worker(report_from_worker())

    assert first["same_session"] and second["same_session"], (
        "the worker handed out a fresh session per call, so nothing is shared between its tasks"
    )
    assert first["session_obj_id"] == second["session_obj_id"], (
        "the two queries saw different session objects, so the session is per-query, not per-process"
    )
    # The second read of the same pinned snapshot is served from the manifest
    # cache, so it adds nothing.
    assert second["session_size"] == first["session_size"], (
        "re-reading the same pinned snapshot grew the session again: the manifest cache was not hit"
    )


def test_writes_open_through_the_process_session(tmp_path_factory: TempPathFactory) -> None:
    """``write_fragments`` reloads the manifest per flush unless it gets a session."""
    uri = str(tmp_path_factory.mktemp("write") / "table.lance")
    write_table(uri)

    before = report_from_worker()
    daft.from_pydict({"i": list(range(8))}).write_lance(uri, mode="append", max_rows_per_file=1).collect()
    after = report_from_worker()

    assert session_growth(before, after) > 0, (
        "the worker's session did not grow across the write: the writer opened outside it"
    )
    assert lance.dataset(uri).count_rows() == NUM_FRAGMENTS + 8


def test_indexed_queries_load_the_index_once_per_process(tmp_path_factory: TempPathFactory) -> None:
    """A vector index is the other half of what a session holds, and the costly half.

    A session carries an index cache as well as a metadata cache, and index size
    scales with rows where a manifest scales with fragments: measured on a
    200k-row table, one IVF_PQ index costs about 5 MB per session, against 9 KB
    of manifest. A ``nearest`` read plans a single index-driven scan task, so
    nothing here multiplies by concurrency -- what sharing changes is that the
    index is loaded once per worker process instead of once per query, which is
    a storage round trip and a decode every query without it.
    """
    uri = str(tmp_path_factory.mktemp("indexed") / "table.lance")
    rows, dim = 512, 8
    rng = np.random.default_rng(0)
    vectors = rng.random((rows, dim), dtype=np.float32)
    table = pa.table(
        {
            "id": pa.array(range(rows), type=pa.int64()),
            "v": pa.FixedSizeListArray.from_arrays(pa.array(vectors.reshape(-1), type=pa.float32()), dim),
        }
    )
    dataset = lance.write_dataset(table, uri, max_rows_per_file=128)
    dataset.create_index("v", index_type="IVF_PQ", num_partitions=4, num_sub_vectors=2)

    # The same vector every time, on purpose: a different one probes different
    # IVF partitions and legitimately loads more of the index, which would make
    # "was it reloaded?" unanswerable on a table this small.
    query = rng.random(dim, dtype=np.float32)

    def search() -> int:
        nearest = {"column": "v", "q": query, "k": 4}
        return daft_lance.read_lance(uri, default_scan_options={"nearest": nearest}).to_arrow().num_rows

    before = report_from_worker()
    assert search() == 4
    after_first = report_from_worker()
    assert search() == 4
    after_second = report_from_worker()

    first_growth = session_growth(before, after_first)
    assert first_growth > 0, (
        "the worker's session did not grow across an indexed query: the index was loaded into a session of its own"
    )
    # The second query must be served from the index cache the first one filled.
    assert session_growth(after_first, after_second) == 0, (
        "the second indexed query grew the session again, so it reloaded the index instead of reusing it"
    )


def test_read_blobs_opens_through_the_process_session(tmp_path_factory: TempPathFactory) -> None:
    """The blob reader reopens the pinned snapshot on the worker.

    That the reopen uses a uri plus open kwargs -- rather than closing over the
    driver's ``LanceDataset``, whose ``__reduce__`` ships the whole re-encoded
    manifest per task -- is asserted by the spy test in ``test_shared_session``,
    which can only run natively. What is checked here is the other half: under
    Ray the payload really does travel and the open really does land in the
    worker's process-wide session.
    """
    uri = str(tmp_path_factory.mktemp("blobs") / "table.lance")
    rows = 4
    table = pa.table(
        {
            "id": pa.array(range(rows), type=pa.int64()),
            "blob": lance.blob_array([b"payload-%d" % i for i in range(rows)]),
        }
    )
    dataset = lance.write_dataset(table, uri, data_storage_version="2.2", max_rows_per_file=1)

    before = report_from_worker()
    df = daft.read_lance(uri, default_scan_options={"with_row_id": True})
    blobs = read_blobs(df, dataset, "blob").sort("id").to_pydict()["blob"]
    after = report_from_worker()

    assert blobs == [b"payload-%d" % i for i in range(rows)]
    assert session_growth(before, after) > 0, (
        "the worker's session did not grow across the blob read: it opened outside the process session"
    )


MAINT_CONCURRENCY = 2


def maintenance_census(open_context: Any, concurrency: int) -> list[dict[str, Any]]:
    """Open the table the way a maintenance worker does, and report who did it.

    Daft's maintenance operators are ``daft.cls`` instances -- one open per
    instance, cached on ``self`` -- so what their memory costs turns entirely on
    where Daft puts those instances. The real operators cannot be asked (they
    return rewrite results, not pids), so this stands in for them, decorated the
    same way: ``max_concurrency`` set, ``use_process`` left alone.

    Defined inside the function so cloudpickle ships it by value, exactly as in
    :func:`report_from_worker`.
    """

    class OpenCensus:
        def __init__(self, context: Any) -> None:
            self.context = context
            self._lance_ds: lance.LanceDataset | None = None

        @daft.method.batch(return_dtype=daft.DataType.string())
        def __call__(self, column: daft.Series) -> list[str]:
            import json as json_
            import os as os_

            from daft_lance.namespace import get_shared_session

            if self._lance_ds is None:
                self._lance_ds = self.context.open_pinned()
            session = get_shared_session()
            payload = json_.dumps(
                {
                    "pid": os_.getpid(),
                    "instance": id(self),
                    "session_obj_id": id(session),
                    "session_size": session.size_bytes(),
                    "fragments": len(self._lance_ds.get_fragments()),
                }
            )
            return [payload] * len(column)

    census = daft.cls(OpenCensus, max_concurrency=concurrency)
    # One partition per instance several times over, so every instance in the
    # pool is given work and reports back.
    partitions = concurrency * 8
    df = daft.from_pydict({"x": list(range(partitions))}).into_partitions(partitions)
    rows = [json.loads(value) for value in df.select(census(open_context)(daft.col("x")).alias("p")).to_pydict()["p"]]
    instances = {(row["pid"], row["instance"]): row for row in rows}
    assert len(instances) >= 2, (
        f"only {len(instances)} instance ran at concurrency {concurrency}, so this measured nothing about concurrency"
    )
    return list(instances.values())


def test_maintenance_instances_each_get_their_own_process(tmp_path_factory: TempPathFactory) -> None:
    """Why concurrency does not multiply a maintenance worker's manifests.

    A scan task shares its process with ``scantask_max_parallel``-1 others, so
    without a shared session their manifests add up and sharing collapses them.
    A maintenance instance does not: Daft runs each one in a process of its own,
    so it opens the table exactly once however high the concurrency, and there
    is nothing for a session to collapse. This is asserted rather than assumed
    because it is the whole reason the maintenance path needs no saving -- if
    Daft ever fuses these instances into one process, the manifests start
    multiplying and both this file and the A/B benchmark need a stage for it.
    """
    uri = str(tmp_path_factory.mktemp("maint_procs") / "table.lance")
    write_table(uri)
    open_context = construct_lance_dataset_handle(uri).worker_open_context()

    instances = maintenance_census(open_context, MAINT_CONCURRENCY)

    pids = {row["pid"] for row in instances}
    assert len(pids) == len(instances), (
        f"{len(instances)} maintenance instances shared {len(pids)} processes. Daft used to give each its own; "
        "now that they overlap, their opens multiply within a process and that saving should be measured"
    )
    assert {row["fragments"] for row in instances} == {NUM_FRAGMENTS}, (
        "the instances disagreed about the table's fragment count, so they did not all open the pinned snapshot"
    )


def test_maintenance_workers_open_through_the_process_session(tmp_path_factory: TempPathFactory) -> None:
    """The open itself still has to go through the session, per process.

    One open per process is the floor, not zero: the worker pays one manifest
    read either way. What this checks is that it lands in the process-wide
    session rather than beside it, which is what keeps the floor at one -- a
    maintenance process that also scans, or that Daft later gives a second
    instance, then shares the one copy.
    """
    uri = str(tmp_path_factory.mktemp("maint_session") / "table.lance")
    dataset = write_table(uri)
    manifest_bytes = len(dataset._ds.serialized_manifest())
    open_context = construct_lance_dataset_handle(uri).worker_open_context()

    instances = maintenance_census(open_context, MAINT_CONCURRENCY)

    for row in instances:
        assert row["session_size"] >= manifest_bytes, (
            f"a maintenance instance's process session holds {row['session_size']} bytes, too little for this "
            f"table's {manifest_bytes}-byte manifest: it opened outside the process session"
        )
    assert len({row["session_obj_id"] for row in instances}) == len(instances), (
        "two instances in different processes reported the same session object id, which cannot be"
    )
