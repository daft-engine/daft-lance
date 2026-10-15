"""Every dataset a process opens must share one lance Session.

Without it each ``lance.dataset()`` builds its own ``Arc<Manifest>`` holding
the table's whole fragment list, plus its own index/metadata cache budgets and
object-store client. Daft opens one dataset per scan task, per maintenance UDF
instance and per writer, so on a many-fragment table that cost is paid again
for every concurrently open dataset.

How much that saves depends on how many of those opens share a process, which
differs by path and is measured rather than assumed: see
``test_ray_session_memory.py`` for the scan and maintenance paths, and
``test_compaction_loads_the_index_in_its_commit_not_in_its_tasks`` below for
where an indexed table's maintenance cost actually sits.
"""

from __future__ import annotations

import os
import pickle

import lance
import numpy as np
import pyarrow as pa
import pytest
from pytest import TempPathFactory

import daft
from daft_lance._blob import read_blobs
from daft_lance.namespace import (
    DatasetOpenContext,
    get_shared_session,
    open_dataset_from_open_kwargs,
    reset_shared_session,
)
from daft_lance.utils import construct_lance_dataset_handle

NUM_FRAGMENTS = 32

ON_RAY = os.environ.get("DAFT_RUNNER") == "ray"


def same_session(left: object, right: object) -> bool:
    """``Session.is_same_as`` exists at runtime; pylance 12's stub omits it."""
    return bool(left.is_same_as(right))  # type: ignore[attr-defined]


@pytest.fixture
def many_fragment_uri(tmp_path_factory: TempPathFactory) -> str:
    """A table with one fragment per row, so the manifest is dominated by fragments."""
    uri = str(tmp_path_factory.mktemp("many_fragments") / "table.lance")
    table = pa.table({"i": pa.array(range(NUM_FRAGMENTS), type=pa.int64())})
    lance.write_dataset(table, uri, mode="create", max_rows_per_file=1)
    return uri


@pytest.fixture(autouse=True)
def fresh_session() -> None:
    reset_shared_session()


def test_get_shared_session_returns_one_session_per_process() -> None:
    first = get_shared_session()
    assert get_shared_session() is first
    assert same_session(first, get_shared_session())


def test_reset_shared_session_builds_a_new_one() -> None:
    first = get_shared_session()
    reset_shared_session()
    assert not same_session(get_shared_session(), first)


def test_kill_switch_gives_every_open_its_own_session(monkeypatch: pytest.MonkeyPatch) -> None:
    """``DAFT_LANCE_SHARE_SESSION=0`` restores lance's per-open sessions.

    This is the only way to get pre-sharing behavior inside a Ray worker, so
    the A/B benchmark depends on it: a driver-side patch never reaches there.
    """
    monkeypatch.setenv("DAFT_LANCE_SHARE_SESSION", "0")
    unshared = get_shared_session()
    assert not same_session(get_shared_session(), unshared)

    monkeypatch.delenv("DAFT_LANCE_SHARE_SESSION")
    shared = get_shared_session()
    assert same_session(get_shared_session(), shared)
    assert not same_session(shared, unshared)


def test_scan_task_opens_share_the_session(many_fragment_uri: str) -> None:
    """``open_dataset_from_open_kwargs`` is what every scan task calls."""
    shared = get_shared_session()
    open_kwargs = {"version": 1}
    datasets = [open_dataset_from_open_kwargs(many_fragment_uri, dict(open_kwargs)) for _ in range(NUM_FRAGMENTS)]

    assert all(same_session(ds.session(), shared) for ds in datasets)
    assert all(len(ds.get_fragments()) == NUM_FRAGMENTS for ds in datasets)


def test_driver_and_worker_opens_share_the_session(many_fragment_uri: str) -> None:
    """The native runner plans and executes in one process, so these must match."""
    handle = construct_lance_dataset_handle(many_fragment_uri)
    worker_ds = handle.worker_open_context().open_pinned()

    assert same_session(handle.dataset.session(), worker_ds.session())
    assert worker_ds.version == handle.dataset.version


def test_open_context_still_pickles(many_fragment_uri: str) -> None:
    """The session must never become a field: it does not pickle, the context must."""
    context = DatasetOpenContext.from_dataset(lance.dataset(many_fragment_uri), many_fragment_uri)

    restored = pickle.loads(pickle.dumps(context))

    assert restored == context
    assert same_session(restored.open_pinned().session(), get_shared_session())


def test_lance_session_itself_does_not_pickle() -> None:
    with pytest.raises(TypeError):
        pickle.dumps(get_shared_session())


@pytest.mark.skipif(ON_RAY, reason="the spy patches the driver process, which is not where a Ray worker runs")
def test_read_blobs_reopens_on_the_worker(tmp_path_factory: TempPathFactory, monkeypatch: pytest.MonkeyPatch) -> None:
    """The UDF must reopen the snapshot, not close over the driver's dataset.

    Closing over it pickles ``LanceDataset``, whose ``__reduce__`` carries the
    whole re-encoded manifest -- one copy per task -- and a dataset rebuilt
    from a serialized manifest also bypasses the session's manifest cache.
    """
    uri = str(tmp_path_factory.mktemp("blobs"))
    table = pa.table({"id": pa.array([1, 2], pa.int64()), "blob": lance.blob_array([b"first", b"second"])})
    ds = lance.write_dataset(table, uri, data_storage_version="2.2", max_rows_per_file=1)

    calls: list[dict[str, object]] = []

    def spy(ds_uri: str | None, open_kwargs: dict[str, object] | None) -> lance.LanceDataset:
        calls.append({"uri": ds_uri, **(open_kwargs or {})})
        return open_dataset_from_open_kwargs(ds_uri, open_kwargs)

    monkeypatch.setattr("daft_lance._blob.open_dataset_from_open_kwargs", spy)

    df = daft.read_lance(ds.uri, default_scan_options={"with_row_id": True})
    blobs = read_blobs(df, ds, "blob").sort("id").to_pydict()["blob"]

    assert blobs == [b"first", b"second"]
    assert calls, "read_blobs did not reopen the dataset on the worker"
    assert all(call["uri"] == ds.uri and call["version"] == ds.version for call in calls)


def test_compaction_loads_the_index_in_its_commit_not_in_its_tasks(tmp_path_factory: TempPathFactory) -> None:
    """On an indexed table, compaction's index cost lands on the driver.

    The worry this settles: a maintenance task opens the table itself, so on an
    indexed table every concurrent task might load the index and the cost would
    multiply by the concurrency. It does not. A task's open brings in the
    index's *metadata* entry and nothing more; the index data is read when the
    rewritten fragments are committed, because that is when every index entry
    has to be remapped onto the new fragment ids -- and the commit runs once,
    on the driver, on ``lance_ds`` rather than on the workers' reopened copy.

    Run in-process on purpose: it needs the tasks and the commit to be
    separable, which ``compact_files`` does not expose. ``CompactionTaskUDF`` is
    exactly what Daft instantiates on a worker, so calling it directly measures
    the same opens.

    The numbers come from ``index_cache_size_bytes()``, which is the accounting
    lance caps its index budget with rather than resident bytes (it reads about
    seven times what ``size_bytes()`` and the process's own RSS move by). Only
    growth within that one accounting is compared here, never against a size in
    bytes.
    """
    from lance.optimize import Compaction, CompactionOptions

    from daft_lance.lance_compaction import CompactionTaskUDF

    uri = str(tmp_path_factory.mktemp("compact_index") / "table.lance")
    rows, dim = 512, 8
    rng = np.random.default_rng(0)
    vectors = rng.random((rows, dim), dtype=np.float32)
    table = pa.table(
        {
            "i": pa.array(range(rows), type=pa.int64()),
            "v": pa.FixedSizeListArray.from_arrays(pa.array(vectors.reshape(-1), type=pa.float32()), dim),
        }
    )
    dataset = lance.write_dataset(table, uri, max_rows_per_file=rows // 16)
    dataset.create_index("v", index_type="IVF_PQ", num_partitions=4, num_sub_vectors=2)

    session = get_shared_session()
    handle = construct_lance_dataset_handle(uri)
    plan = Compaction.plan(handle.dataset, CompactionOptions(target_rows_per_fragment=rows // 4))
    assert plan.num_tasks() > 1, "nothing to compact, so this measured no task opens at all"

    before_tasks = session.index_cache_size_bytes()
    worker = CompactionTaskUDF(handle.worker_open_context())
    rewrites = [worker(task) for task in plan.tasks]
    after_tasks = session.index_cache_size_bytes()
    Compaction.commit(handle.dataset, rewrites)
    after_commit = session.index_cache_size_bytes()

    task_growth = after_tasks - before_tasks
    commit_growth = after_commit - after_tasks
    assert commit_growth > 3 * task_growth, (
        f"the {plan.num_tasks()} compaction tasks grew the index cache by {task_growth} bytes and the commit by "
        f"{commit_growth}; the index is no longer remapped in the commit, so a maintenance task's index cost may "
        "now multiply by the concurrency and benchmarks/lance_session_memory.py needs revisiting"
    )
