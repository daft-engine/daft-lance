"""Measure what sharing one lance ``Session`` saves inside a real Ray worker.

``daft_lance`` passes one process-level ``lance.Session`` to every dataset it
opens (``daft_lance.namespace.get_shared_session``), so a table's fragment list
and lance's caches are held once per process instead of once per scan task,
maintenance UDF and writer. The numbers that motivated the change were measured
in a driver process, but under Ray the memory is spent in the worker, and a
driver-side monkeypatch cannot reach it. So this script runs the real pipeline
twice in two subprocesses that differ only in ``DAFT_LANCE_SHARE_SESSION``, and
reads the worker's memory back out of the worker itself:

* a probe UDF, fused into the scan's own ``SwordfishTask`` (no ``concurrency=``,
  no ``use_process=True``, or Daft would give it a process of its own), reports
  ``getrusage(RUSAGE_SELF).ru_maxrss`` -- the worker process's high-water mark --
  plus ``Session.size_bytes()`` and the flag value it actually sees;
* Daft's own sampler (``DAFT_PROCESS_MONITOR_ENABLED=1`` + ``DAFT_EVENT_LOG_DIR``)
  writes ``process.memory.rss`` every 200ms from inside each worker to
  ``<dir>/<query_id>/events.jsonl``, which is an independent second opinion.

The main stage is the shape that matters most in practice: **read Lance, write
Parquet**. It is the one the thresholds are derived from, and it is measured as
a write rather than an aggregation on purpose -- a task holds its parquet
writer's buffers at the same time as its manifest, so the two compete for the
same memory limit, which is what a real backfill does. An aggregation keeps
almost nothing resident and would flatter the worker.

Three more stages follow, and they make different claims. ``read_blobs`` and the
lance write share the main stage's shape: ``--parallel`` concurrent tasks in one
worker process, so their manifests multiply by concurrency and sharing collapses
them. The indexed stage multiplies by nothing -- a ``nearest`` read plans a
single index-driven task -- and what sharing buys there is loading the index
once per process instead of once per query. The maintenance stage (compaction
and ``update_columns``) is the one that turns out *not* to multiply: Daft gives
each concurrent class instance a process of its own, so a maintenance worker
opens the table exactly once however high the concurrency, and an indexed
table's real maintenance cost is the index remap in the commit, on the driver.

Usage::

    make bench
    make bench EXTRA_ARGS="--fragments 2000 --parallel 16"
    uv run --group ray python benchmarks/lance_session_memory.py --mode shared
"""

from __future__ import annotations

import argparse
import json
import os
import pickle
import resource
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

import lance
import pyarrow as pa
import ray
from lance.lance import _Session as LanceSession

import daft
import daft_lance
from daft import method
from daft_lance._blob import read_blobs
from daft_lance.namespace import get_shared_session
from daft_lance.utils import construct_lance_dataset_handle

MB = 1024 * 1024
RESULT_PREFIX = "RESULT_JSON "
# The indexed stage. A vector index costs roughly 26 bytes per row in a
# session's index cache (measured: 5.1 MB at 200k rows), which dwarfs the
# manifest on any normally shaped table -- so a benchmark that only ever reads
# an unindexed table understates what sharing a session saves.
INDEX_ROWS = 200_000
INDEX_DIM = 128
INDEX_PARTITIONS = 256
INDEX_SUB_VECTORS = 16
INDEX_QUERIES = 6
RSS_KEY = "process.memory.rss"
WRITE_ROWS = 8
# Rows per fragment in the table under test. Also load-bearing for the
# measurement, not just for realism -- see build_table.
ROWS_PER_FRAGMENT = 10_000
# Worker RSS wobbles by a megabyte or so between runs (Ray's own buffers, the
# arrow allocator). Below this the manifest saving is inside the noise, and the
# table under test is simply too small to make the claim with RSS.
RSS_NOISE_FLOOR = 4 * MB
# How long one side of the A/B may take before something is clearly wrong.
CHILD_TIMEOUT = 1800
# The maintenance stage. Fewer fragments than the read table and no vector
# column: what is being measured there is where a maintenance *worker's* memory
# goes, and a worker opens the table once whatever the fragment count. The index
# is a scalar one for the same reason -- cheap to build, and the compaction
# commit still has to remap it.
MAINT_FRAGMENTS = 200
MAINT_CONCURRENCY = 6
# Rows in each small fragment the maintenance stage appends to give compaction
# something to merge. Compaction's target is a few of these, so the table's own
# large fragments stay above the target and are never rewritten -- the stage
# costs seconds whatever the table's size.
MAINT_ROWS_PER_APPEND = 1_000
MAINT_TARGET_FRAGMENT_ROWS = MAINT_ROWS_PER_APPEND * 4
# How many extra opens calibrate_per_open_bytes may hold at once.
CALIBRATION_OPENS = 4


def maxrss_bytes() -> int:
    """``ru_maxrss`` normalized to bytes: macOS reports bytes, Linux kilobytes."""
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(rss) if sys.platform == "darwin" else int(rss) * 1024


def worker_parallelism() -> dict[str, Any]:
    """What the executing process thinks it may run in parallel.

    The scheduling ceiling is min(the Ray node's CPUs, scantask_max_parallel),
    but a container's real ceiling is its cgroup quota, and that is what sizes
    Daft's execution thread pool. On a laptop these agree; in a pod with
    ``num-cpus: 24`` and ``limits.cpu: 2`` they do not, so both get recorded.
    """
    info: dict[str, Any] = {"cpu_count": os.cpu_count()}
    affinity = getattr(os, "sched_getaffinity", None)
    if affinity is not None:
        info["affinity"] = len(affinity(0))
    try:
        quota, period = Path("/sys/fs/cgroup/cpu.max").read_text().split()
        info["cgroup_cpu_max"] = None if quota == "max" else round(int(quota) / int(period), 2)
    except (OSError, ValueError):
        info["cgroup_cpu_max"] = None
    return info


@daft.func.batch(return_dtype=daft.DataType.string())
def probe(column: daft.Series) -> list[str]:
    """Report the executing process's memory and session state, one row at a time.

    Deliberately a plain batch UDF: Daft fuses those into the scan's task
    instead of giving them their own actor, so what this returns describes the
    very process that opened the lance datasets.
    """
    session = get_shared_session()
    payload = json.dumps(
        {
            "pid": os.getpid(),
            "maxrss": maxrss_bytes(),
            "session_size": session.size_bytes(),
            "session_obj_id": id(session),
            "same_session": bool(session.is_same_as(get_shared_session())),
            "flag": os.environ.get("DAFT_LANCE_SHARE_SESSION"),
            "parallelism": worker_parallelism(),
        }
    )
    return [payload] * len(column)


def build_table(uri: str, fragments: int, rows_per_fragment: int = ROWS_PER_FRAGMENT) -> None:
    """``fragments`` files of ``rows_per_fragment`` rows each.

    A real lance table holds tens of thousands of rows per fragment, and the row
    count matters to this benchmark beyond realism: with one row per fragment a
    scan task opens its dataset, reads a row and drops it, so the opens barely
    overlap and far fewer than ``--parallel`` manifests are ever resident at
    once. Filling the fragments keeps each task busy long enough for the
    concurrency to actually materialize.
    """
    # Written in chunks of about ten million rows so the whole table is never in
    # memory at once, and each chunk is a whole number of fragments so no chunk
    # boundary leaves a half-filled one. Chunks are not just a memory knob: each
    # one is an append, and an append rewrites the whole manifest, so chunking a
    # 100k-fragment table a million rows at a time spends most of its time
    # rewriting an ever-growing fragment list. Ten million keeps the arrow
    # buffer under a hundred megabytes and cuts the commits tenfold.
    rows_per_chunk = max(rows_per_fragment, (10_000_000 // max(rows_per_fragment, 1)) * rows_per_fragment)
    total = fragments * rows_per_fragment
    start = 0
    while start < total:
        count = min(rows_per_chunk, total - start)
        table = pa.table({"i": pa.array(range(start, start + count), type=pa.int64())})
        lance.write_dataset(
            table,
            uri,
            mode="create" if start == 0 else "append",
            max_rows_per_file=rows_per_fragment,
        )
        start += count


def build_blob_table(uri: str, fragments: int) -> None:
    rows = max(fragments, 2)
    table = pa.table(
        {
            "id": pa.array(range(rows), type=pa.int64()),
            "blob": lance.blob_array([b"payload-%d" % i for i in range(rows)]),
        }
    )
    lance.write_dataset(table, uri, mode="create", data_storage_version="2.2", max_rows_per_file=1)


def build_indexed_table(uri: str, rows: int = INDEX_ROWS, dim: int = INDEX_DIM) -> None:
    """A normally shaped table -- few fragments, many rows -- carrying a vector index.

    The point of this one is the index, not the fragment list: index size scales
    with rows while a manifest scales with fragments, so this is the mirror image
    of the one-row-per-fragment table above.
    """
    import numpy as np

    rng = np.random.default_rng(0)
    chunk = 50_000
    for start in range(0, rows, chunk):
        count = min(chunk, rows - start)
        vectors = rng.random((count, dim), dtype=np.float32)
        table = pa.table(
            {
                "id": pa.array(range(start, start + count), type=pa.int64()),
                "v": pa.FixedSizeListArray.from_arrays(pa.array(vectors.reshape(-1), type=pa.float32()), dim),
            }
        )
        lance.write_dataset(table, uri, mode="create" if start == 0 else "append", max_rows_per_file=25_000)
    dataset = lance.dataset(uri)
    dataset.create_index(
        "v",
        index_type="IVF_PQ",
        num_partitions=INDEX_PARTITIONS,
        num_sub_vectors=INDEX_SUB_VECTORS,
    )


def indexed_query_stage(uri: str) -> dict[str, Any]:
    """Run the same vector search repeatedly and time each one.

    ``read_lance`` with ``nearest`` plans a *single* index-driven scan task, so
    this is not about concurrency: it is about whether the index is loaded once
    per process or once per query. Shared, the first query pays for the load and
    the rest are served from the session's index cache; unshared, every query
    re-reads and re-decodes the index.
    """
    import numpy as np

    rng = np.random.default_rng(7)
    durations: list[float] = []
    sessions: list[int] = []
    for _ in range(INDEX_QUERIES):
        query = rng.random(INDEX_DIM, dtype=np.float32)
        df = daft_lance.read_lance(uri, default_scan_options={"nearest": {"column": "v", "q": query, "k": 10}})
        started = time.monotonic()
        rows = df.to_arrow().num_rows
        durations.append((time.monotonic() - started) * 1000)
        if rows != 10:
            raise SystemExit(f"the indexed query returned {rows} rows, expected 10")
        report = report_from_worker()
        sessions.append(max(int(row["session_size"]) for row in report.values()))
    return {
        "durations_ms": durations,
        # Steady state, i.e. every query but the first: the first one is where a
        # shared session does its one load, and including it would hide the
        # difference the rest of them show.
        "steady_median_ms": statistics.median(durations[1:]),
        "first_ms": durations[0],
        "worker_session_sizes": sessions,
    }


def build_maintenance_table(uri: str, fragments: int, rows_per_fragment: int) -> None:
    """A many-fragment table with a scalar index, for the maintenance stage.

    Compaction rewrites every row it touches, so this one carries no vector
    column: the index is a BTREE over a string key, which still has to be
    remapped at commit but costs seconds to build rather than gigabytes to
    rewrite. Written with pyarrow rather than through Daft because Daft emits
    ``large_string``, which lance will not index.
    """
    total = fragments * rows_per_fragment
    rows_per_chunk = max(rows_per_fragment, (1_000_000 // max(rows_per_fragment, 1)) * rows_per_fragment)
    start = 0
    while start < total:
        count = min(rows_per_chunk, total - start)
        table = pa.table(
            {
                "i": pa.array(range(start, start + count), type=pa.int64()),
                "k": pa.array([f"k{value:09d}" for value in range(start, start + count)], type=pa.string()),
            }
        )
        lance.write_dataset(
            table,
            uri,
            mode="create" if start == 0 else "append",
            max_rows_per_file=rows_per_fragment,
        )
        start += count
    lance.dataset(uri).create_scalar_index("k", index_type="BTREE")


def append_compaction_work(uri: str, groups: int, rows_per_append: int = MAINT_ROWS_PER_APPEND) -> int:
    """Append small fragments so there is always something to compact.

    The data volume is deliberately trivial. Compaction consumes its own input,
    so a stage that compacts whatever it finds works exactly once per table and
    reports "nothing to do" on every later run -- which is how a reused table
    turned a passing A/B into a failure. Appending is also the honest shape of
    the problem: small fragments arriving from streaming writes are what
    compaction exists for.

    None of this touches what the stage measures. A maintenance instance's cost
    is one open of the table, and an open carries the whole fragment list
    whatever the task then rewrites.
    """
    dataset = lance.dataset(uri)
    start = dataset.count_rows()
    # Enough fragments that every instance in the pool gets work, with slack so
    # the plan does not degenerate to one task.
    count = max(groups, 2) * 4 * rows_per_append
    table = pa.table(
        {
            "i": pa.array(range(start, start + count), type=pa.int64()),
            "k": pa.array([f"k{value:09d}" for value in range(start, start + count)], type=pa.string()),
        }
    )
    lance.write_dataset(table, uri, mode="append", max_rows_per_file=rows_per_append)
    return count // rows_per_append


class OpenCensus:
    """Open the table the way a maintenance worker does, and report who did it.

    Decorated at the call site exactly like ``CompactionTaskUDF`` and the
    ``update_columns`` handlers -- ``max_concurrency`` set, ``use_process``
    left alone -- because the question this answers is about placement: if
    Daft puts the N instances in one process, the N opens collapse into one
    shared session and sharing saves N-1 manifests; if it gives each its own
    process, there is exactly one open per process and nothing to collapse.
    The real maintenance UDFs cannot be asked this (they return rewrite
    results, not pids), so this stands in for them.
    """

    def __init__(self, open_context: Any) -> None:
        self.open_context = open_context
        self._lance_ds: lance.LanceDataset | None = None

    @method.batch(return_dtype=daft.DataType.string())
    def __call__(self, column: daft.Series) -> list[str]:
        if self._lance_ds is None:
            # Once per instance, which is what the maintenance UDFs do.
            self._lance_ds = self.open_context.open_pinned()
        session = get_shared_session()
        payload = json.dumps(
            {
                "pid": os.getpid(),
                "instance": id(self),
                "session_obj_id": id(session),
                "session_size": session.size_bytes(),
                "fragments": len(self._lance_ds.get_fragments()),
                "flag": os.environ.get("DAFT_LANCE_SHARE_SESSION"),
            }
        )
        return [payload] * len(column)


def maintenance_census(open_context: Any, concurrency: int) -> dict[str, Any]:
    """Run ``concurrency`` maintenance-shaped instances and count processes."""
    census = daft.cls(OpenCensus, max_concurrency=concurrency)
    partitions = concurrency * 8
    df = daft.from_pydict({"x": list(range(partitions))}).into_partitions(partitions)
    rows = [json.loads(value) for value in df.select(census(open_context)(daft.col("x")).alias("p")).to_pydict()["p"]]
    instances = {(row["pid"], row["instance"]): row for row in rows}
    return {
        "requested": concurrency,
        "instances": len(instances),
        "processes": len({pid for pid, _ in instances}),
        "sessions": len({row["session_obj_id"] for row in instances.values()}),
        "flags": sorted({row["flag"] for row in instances.values()}, key=str),
        "fragments": sorted({row["fragments"] for row in instances.values()}),
        # Per process, so it is comparable between the sides regardless of how
        # many processes each got.
        "session_size_max": max(int(row["session_size"]) for row in instances.values()),
    }


def maintenance_stage(data_dir: str, side: str, fragments: int, concurrency: int) -> dict[str, Any]:
    """Compaction and ``update_columns`` on an indexed table, with the driver watched too.

    Two separate things are measured here, and they answer different halves of
    "does an index make maintenance expensive":

    * the worker side, via :func:`maintenance_census`: how many processes the
      concurrent instances land in, and what one of them holds;
    * the driver side, directly, because under Ray the driver *is* this process:
      what the compaction leaves in it. Compaction has to remap every index
      entry onto the new fragment ids, and that happens in the commit, on the
      driver, not in the tasks -- so this is where an indexed table's
      maintenance cost shows up at all. The figure is manifest and index
      together; ``test_shared_session.py`` is where the two are separated,
      which needs the tasks and the commit pulled apart.

    Each side of the A/B gets its own copy of the table: compaction consumes its
    own input, so whoever ran second would otherwise find nothing to compact.
    The stage also makes its own work rather than compacting whatever happens to
    be there, because the tables outlive a run -- see :func:`append_compaction_work`.
    """
    uri = str(Path(data_dir) / f"maint-{side}-{fragments}.lance")
    appended = append_compaction_work(uri, concurrency)
    session = get_shared_session()

    def snapshot() -> dict[str, int]:
        # ``size_bytes()`` only. A session also reports
        # ``index_cache_size_bytes()``, but that is the accounting lance
        # enforces its index budget with, not resident bytes: measured on a
        # 200k-row IVF_PQ index it reads 37 MB where both ``size_bytes()`` and
        # the process's own RSS move by about 5 MB per loaded copy. Printing it
        # as memory would overstate an index's cost sevenfold.
        return {"total": session.size_bytes()}

    driver: dict[str, dict[str, int]] = {"start": snapshot()}
    handle = construct_lance_dataset_handle(uri)
    driver["after_open"] = snapshot()
    result: dict[str, Any] = {
        "uri": uri,
        "fragments_appended": appended,
        "fragments_before": len(handle.dataset.get_fragments()),
        "rows": handle.dataset.count_rows(),
        "indices": [index["name"] for index in handle.dataset.list_indices()],
        "concurrency": concurrency,
        "census": maintenance_census(handle.worker_open_context(), concurrency),
    }
    driver["after_census"] = snapshot()

    started = time.monotonic()
    metrics = daft_lance.compact_files(
        uri,
        # Keyed to the appended fragments, not to the table's own: the large
        # fragments already sit above this and are not candidates, so only the
        # freshly appended ones are rewritten.
        compaction_options={"target_rows_per_fragment": MAINT_TARGET_FRAGMENT_ROWS},
        partition_num=concurrency,
        concurrency=concurrency,
    )
    result["compact_seconds"] = time.monotonic() - started
    driver["after_compact"] = snapshot()
    if metrics is None:
        raise SystemExit(
            f"compaction found nothing to do on {uri} even after appending {appended} small fragments; "
            "check that target_rows_per_fragment is above the appended fragments' size"
        )
    result["compaction"] = {
        "fragments_removed": metrics.fragments_removed,
        "fragments_added": metrics.fragments_added,
    }

    started = time.monotonic()
    updated = daft_lance.update_columns(
        uri,
        transform={"i": "i + 0"},
        columns=["i"],
        max_concurrency=concurrency,
    )
    result["update_seconds"] = time.monotonic() - started
    driver["after_update"] = snapshot()
    if updated.rows_updated != result["rows"]:
        raise SystemExit(f"update_columns touched {updated.rows_updated} rows, expected {result['rows']}")
    result["rows_updated"] = updated.rows_updated
    result["driver_session"] = driver
    return result


def report_from_worker() -> dict[int, dict[str, Any]]:
    """Run a one-row query so the worker reports its current state back."""
    df = daft.from_pydict({"x": [0]}).with_column("p", probe(daft.col("x")))
    rows = [json.loads(value) for value in df.to_pydict()["p"]]
    return {row["pid"]: row for row in rows}


def sampled_rss_peaks(event_log_dir: str) -> dict[int, dict[str, Any]]:
    """Peak ``process.memory.rss`` per pid, from Daft's own 200ms sampler."""
    peaks: dict[int, dict[str, Any]] = {}
    for path in sorted(Path(event_log_dir).glob("*/events.jsonl")):
        for line in path.read_text().splitlines():
            try:
                record = json.loads(line)
            except ValueError:
                continue
            if record.get("event") != "process_stats":
                continue
            rss = record.get("metrics", {}).get(RSS_KEY)
            pid = record.get("pid")
            if rss is None or pid is None:
                continue
            entry = peaks.setdefault(int(pid), {"component": record.get("component"), "peak": 0})
            entry["peak"] = max(entry["peak"], int(rss))
    return peaks


def payload_sizes(uri: str) -> dict[str, int]:
    """Bytes a scan task used to carry versus what it carries now.

    ``LanceDataset.__reduce__`` ships the re-encoded manifest, so closing over a
    dataset -- which ``read_blobs`` used to do -- cost that much per task. The
    replacement is a uri plus the pinned open kwargs.
    """
    dataset = lance.dataset(uri)
    open_kwargs = {"version": dataset.version, "storage_options": dataset._storage_options}
    return {
        "pickled_dataset": len(pickle.dumps(dataset)),
        "pickled_open_kwargs": len(pickle.dumps((uri, open_kwargs))),
    }


def run_child(args: argparse.Namespace) -> int:
    """One half of the A/B: a full read + read_blobs + write under Ray.

    Either starts a cluster of its own (the local ``--mode both`` path, which
    spawns this twice) or attaches to one that is already there, which is how it
    runs as a RayJob entrypoint: there the two sides are separate jobs and the
    flag arrives through the pod spec instead of process inheritance.
    """
    address = os.environ.get("RAY_ADDRESS")
    if address:
        # An attached run must not ask for CPUs: the cluster already decided,
        # and Ray ignores num_cpus with a warning when it is only connecting.
        # Concurrency comes from the worker's ``ray start --num-cpus``, which is
        # a logical count and need not match the pod's cpu limit.
        ray.init()
    else:
        ray.init(num_cpus=args.parallel, include_dashboard=False)
    daft.set_runner_ray(noop_if_initialized=True)
    daft.set_execution_config(scantask_max_parallel=args.parallel)

    uri = str(Path(args.data_dir) / f"table-{args.fragments}.lance")
    blob_uri = str(Path(args.data_dir) / "blobs.lance")
    dataset = lance.dataset(uri)
    result: dict[str, Any] = {
        "mode": args.mode,
        "parallel": args.parallel,
        "flag": os.environ.get("DAFT_LANCE_SHARE_SESSION"),
        "ray_address": address,
        "rows_per_fragment": args.rows_per_fragment,
        "cluster_cpus": ray.cluster_resources().get("CPU"),
        "fragments_at_read": len(dataset.get_fragments()),
        "manifest_bytes": len(dataset._ds.serialized_manifest()),
    }

    durations: dict[str, float] = {}
    before = report_from_worker()

    # The pipeline under test: read Lance, write Parquet. One task per fragment
    # group, `parallel` of them open at once, and each task holds a parquet
    # writer's buffers at the same time as its manifest -- which is the point of
    # measuring this shape rather than an aggregation. An aggregation keeps
    # almost nothing resident, so it flatters the worker; a write makes the
    # manifests compete with the writer for the same memory limit, which is what
    # a real backfill does.
    #
    # Deliberately not count_rows(): count pushdown would collapse the whole
    # plan into a single task that opens no fragment at all.
    #
    # count_rows() here is the dataset's own metadata, read on the driver before
    # the scan: cheap, and it says how much the scan below had to touch. The two
    # sides see slightly different totals because each appends in its lance
    # write stage, which is why this is recorded rather than asserted against a
    # formula.
    rows_total = dataset.count_rows()
    result["rows_total"] = rows_total
    # Two knobs for keeping a large table affordable, and neither weakens the
    # measurement: a manifest is the table's whole fragment list, so an open
    # costs the same whether its task reads one fragment or fifty, one row or
    # ten thousand. --read-fraction prunes whole fragments (``i`` is written
    # strictly increasing, so a cutoff is a fragment prefix) and
    # --fragment-group-size puts several fragments in one task.
    #
    # What does have to survive is the overlap: enough tasks to keep
    # --parallel of them in flight, each long enough that their opens coexist.
    # Both counts are reported so that is checkable rather than hoped for.
    result["read_fraction"] = args.read_fraction
    result["fragment_group_size"] = args.fragment_group_size
    reader = daft_lance.read_lance(uri, fragment_group_size=args.fragment_group_size)
    if args.read_fraction < 1.0:
        cutoff = int(rows_total * args.read_fraction)
        result["read_cutoff"] = cutoff
        # Arithmetic, not a second pass over 100k fragments: build_table fills
        # fragments in order, so a cutoff of N rows is the first N/rows
        # fragments.
        touched = -(-cutoff // args.rows_per_fragment)
        result["fragments_touched"] = touched
        result["tasks_estimated"] = -(-touched // max(args.fragment_group_size or 1, 1))
        reader = reader.where(daft.col("i") < cutoff)
    else:
        result["fragments_touched"] = result["fragments_at_read"]
        result["tasks_estimated"] = -(-result["fragments_at_read"] // max(args.fragment_group_size or 1, 1))
    if result["tasks_estimated"] < 2 * args.parallel:
        raise SystemExit(
            f"lance -> parquet would plan about {result['tasks_estimated']} tasks at concurrency "
            f"{args.parallel}: too few for the opens to overlap, so raise --read-fraction or lower "
            "--fragment-group-size"
        )
    # One output directory per side. Overwrite rather than append so a rerun
    # does not measure the previous run's files, and so the two sides never read
    # each other's output back as input to anything.
    parquet_dir = Path(args.data_dir) / f"out-parquet-{args.mode}"
    started = time.monotonic()
    written = reader.write_parquet(str(parquet_dir), write_mode="overwrite").to_pydict()
    durations["read_to_parquet"] = time.monotonic() - started
    files = written.get("path") or []
    if not files:
        raise SystemExit("the pipeline wrote no parquet files, so it never reached the data")
    result["parquet_files"] = len(files)
    result["parquet_bytes"] = sum(path.stat().st_size for path in parquet_dir.rglob("*.parquet"))
    after_read = report_from_worker()

    started = time.monotonic()
    blob_ds = lance.dataset(blob_uri)
    blob_df = daft.read_lance(blob_uri, default_scan_options={"with_row_id": True})
    result["blobs_read"] = len(read_blobs(blob_df, blob_ds, "blob").to_pydict()["blob"])
    durations["read_blobs"] = time.monotonic() - started
    after_blobs = report_from_worker()

    # A small append with tiny files, so the writer flushes `write_fragments`
    # several times; each flush is a manifest load when no session is passed.
    started = time.monotonic()
    daft.from_pydict({"i": list(range(WRITE_ROWS))}).write_lance(uri, mode="append", max_rows_per_file=1).collect()
    durations["write"] = time.monotonic() - started
    after_write = report_from_worker()

    # Last, because it loads an index into whatever session it gets and would
    # otherwise contaminate the manifest measurements above.
    indexed_uri = str(Path(args.data_dir) / f"indexed-{INDEX_ROWS}.lance")
    started = time.monotonic()
    result["indexed"] = indexed_query_stage(indexed_uri)
    durations["indexed_queries"] = time.monotonic() - started

    # After everything above, and on a table of its own, because compaction
    # rewrites its input: it must not disturb the read table the stages above
    # measured, and its own driver-side numbers are read as deltas so the
    # earlier stages' caches do not matter.
    started = time.monotonic()
    result["maintenance"] = maintenance_stage(
        args.data_dir,
        args.mode,
        args.maint_fragments,
        args.maint_concurrency,
    )
    durations["maintenance"] = time.monotonic() - started
    result["durations"] = durations

    # Calibrated here rather than by whoever orchestrates the A/B: the threshold
    # then comes from the same architecture, the same lance build and the same
    # allocator as the measurement it gates. Last, not first: it holds several
    # unshared copies of the manifest, which at 100k fragments is a few hundred
    # megabytes, and the driver needs that headroom while it is planning one
    # scan task per fragment. Nothing above depends on the number.
    result["per_open_bytes"] = calibrate_per_open_bytes(uri, args.parallel)

    workers: dict[str, Any] = {}
    for pid, row in after_write.items():
        workers[str(pid)] = {
            "flag": row["flag"],
            "same_session": row["same_session"],
            "parallelism": row.get("parallelism"),
            "session_obj_ids": sorted(
                {
                    stage[pid]["session_obj_id"]
                    for stage in (before, after_read, after_blobs, after_write)
                    if pid in stage
                }
            ),
            "maxrss": {
                "before": before.get(pid, {}).get("maxrss"),
                "after_read": after_read.get(pid, {}).get("maxrss"),
                "after_blobs": after_blobs.get(pid, {}).get("maxrss"),
                "after_write": after_write.get(pid, {}).get("maxrss"),
            },
            "session_size": {
                "before": before.get(pid, {}).get("session_size"),
                "after_read": after_read.get(pid, {}).get("session_size"),
                "after_blobs": after_blobs.get(pid, {}).get("session_size"),
                "after_write": after_write.get(pid, {}).get("session_size"),
            },
        }
    result["workers"] = workers
    result["worker_pids_per_stage"] = {
        "before": sorted(before),
        "after_read": sorted(after_read),
        "after_blobs": sorted(after_blobs),
        "after_write": sorted(after_write),
    }
    event_log_dir = os.environ.get("DAFT_EVENT_LOG_DIR")
    result["sampled_rss_peak"] = (
        {str(pid): entry for pid, entry in sampled_rss_peaks(event_log_dir).items()} if event_log_dir else {}
    )
    print(RESULT_PREFIX + json.dumps(result))
    return 0


def calibrate_per_open_bytes(uri: str, opens: int) -> float:
    """What one *additional* unshared open of this table costs, measured here and now.

    Thresholds are derived from this rather than hardcoded, so the script stays
    honest on a different table or a different lance version.

    Marginal rather than average, and capped at a handful of opens, for two
    reasons. The cap: at 100k fragments one open is around 64 MB, so holding
    ``--parallel`` of them puts half a gigabyte in the driver -- a calibration
    must not be able to push over the process it is calibrating, and at 100k
    fragments on a 3Gi head it did. Marginal: the first open also pays one-time
    costs (the object-store client, lance's own statics) that do not repeat per
    open, so averaging them in overstates the per-open cost and with it the
    threshold.
    """
    first = [lance.dataset(uri, session=LanceSession())]
    extra = max(min(opens - 1, CALIBRATION_OPENS), 1)
    before = maxrss_bytes()
    held = [lance.dataset(uri, session=LanceSession()) for _ in range(extra)]
    after = maxrss_bytes()
    assert len(first) == 1 and len(held) == extra
    return (after - before) / extra


def spawn_child(mode: str, args: argparse.Namespace, event_log_dir: Path) -> dict[str, Any]:
    env = dict(os.environ)
    env.update(
        {
            "DAFT_LANCE_SHARE_SESSION": "1" if mode == "shared" else "0",
            "DAFT_RUNNER": "ray",
            "DAFT_PROGRESS_BAR": "0",
            "DAFT_ANALYTICS_ENABLED": "0",
            "DAFT_PROCESS_MONITOR_ENABLED": "1",
            "DAFT_EVENT_LOG_DIR": str(event_log_dir),
            # Ray notices when the driver's process tree was started by `uv run`
            # -- which it is, via `make bench` -- and then replicates that
            # environment to the workers: it packages the whole working
            # directory and reinstalls it in a fresh venv per worker. That costs
            # minutes here and has hung outright. The workers already inherit
            # this venv's interpreter from the raylet, so turn it off.
            "RAY_ENABLE_UV_RUN_RUNTIME_ENV": "0",
        }
    )
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--mode",
        mode,
        "--fragments",
        str(args.fragments),
        "--parallel",
        str(args.parallel),
        "--data-dir",
        str(args.data_dir),
        "--rows-per-fragment",
        str(args.rows_per_fragment),
        "--maint-fragments",
        str(args.maint_fragments),
        "--maint-concurrency",
        str(args.maint_concurrency),
        "--read-fraction",
        str(args.read_fraction),
    ]
    if args.fragment_group_size:
        command += ["--fragment-group-size", str(args.fragment_group_size)]
    print(f"\n=== running {mode} (DAFT_LANCE_SHARE_SESSION={env['DAFT_LANCE_SHARE_SESSION']}) ===", flush=True)
    try:
        process = subprocess.run(command, env=env, capture_output=True, text=True, check=False, timeout=CHILD_TIMEOUT)
    except subprocess.TimeoutExpired as expired:
        sys.stderr.write(expired.stderr or "")
        raise SystemExit(
            f"{mode} child did not finish within {CHILD_TIMEOUT}s. The read is one task per fragment, so a large "
            "--fragments takes a long time; lower it, or raise CHILD_TIMEOUT."
        ) from expired
    payload = None
    for line in process.stdout.splitlines():
        if line.startswith(RESULT_PREFIX):
            payload = json.loads(line[len(RESULT_PREFIX) :])
    if payload is None:
        sys.stdout.write(process.stdout)
        sys.stderr.write(process.stderr)
        raise SystemExit(f"{mode} child produced no result (exit code {process.returncode})")
    return payload


def worker_row(result: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    workers = result["workers"]
    if len(workers) != 1:
        raise SystemExit(f"expected one worker process, got {sorted(workers)}; run this on a single-node cluster")
    return next(iter(workers.items()))


def compare(baseline: dict[str, Any], shared: dict[str, Any], per_open_bytes: float) -> list[str]:
    """Print the comparison and return the failures, if any."""
    failures: list[str] = []
    baseline_pid, baseline_worker = worker_row(baseline)
    shared_pid, shared_worker = worker_row(shared)

    # Before anything else: the A/B only means something if the flag actually
    # reached the worker. Ray actors do not inherit the driver's environment by
    # contract, so this must be observed, never assumed.
    for name, result, worker, expected in (
        ("baseline", baseline, baseline_worker, "0"),
        ("shared", shared, shared_worker, "1"),
    ):
        if worker["flag"] != expected:
            failures.append(
                f"{name}: worker saw DAFT_LANCE_SHARE_SESSION={worker['flag']!r}, driver set {expected!r} -- "
                "the environment did not reach the worker, so these numbers compare nothing"
            )
        if result["flag"] != expected:
            failures.append(f"{name}: driver saw flag {result['flag']!r}, expected {expected!r}")

    # Concurrency decides how many manifests can be resident at once, so the two
    # sides have to have had the same ceiling, and the reader has to be able to
    # see what it was.
    for name, result, worker in (
        ("baseline", baseline, baseline_worker),
        ("shared", shared, shared_worker),
    ):
        limits = worker.get("parallelism") or {}
        print(
            f"{name:10} rows/fragment {result.get('rows_per_fragment')}, "
            f"scantask_max_parallel {result['parallel']}, ray cluster CPUs {result.get('cluster_cpus')}, "
            f"worker sees cpu_count={limits.get('cpu_count')} affinity={limits.get('affinity')} "
            f"cgroup_cpu_max={limits.get('cgroup_cpu_max')}"
        )
    for key in ("parallel", "rows_per_fragment", "read_fraction", "fragment_group_size"):
        if baseline.get(key) != shared.get(key):
            failures.append(
                f"the two sides ran with different {key} ({baseline.get(key)!r} vs {shared.get(key)!r}), "
                "so their memory is not comparable"
            )

    def delta(worker: dict[str, Any], field: str, stage: str) -> int:
        return int(worker[field][stage]) - int(worker[field]["before"])

    rows = []
    for name, result, worker, pid in (
        ("baseline", baseline, baseline_worker, baseline_pid),
        ("shared", shared, shared_worker, shared_pid),
    ):
        sampled = result["sampled_rss_peak"].get(pid, {})
        rows.append(
            {
                "name": name,
                "pid": pid,
                "fragments": result["fragments_at_read"],
                "rss_delta_read": delta(worker, "maxrss", "after_read"),
                "rss_delta_total": delta(worker, "maxrss", "after_write"),
                "sampled_peak": sampled.get("peak"),
                "sampled_component": sampled.get("component"),
                "session_delta_read": delta(worker, "session_size", "after_read"),
                "session_delta_blobs": delta(worker, "session_size", "after_blobs"),
                "session_delta_total": delta(worker, "session_size", "after_write"),
                "same_session": worker["same_session"],
                "sessions_seen": len(worker["session_obj_ids"]),
            }
        )

    print(f"\nper-open cost of this table, measured here: {per_open_bytes / MB:.2f} MB")
    for name, result in (("baseline", baseline), ("shared", shared)):
        stages = ", ".join(f"{stage} {seconds:.1f}s" for stage, seconds in result["durations"].items())
        print(f"{name:10} {stages}")
    print(
        f"manifest: {baseline['manifest_bytes'] / 1024:.1f} KiB, table rows: {baseline.get('rows_total')}, "
        f"parquet written: {baseline.get('parquet_files')} files / "
        f"{(baseline.get('parquet_bytes') or 0) / MB:.1f} MB"
    )
    print(
        f"lance -> parquet: {baseline.get('read_fraction')} of the table, fragment_group_size "
        f"{baseline.get('fragment_group_size')}, so about {baseline.get('fragments_touched')} fragments in "
        f"{baseline.get('tasks_estimated')} tasks at concurrency {baseline['parallel']} "
        "(a manifest costs the same per open whatever the task reads)"
    )
    print(
        f"{'':10} {'pid':>7} {'frags':>7} {'rssDpq':>9} {'rssDall':>9} {'rssPeak':>9} "
        f"{'sessDpq':>10} {'sessDblob':>10} {'sessDall':>10}"
    )
    for row in rows:
        sampled = "-" if row["sampled_peak"] is None else f"{row['sampled_peak'] / MB:.1f}"
        print(
            f"{row['name']:10} {row['pid']:>7} {row['fragments']:>7} "
            f"{row['rss_delta_read'] / MB:>9.1f} {row['rss_delta_total'] / MB:>9.1f} {sampled:>9} "
            f"{row['session_delta_read'] / MB:>10.3f} "
            f"{(row['session_delta_blobs'] - row['session_delta_read']) / MB:>10.3f} "
            f"{row['session_delta_total'] / MB:>10.3f}"
        )
    print(
        "(MB. rssDpq: the worker's ru_maxrss growth across the lance-to-parquet stage. rssPeak: Daft's\n"
        " own sampled rss peak for that pid, an independent reading of the same process. sessD: growth of\n"
        " the process-wide lance session, which is zero whenever an open did not go through it.)"
    )

    baseline_row, shared_row = rows[0], rows[1]

    # Mechanism, with no RSS noise in it: with sharing on, a scan task's open
    # lands in the process-wide session, so its size grows. With sharing off
    # the global session is never touched at all.
    if baseline_row["session_delta_total"] != 0:
        failures.append(
            f"baseline: process session grew by {baseline_row['session_delta_total']} bytes; "
            "with sharing off no open should touch it"
        )
    if shared_row["session_delta_read"] <= 0:
        failures.append(
            "shared: the lance-to-parquet stage did not grow the process session, so its scan tasks bypassed it"
        )
    if shared_row["session_delta_total"] <= shared_row["session_delta_read"]:
        failures.append("shared: read_blobs and the write did not add to the process session")
    if not shared_row["same_session"] or shared_row["sessions_seen"] != 1:
        failures.append(
            f"shared: worker reported {shared_row['sessions_seen']} distinct sessions, expected one per process"
        )
    if baseline_row["same_session"]:
        failures.append("baseline: the kill switch did not take effect in the worker")

    # The memory claim itself. Only the per-open cost beyond the first is
    # saved, and never all of it: a task holds its manifest just while its
    # dataset is open, so the opens overlap only partly and the peak never
    # reaches the full K-way sum. Measured at --parallel 24, the saving came in
    # at 33-41 MB against a 67 MB theoretical maximum, so demanding 30% of it
    # keeps roughly a 2x margin over the observed value while still failing
    # outright if sharing stops working, which takes the saving to zero.
    #
    # Measured over the lance-to-parquet stage alone, not the whole run.
    # ``ru_maxrss`` is a high-water mark, and the later stages allocate about ten
    # megabytes of their own: on the shared side, whose peak was lower, that
    # pushes the mark back up to roughly where the baseline already was, hiding
    # the very difference being measured. That stage is also the only one whose
    # concurrency is pinned, and the one the pipeline this benchmark exists for
    # actually runs, so it is the one the threshold is derived from.
    # The indexed stage, which is a different claim from everything above: a
    # `nearest` read plans one index-driven scan task, so nothing multiplies by
    # concurrency. What sharing changes is that the index is loaded once per
    # process instead of once per query.
    baseline_index, shared_index = baseline.get("indexed"), shared.get("indexed")
    if baseline_index and shared_index:
        print(
            f"\nindexed vector search, {INDEX_QUERIES} queries of the same table:\n"
            f"{'':10} {'first':>9} {'steady':>9} {'sessionEnd':>12}"
        )
        for name, side in (("baseline", baseline_index), ("shared", shared_index)):
            print(
                f"{name:10} {side['first_ms']:>8.1f}ms {side['steady_median_ms']:>8.1f}ms "
                f"{side['worker_session_sizes'][-1] / 1024:>10.1f}KiB"
            )
        print(
            "(first: the query that pays for loading the index. steady: the median of the rest. sessionEnd: what\n"
            " the worker's process-wide session holds at the end, so zero means every query loaded the index\n"
            " into a session of its own and threw it away.)"
        )
        baseline_end = baseline_index["worker_session_sizes"][-1]
        shared_end = shared_index["worker_session_sizes"][-1]
        if shared_end <= 0:
            failures.append("shared: the indexed queries left nothing in the process session, so none of them used it")
        # Compared against the shared side rather than against zero: an empty
        # lance Session is not zero bytes, it is about 152 of bookkeeping, so
        # "holds no index" means "holds orders of magnitude less than one".
        elif baseline_end > shared_end // 10:
            failures.append(
                f"baseline: the process session holds {baseline_end / 1024:.1f} KiB after the indexed queries, "
                f"against {shared_end / 1024:.1f} KiB shared; with sharing off no query should have filled it"
            )
        # Deliberately not a hard threshold on the ratio: it depends on index
        # size and on how far the storage is, and on a local disk with a warm
        # page cache it is the smallest it will ever be. Measured locally at
        # 200k rows: 5.6-7.0ms shared against 14.0-14.7ms unshared.
        ratio = baseline_index["steady_median_ms"] / max(shared_index["steady_median_ms"], 1e-9)
        print(
            f"steady-state queries are {ratio:.2f}x slower without a shared session "
            f"({baseline_index['steady_median_ms']:.1f}ms vs {shared_index['steady_median_ms']:.1f}ms); "
            "the gap grows with index size and storage latency"
        )
        if ratio < 1.0:
            failures.append(
                f"the unshared side was not slower on repeated indexed queries (ratio {ratio:.2f}x), so reloading "
                "the index cost nothing measurable here -- check that the index exists and that the queries used it"
            )

    # The maintenance path, which is a different shape of claim again. A scan
    # task shares a process with `parallel`-1 others, so its manifest is one of
    # many; a maintenance instance is given a process to itself, so its open is
    # the only one there. Both are measured, because only measuring says which.
    baseline_maint, shared_maint = baseline.get("maintenance"), shared.get("maintenance")
    if baseline_maint and shared_maint:
        print(
            f"\nmaintenance on an indexed table "
            f"({shared_maint['fragments_before']} fragments, {shared_maint['rows']} rows, "
            f"indices {shared_maint['indices']}), concurrency {shared_maint['concurrency']}:\n"
            f"{'':10} {'insts':>6} {'procs':>6} {'sessPerProc':>12} {'driverCommit':>13} "
            f"{'compact':>9} {'update':>9}"
        )
        for name, side in (("baseline", baseline_maint), ("shared", shared_maint)):
            census = side["census"]
            driver = side["driver_session"]
            driver_commit = driver["after_compact"]["total"] - driver["after_census"]["total"]
            print(
                f"{name:10} {census['instances']:>6} {census['processes']:>6} "
                f"{census['session_size_max'] / 1024:>10.1f}KiB {driver_commit / 1024:>11.1f}KiB "
                f"{side['compact_seconds']:>8.1f}s {side['update_seconds']:>8.1f}s"
            )
        print(
            "(insts/procs: concurrent maintenance instances, and how many processes they landed in. sessPerProc:\n"
            " the largest process-wide session any one of them held. driverCommit: what compaction added to the\n"
            " *driver's* session, manifest and remapped index together -- remapping an index onto the new fragment\n"
            " ids happens in the commit, on the driver, not in the tasks.)"
        )
        for name, side, expected in (("baseline", baseline_maint, "0"), ("shared", shared_maint, "1")):
            census = side["census"]
            if census["flags"] != [expected]:
                failures.append(
                    f"{name}: the maintenance instances saw DAFT_LANCE_SHARE_SESSION={census['flags']}, "
                    f"expected [{expected!r}] -- the flag did not reach them"
                )
            if census["instances"] < 2:
                failures.append(
                    f"{name}: only {census['instances']} maintenance instance ran, so concurrency was never "
                    "reached and this stage measured nothing"
                )
        shared_census, baseline_census = shared_maint["census"], baseline_maint["census"]
        if shared_census["session_size_max"] <= baseline_census["session_size_max"]:
            failures.append(
                "shared: a maintenance instance's process session is no larger than the baseline's, so the "
                "maintenance open did not go through it"
            )
        if baseline_census["sessions"] != baseline_census["instances"]:
            failures.append(
                f"baseline: {baseline_census['instances']} maintenance instances reported "
                f"{baseline_census['sessions']} sessions; with sharing off each open gets its own"
            )
        shared_driver = shared_maint["driver_session"]
        shared_driver_commit = shared_driver["after_compact"]["total"] - shared_driver["after_census"]["total"]
        if shared_driver_commit <= 0:
            failures.append(
                "shared: compaction left nothing in the driver's session, so the driver's planning and commit "
                "opened outside the process session"
            )
        baseline_driver = baseline_maint["driver_session"]
        if baseline_driver["after_compact"]["total"] != baseline_driver["start"]["total"]:
            failures.append(
                "baseline: the driver's process session grew during maintenance; with sharing off every open "
                "should get a session of its own"
            )
        # Not a failure, a finding: if Daft ever fuses these instances into one
        # process, the maintenance path acquires a saving to measure, and this
        # stage has to be rewritten to measure it rather than quietly passing.
        if shared_census["processes"] < shared_census["instances"]:
            print(
                f"NOTE: {shared_census['instances']} instances shared {shared_census['processes']} processes. "
                "Daft used to give each one its own, which is why this stage asserts nothing about concurrency "
                "multiplying manifests. It now does multiply them -- this stage should be extended to measure it."
            )
        else:
            print(
                f"Each of the {shared_census['instances']} instances got its own process, so a maintenance worker "
                f"opens the table exactly once ({shared_census['session_size_max'] / 1024:.1f} KiB of session) no "
                "matter the concurrency: nothing here multiplies by it, and the index cost sits in the driver's "
                "commit instead."
            )

    extra_opens = max(shared["parallel"] - 1, 1)
    expected_saving = 0.3 * extra_opens * per_open_bytes
    observed_saving = baseline_row["rss_delta_read"] - shared_row["rss_delta_read"]
    print(
        f"\nsaving over lance -> parquet: {observed_saving / MB:.1f} MB observed, "
        f"{expected_saving / MB:.1f} MB required "
        f"(30% of {extra_opens} extra opens x {per_open_bytes / MB:.2f} MB). "
        f"Over the whole run: {(baseline_row['rss_delta_total'] - shared_row['rss_delta_total']) / MB:.1f} MB, "
        "which the later stages' own allocations dilute."
    )
    if expected_saving < RSS_NOISE_FLOOR:
        print(
            f"NOT MEASURED: {extra_opens} extra opens of this table only amount to "
            f"{extra_opens * per_open_bytes / MB:.1f} MB, which RSS cannot resolve. The deterministic checks above "
            "still hold; raise --fragments (or --parallel) to put a number on the saving."
        )
    elif observed_saving < expected_saving:
        failures.append(
            f"the shared run saved {observed_saving / MB:.1f} MB, expected at least {expected_saving / MB:.1f} MB"
        )
    return failures


def run_prepare(args: argparse.Namespace) -> int:
    """Write the tables and nothing else, for a one-off prep job.

    Both sides of a Kubernetes A/B read the *same* copy, so the data has to be
    built once into a shared volume: lance names its data files with random
    uuids, so a per-pod copy would leave the driver's manifest pointing at file
    names the worker's copy does not have.
    """
    data_dir = Path(args.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    uri = data_dir / f"table-{args.fragments}.lance"
    blob_uri = data_dir / "blobs.lance"
    if uri.exists():
        print(f"{uri} already exists, keeping it")
    else:
        build_table(str(uri), args.fragments, args.rows_per_fragment)
        print(f"wrote {args.fragments} fragments x {args.rows_per_fragment} rows to {uri}")
    if blob_uri.exists():
        print(f"{blob_uri} already exists, keeping it")
    else:
        build_blob_table(str(blob_uri), min(args.fragments, 64))
        print(f"wrote {blob_uri}")
    indexed_uri = data_dir / f"indexed-{INDEX_ROWS}.lance"
    if indexed_uri.exists():
        print(f"{indexed_uri} already exists, keeping it")
    else:
        build_indexed_table(str(indexed_uri))
        print(f"wrote {INDEX_ROWS} rows with an IVF_PQ index to {indexed_uri}")
    # One per side: the maintenance stage compacts its table, so a shared copy
    # would leave whichever side ran second with nothing to compact. They are
    # built identically, which is what makes the two sides comparable.
    for side in ("baseline", "shared"):
        maint_uri = data_dir / f"maint-{side}-{args.maint_fragments}.lance"
        if maint_uri.exists():
            print(f"{maint_uri} already exists, keeping it")
        else:
            build_maintenance_table(str(maint_uri), args.maint_fragments, args.rows_per_fragment)
            print(
                f"wrote {args.maint_fragments} fragments x {args.rows_per_fragment} rows "
                f"with a BTREE index to {maint_uri}"
            )
    return 0


def run_compare(args: argparse.Namespace) -> int:
    """Compare two result files produced by separate runs of ``--mode``."""
    baseline = json.loads(Path(args.baseline).read_text())
    shared = json.loads(Path(args.shared).read_text())
    failures = compare(baseline, shared, float(baseline["per_open_bytes"]))
    if failures:
        print("\nFAILED:")
        for failure in failures:
            print(f"  - {failure}")
        return 1
    print("\nOK: sharing one session cut the worker's memory as expected.")
    return 0


def run_parent(args: argparse.Namespace) -> int:
    address = os.environ.get("RAY_ADDRESS")
    if address and not address.startswith("local"):
        raise SystemExit(
            f"RAY_ADDRESS={address!r} points at an existing cluster. The A/B needs the worker to inherit this "
            "process's environment, which only holds for a cluster started here, so refusing to report numbers."
        )

    data_dir = Path(args.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    uri = str(data_dir / f"table-{args.fragments}.lance")
    blob_uri = str(data_dir / "blobs.lance")
    if not Path(uri).exists():
        print(f"writing a {args.fragments}-fragment table to {uri} (reused by later runs)", flush=True)
        build_table(uri, args.fragments, args.rows_per_fragment)
    if not Path(blob_uri).exists():
        build_blob_table(blob_uri, min(args.fragments, 64))
    indexed_uri = str(data_dir / f"indexed-{INDEX_ROWS}.lance")
    if not Path(indexed_uri).exists():
        print(f"writing a {INDEX_ROWS}-row table with an IVF_PQ index to {indexed_uri}", flush=True)
        build_indexed_table(indexed_uri)
    for side in ("baseline", "shared"):
        maint_uri = data_dir / f"maint-{side}-{args.maint_fragments}.lance"
        if not maint_uri.exists():
            print(f"writing the {side} maintenance table to {maint_uri}", flush=True)
            build_maintenance_table(str(maint_uri), args.maint_fragments, args.rows_per_fragment)

    # Each child gets its own event-log directory: the sampler keys records by
    # pid, and pids are not unique across runs.
    with tempfile.TemporaryDirectory(prefix="lance-session-events-") as events:
        baseline = spawn_child("baseline", args, Path(events) / "baseline")
        shared = spawn_child("shared", args, Path(events) / "shared")

    payload = payload_sizes(uri)
    small = payload_sizes(blob_uri)
    print(
        f"\nper-task payload: pickling the dataset costs {payload['pickled_dataset'] / 1024:.1f} KiB at "
        f"{args.fragments} fragments versus {small['pickled_dataset'] / 1024:.1f} KiB at "
        f"{min(args.fragments, 64)}; the uri plus pinned open kwargs that replaced it costs "
        f"{payload['pickled_open_kwargs']} and {small['pickled_open_kwargs']} bytes."
    )

    per_open_bytes = float(baseline["per_open_bytes"])
    failures = compare(baseline, shared, per_open_bytes)
    if args.json:
        print(
            json.dumps(
                {"per_open_bytes": per_open_bytes, "baseline": baseline, "shared": shared, "failures": failures},
                indent=2,
            )
        )
    if failures:
        print("\nFAILED:")
        for failure in failures:
            print(f"  - {failure}")
        return 1
    print("\nOK: sharing one session cut the worker's memory as expected.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--fragments",
        type=int,
        default=4000,
        help=(
            "fragments in the table under test. The per-open cost grows with this, and so does the saving, but "
            "the read is one task per fragment, so the run time grows with it too"
        ),
    )
    parser.add_argument(
        "--rows-per-fragment",
        type=int,
        default=ROWS_PER_FRAGMENT,
        help=(
            "rows in each fragment. Below a few thousand a scan task finishes too fast for the opens to overlap, "
            "so fewer manifests are resident at once than --parallel suggests"
        ),
    )
    parser.add_argument(
        "--parallel",
        type=int,
        default=24,
        help=(
            "Ray CPUs and concurrent scan tasks. This, not --fragments, is the lever that makes the saving "
            "stand out of the RSS noise: it multiplies how many manifests are held at once without adding "
            "tasks, so the run takes no longer. It may exceed the machine's core count"
        ),
    )
    parser.add_argument(
        "--read-fraction",
        type=float,
        default=1.0,
        help=(
            "fraction of the table the read stage actually reads, as a prefix of the sorted key. A manifest "
            "costs the same per open whatever the task reads, so this cuts the run time without touching the "
            "measurement -- it is how a 100k-fragment table stays affordable"
        ),
    )
    parser.add_argument(
        "--fragment-group-size",
        type=int,
        default=None,
        help=(
            "fragments per scan task (read_lance's own option). Fewer, longer tasks; each still opens the "
            "dataset once and still carries the whole fragment list, so the per-open cost is unchanged"
        ),
    )
    parser.add_argument(
        "--maint-fragments",
        type=int,
        default=MAINT_FRAGMENTS,
        help=(
            "fragments in the maintenance table. Compaction rewrites all of them, so this is the stage's whole "
            "cost; it does not change what the stage concludes, since a maintenance worker opens the table once"
        ),
    )
    parser.add_argument(
        "--maint-concurrency",
        type=int,
        default=MAINT_CONCURRENCY,
        help=(
            "concurrent compaction / update_columns instances. Each one is a separate process, so this is a "
            "process count, not a thread count -- keep it near the pod's real cpu limit"
        ),
    )
    parser.add_argument(
        "--data-dir",
        default=str(Path(tempfile.gettempdir()) / "daft-lance-session-bench"),
        help="where the test tables live; reused across runs",
    )
    parser.add_argument(
        "--mode",
        choices=["both", "shared", "baseline", "prepare", "compare"],
        default="both",
        help=(
            "both: run the whole A/B locally in two subprocesses. shared/baseline: one side only, for an "
            "orchestrator that runs each side separately (a RayJob per side). prepare: only write the tables. "
            "compare: read two sides' result files and judge them"
        ),
    )
    parser.add_argument("--baseline", help="--mode compare: the baseline side's result JSON")
    parser.add_argument("--shared", help="--mode compare: the shared side's result JSON")
    parser.add_argument("--json", action="store_true", help="also dump the raw measurements")
    args = parser.parse_args()
    if args.mode == "compare":
        if not args.baseline or not args.shared:
            raise SystemExit("--mode compare needs both --baseline and --shared")
        return run_compare(args)
    if args.parallel < 2:
        raise SystemExit("--parallel must be at least 2 for the comparison to mean anything")
    if not 0 < args.read_fraction <= 1.0:
        raise SystemExit("--read-fraction must be in (0, 1]")
    if args.mode == "prepare":
        return run_prepare(args)
    return run_parent(args) if args.mode == "both" else run_child(args)


if __name__ == "__main__":
    raise SystemExit(main())
