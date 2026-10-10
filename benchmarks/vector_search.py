"""Reproducible vector-search benchmark; see benchmarks/README.md."""

from __future__ import annotations

import argparse
import json
import os
import platform
import subprocess
import threading
import time
from pathlib import Path
from typing import Any


def _prepare(args: argparse.Namespace) -> None:
    import lance
    import numpy as np
    import pyarrow as pa

    from daft_lance import create_vector_index

    root = Path(args.root)
    root.mkdir(parents=True, exist_ok=True)
    uri = str(root / "data.lance")
    if Path(uri).exists():
        raise ValueError("Dataset already exists; use a new root to avoid replacing benchmark data")
    rng = np.random.default_rng(args.seed)
    centers = rng.normal(size=(16, args.dim)).astype("float32")
    for segment in range(args.segments):
        start = args.rows * segment // args.segments
        stop = args.rows * (segment + 1) // args.segments
        labels = rng.integers(0, len(centers), stop - start)
        vectors = centers[labels] + rng.normal(scale=0.5, size=(stop - start, args.dim)).astype("float32")
        lance.write_dataset(
            pa.table(
                {
                    "id": np.arange(start, stop),
                    "bucket": rng.integers(0, 1000, stop - start),
                    "vector": pa.FixedSizeListArray.from_arrays(pa.array(vectors.ravel()), args.dim),
                }
            ),
            uri,
            mode="create" if segment == 0 else "append",
            max_rows_per_file=stop - start,
        )
    # Independent queries, drawn from the same mixture rather than stored rows.
    query_rng = np.random.default_rng(args.seed + 1)
    queries = centers[query_rng.integers(0, len(centers), args.queries)] + query_rng.normal(
        scale=0.5, size=(args.queries, args.dim)
    ).astype("float32")
    np.save(root / "queries.npy", queries)
    create_vector_index(
        uri,
        column="vector",
        index_type=args.index_type,
        name="bench_idx",
        num_partitions=args.partitions,
        num_sub_vectors=args.subvectors if args.index_type == "IVF_PQ" else None,
        metric=args.metric,
        sample_rate=2,
        fragment_group_size=1,
    )
    ds = lance.dataset(uri)
    metadata = {
        key: getattr(args, key)
        for key in ("rows", "dim", "segments", "queries", "seed", "index_type", "partitions", "metric")
    }
    metadata.update(version=ds.version, actual_segments=len(ds.describe_indices()[0].segments))
    (root / "dataset.json").write_text(json.dumps(metadata, indent=2) + "\n")
    print(json.dumps(metadata))


class _Resources:
    """Sample local driver descendants; remote workers are outside this metric."""

    def __init__(self) -> None:
        import psutil

        self.process = psutil.Process()
        self.stop = threading.Event()
        self.peak_rss = 0
        self.initial: dict[int, tuple[float, int]] = {}
        self.cpu_seconds = 0.0
        self.read_bytes = 0
        self.thread = threading.Thread(target=self._sample, daemon=True)

    def _sample_once(self) -> None:
        import psutil

        rss = 0
        cpu = 0.0
        read_bytes = 0
        for process in [self.process, *self.process.children(recursive=True)]:
            try:
                times = process.cpu_times()
                current = times.user + times.system
                reads = process.io_counters().read_bytes
                initial_cpu, initial_reads = self.initial.setdefault(process.pid, (current, reads))
                cpu += current - initial_cpu
                read_bytes += reads - initial_reads
                rss += process.memory_info().rss
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        self.peak_rss = max(self.peak_rss, rss)
        self.cpu_seconds = max(self.cpu_seconds, cpu)
        self.read_bytes = max(self.read_bytes, read_bytes)

    def _sample(self) -> None:
        while not self.stop.wait(0.02):
            self._sample_once()

    def __enter__(self) -> _Resources:
        self._sample_once()
        self.thread.start()
        return self

    def __exit__(self, *args: Any) -> None:
        self.stop.set()
        self.thread.join()
        self._sample_once()


def _run(args: argparse.Namespace) -> None:
    import hashlib
    import io

    import lance
    import numpy as np
    import pyarrow as pa

    import daft
    from daft_lance import read_lance, vector_search

    root = Path(args.root)
    uri = args.uri or str(root / "data.lance")
    queries = np.load(args.query_file or root / "queries.npy")[: args.queries]
    if len(queries) == 0:
        raise ValueError("Query file is empty")
    ds = lance.dataset(uri)
    version = ds.version
    # The single-scan baselines select their index through Lance. Requiring one
    # relevant index makes every backend use the same files without assuming
    # Lance's default selection agrees with an explicit distributed selection.
    indices = [
        idx
        for idx in ds.describe_indices()
        if str(idx.index_type).startswith("IVF_") and args.column in idx.field_names
    ]
    if len(indices) != 1:
        raise ValueError("Benchmark requires exactly one vector index for the selected column")
    index_name = indices[0].name
    if args.index_name is not None and args.index_name != index_name:
        raise ValueError(f"Requested index {args.index_name!r} does not match the column's index {index_name!r}")
    predicate = args.filter
    if predicate is None and args.selectivity < 1:
        predicate = f"bucket < {round(1000 * args.selectivity)}"
    base_nearest = {"column": args.column, "k": args.k, "nprobes": args.nprobes, "metric": args.metric}
    if args.refine_factor is not None:
        base_nearest["refine_factor"] = args.refine_factor
    # Explicit segments make Lance reject a metric mismatch instead of allowing
    # a baseline to silently fall back to brute-force search.
    ds.scanner(
        columns=[args.id_column],
        nearest={**base_nearest, "q": queries[0]},
        index_segments=[str(segment.uuid) for segment in indices[0].segments],
        filter=predicate,
        prefilter=True,
        fast_search=True,
    ).projected_schema
    ray_module = None
    if args.runner == "ray" and args.backend != "pylance":
        import ray

        ray_module = ray
        ray.init(num_cpus=args.workers, include_dashboard=False, object_store_memory=128 * 1024 * 1024)
        daft.set_runner_ray()

    def search() -> list[list[int]]:
        if args.backend in ("segments", "queries"):
            df = vector_search(
                uri,
                version=version,
                nearest={**base_nearest, "q": queries},
                index_name=index_name,
                columns=[args.id_column],
                filter=predicate,
                num_tasks=1 if args.backend == "queries" else args.tasks,
            )
            table = df.to_arrow()
            return [
                table.filter(pa.compute.equal(table["_query_id"], query_id))[args.id_column].to_pylist()
                for query_id in range(len(queries))
            ]
        results = []
        for q in queries:
            nearest = {**base_nearest, "q": q}
            if args.backend == "pylance":
                table = ds.scanner(
                    columns=[args.id_column, "_distance"],
                    nearest=nearest,
                    filter=predicate,
                    prefilter=True,
                    with_row_id=True,
                ).to_table()
            else:
                df = read_lance(uri, version=version, default_scan_options={"nearest": nearest, "prefilter": True})
                if predicate is not None:
                    df = df.where(daft.sql_expr(predicate))
                table = df.select(args.id_column).to_arrow()
            results.append(table[args.id_column].to_pylist())
        return results

    # Exact ground truth is outside all timed measurements and computed after
    # the first query so it cannot prewarm the first-query cache observation.
    with _Resources() as first_resources:
        start = time.perf_counter()
        first_results = search()
        first_seconds = time.perf_counter() - start
    truth = []
    for q in queries:
        table = ds.scanner(
            columns=[args.id_column],
            nearest={"column": args.column, "q": q, "k": args.k, "metric": args.metric, "use_index": False},
            filter=predicate,
            prefilter=True,
        ).to_table()
        truth.append(set(table[args.id_column].to_pylist()))

    def recall(result: list[list[int]]) -> float:
        return float(
            np.mean(
                [len(set(got) & expected) / len(expected) if expected else 1.0 for got, expected in zip(result, truth)]
            )
        )

    if recall(first_results) < args.min_recall:
        raise RuntimeError(f"Recall {recall(first_results):.4f} is below required {args.min_recall}")
    for _ in range(args.warmup):
        search()
    durations = []
    recalls = []
    with _Resources() as resources:
        for _ in range(args.repeats):
            start = time.perf_counter()
            result = search()
            durations.append(time.perf_counter() - start)
            recalls.append(recall(result))
    if min(recalls) < args.min_recall:
        raise RuntimeError("A timed run failed the recall gate")
    explanation = None
    if args.backend in ("segments", "queries"):
        output = io.StringIO()
        vector_search(
            uri,
            version=version,
            nearest={**base_nearest, "q": queries[0]},
            index_name=index_name,
            columns=[args.id_column],
            filter=predicate,
            num_tasks=1 if args.backend == "queries" else args.tasks,
        ).explain(show_all=True, file=output)
        explanation = output.getvalue()
    record = {
        "backend": args.backend,
        "runner": args.runner,
        "workers": args.workers,
        "cpu_budget": args.cpu_budget,
        "cpu_affinity": sorted(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None,
        "threads_per_process": os.environ["RAYON_NUM_THREADS"],
        "dataset_version": version,
        "index_name": index_name,
        "rows": ds.count_rows(),
        "segments": {idx.name: len(idx.segments) for idx in ds.describe_indices()},
        "queries_per_run": len(queries),
        "nearest": base_nearest,
        "filter": predicate,
        "tasks": args.tasks,
        "first_run_seconds": first_seconds,
        "first_run_recall": recall(first_results),
        "warm_batch_p50_seconds": float(np.percentile(durations, 50)),
        "warm_batch_p95_seconds": float(np.percentile(durations, 95)),
        "queries_per_second": len(queries) * len(durations) / sum(durations),
        "recall_min": min(recalls),
        "local_cpu_seconds": resources.cpu_seconds,
        "local_physical_read_bytes": resources.read_bytes,
        "local_peak_aggregate_rss_bytes": resources.peak_rss,
        "first_run_peak_aggregate_rss_bytes": first_resources.peak_rss,
        "durations_seconds": durations,
        "execution_plan": explanation,
        "versions": {"daft": daft.__version__, "pylance": lance.__version__, "python": platform.python_version()},
        "ray_version": ray_module.__version__ if ray_module is not None else None,
        "source_sha256": {
            str(path.relative_to(Path(__file__).resolve().parents[1])): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in [
                Path(__file__).resolve(),
                Path(__file__).resolve().parents[1] / "daft_lance/lance_vector_search.py",
                Path(__file__).resolve().parents[1] / "daft_lance/_lance.py",
            ]
        },
        "git_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "git_dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], text=True).strip()),
    }
    destination = Path(args.output)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("a") as output_file:
        output_file.write(json.dumps(record) + "\n")
    print(json.dumps({key: value for key, value in record.items() if key != "execution_plan"}, indent=2))
    if ray_module is not None:
        ray_module.shutdown()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["prepare", "run"])
    parser.add_argument("--root", required=True)
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--dim", type=int, default=128)
    parser.add_argument("--segments", type=int, default=8)
    parser.add_argument("--queries", type=int, default=20)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--partitions", type=int, default=32)
    parser.add_argument("--index-type", choices=["IVF_FLAT", "IVF_PQ"], default="IVF_FLAT")
    parser.add_argument("--subvectors", type=int, default=16)
    parser.add_argument("--backend", choices=["pylance", "daft-single", "segments", "queries"], default="pylance")
    parser.add_argument("--runner", choices=["native", "ray"], default="native")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--cpu-budget", type=int, default=4)
    parser.add_argument("--tasks", type=int)
    parser.add_argument("--k", type=int, default=10)
    parser.add_argument("--nprobes", type=int, default=32)
    parser.add_argument("--metric", choices=["l2", "cosine", "dot"], default="l2")
    parser.add_argument("--refine-factor", type=int)
    parser.add_argument("--selectivity", type=float, default=1)
    parser.add_argument("--filter")
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument("--repeats", type=int, default=10)
    parser.add_argument("--min-recall", type=float, default=0.99)
    parser.add_argument("--output", default="benchmark-results.jsonl")
    parser.add_argument("--uri", help="Existing local or object-store table; prepare is not required")
    parser.add_argument("--query-file", help="Independent query vectors stored as a NumPy matrix")
    parser.add_argument("--column", default="vector")
    parser.add_argument("--id-column", default="id")
    parser.add_argument("--index-name")
    args = parser.parse_args()
    os.environ["DAFT_PROGRESS_BAR"] = "0"
    os.environ["DAFT_ANALYTICS_ENABLED"] = "0"
    if args.cpu_budget <= 0 or args.workers <= 0 or args.repeats <= 0 or args.queries <= 0:
        parser.error("CPU budget, workers, repeats and queries must be positive")
    if not 0 < args.selectivity <= 1 or not 0 <= args.min_recall <= 1:
        parser.error("selectivity must be in (0, 1] and min-recall in [0, 1]")
    if args.rows < args.segments or args.segments <= 0 or args.dim <= 0:
        parser.error("Require rows >= segments > 0 and dim > 0")
    if hasattr(os, "sched_setaffinity"):
        available = sorted(os.sched_getaffinity(0))
        if args.cpu_budget > len(available):
            parser.error("CPU budget exceeds this process's available CPU affinity")
        os.sched_setaffinity(0, available[: args.cpu_budget])
    # A sequential single-task baseline can use the whole CPU budget. Giving
    # it only budget/workers threads would manufacture a sharding speedup.
    distributed = args.runner == "ray" and args.backend in ("segments", "queries")
    threads = max(1, args.cpu_budget // args.workers) if distributed else args.cpu_budget
    os.environ["RAYON_NUM_THREADS"] = str(threads)
    os.environ["OMP_NUM_THREADS"] = str(threads)
    os.environ["OPENBLAS_NUM_THREADS"] = str(threads)
    if args.action == "prepare":
        _prepare(args)
    else:
        _run(args)


if __name__ == "__main__":
    main()
