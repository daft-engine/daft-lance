# Vector search benchmark

This benchmark compares search strategies on **the same dataset snapshot and
index files**. It does not include data generation or index training in query
timings. It works with synthetic data or an existing table and independent query
embeddings. Install the project's dev and Ray dependencies plus `psutil` first.
You can run the commands below by replacing `python` with
`uv run --group ray --with psutil python` to resolve these dependencies.

## Prepare once

```bash
python benchmarks/vector_search.py prepare --root /tmp/vector-bench \
  --rows 1000000 --dim 128 --segments 8 --queries 100 --cpu-budget 8
```

Generation uses a fixed seed and a mixture of 16 Gaussian clusters. Queries use
an independent random stream, not vectors selected from stored rows. Each fragment
becomes an IVF_FLAT segment; all segments share the globally trained centroids.
The default 32 probes visit all 32 partitions, making FLAT search exact. Dataset
metadata and query vectors are saved beside the table. Preparation refuses to
overwrite an existing table.

## Compare end-to-end query execution

Run cases **serially**, each in a fresh process, on an otherwise idle machine.
For each strategy, use the same root, query count, K, probes, filter and recall gate.

```bash
python benchmarks/vector_search.py run --root /tmp/vector-bench \
  --backend pylance --cpu-budget 4 --queries 20 --output /tmp/results.jsonl
python benchmarks/vector_search.py run --root /tmp/vector-bench \
  --backend daft-single --runner ray --workers 4 --cpu-budget 4 \
  --queries 20 --output /tmp/results.jsonl
python benchmarks/vector_search.py run --root /tmp/vector-bench \
  --backend segments --runner ray --workers 4 --cpu-budget 4 \
  --queries 20 --output /tmp/results.jsonl
python benchmarks/vector_search.py run --root /tmp/vector-bench \
  --backend queries --runner ray --workers 4 --cpu-budget 4 \
  --queries 20 --output /tmp/results.jsonl
```

Strategies:

| Backend | Execution |
| --- | --- |
| `pylance` | Sequential queries on one reusable LanceDataset |
| `daft-single` | Existing read_lance nearest path, one task per query, sequential queries |
| `segments` | Multiple segment scan tasks per query and a global Top-K in Daft |
| `queries` | One scan task per query over all segments, multiple queries in one Daft plan |

`segments` and `queries` reuse a pinned worker dataset, preserving its index
cache between tasks. The existing `daft-single` path opens a dataset per scan
task. These cache lifetimes are part of the implementations being compared.
The latter two backends time lazy planning, execution, Arrow materialization
and grouping results by query ID. All backends project IDs and produce K IDs
per query. Segment execution additionally carries row IDs and distances to
perform its merge. Ray startup is excluded; first execution includes worker
initialization and index loading. A first execution is **not a cold-storage
measurement**: OS and object-store caches are not reset.

For single-query latency use `--queries 1`. With multiple queries the recorded
p50/p95 are **batch latency**, not individual-query percentiles. The default
10 measured repetitions are exploratory; use `--repeats 100` for steadier tail
estimates. Each repetition builds a fresh DataFrame, avoiding Daft result-cache
reuse. Warmups and brute-force ground truth run outside measured repetitions.

## Resource and quality controls

- **Fixed CPU:** keep `--cpu-budget 4`, vary `--workers 1/2/4`. On Linux,
  CPU affinity bounds the driver and inherited local Ray process tree to the
  same four logical CPUs. Rayon/BLAS thread limits are set before imports.
  `workers` configures the Ray scheduler CPU budget; it is not a promise that
  each logical scan task owns a distinct process.
- **Scaling:** compare `(cpu-budget, workers) = (1,1), (2,2), (4,4), (8,8)`.
  Report the extra CPU cost with speedup. Affinity is not portable to remote
  clusters; those require explicit worker placement and CPU limits.
- **Quality:** every query is compared against a brute-force Lance query of
  the same version and predicate. Below `--min-recall` (default 0.99), the run
  fails and writes no performance record. Approximate settings or IVF_PQ need
  a justified quality threshold; compare configurations meeting the same
  threshold rather than claiming speed from lower recall.
- **Filters:** `--selectivity 0.1` and `0.01` prefilter the synthetic `bucket`
  column. `--filter` overrides this with a Lance SQL predicate. K is global
  per query, not per segment.
- **Data/layout:** vary rows (100K/1M/10M), dimensions (128/768), segments
  (1/4/8/16), K (10/100), probes and query batch size independently. Start
  with a small matrix, then expand around observed bottlenecks.
- **Cost:** output includes sampled local process-tree CPU seconds,
  aggregate peak RSS and physical disk read bytes. Shared pages are counted
  in each process's RSS; remote workers are excluded. Physical reads can be
  zero for cached data and are not object-store request/transfer statistics.
  Use storage-side metrics for TOS/S3 requests and bytes. These are local
  resource observations, not production billing estimates.

Records append as JSONL with dependency versions, Git state, snapshot version,
CPU affinity, parameters, raw durations, recall and execution plan. Keep the
dataset metadata with results. Local disk results do not predict object-store
or multi-node behavior.

## Existing embeddings and object storage

```bash
python benchmarks/vector_search.py run --root /tmp/real-bench \
  --uri s3://bucket/table.lance --query-file /path/independent-queries.npy \
  --column embedding --id-column id --index-name embedding_idx \
  --backend segments --runner ray --workers 4 --cpu-budget 4 \
  --queries 20 --nprobes 32 --min-recall 0.95 --output /tmp/real-results.jsonl
```

Use environment-based object-store credentials and a completely covered index.
Queries must be a finite matrix with the same dimension as the vector field;
the ID column must uniquely identify rows for recall calculation. Use an
explicit predicate for existing tables instead of synthetic `--selectivity`.
