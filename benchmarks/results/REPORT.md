# Exploratory local vector-search results — 2026-10-09

These historical measurements precede the upstream rebase and review fixes
for bounded local candidates and row-balanced segment grouping. The hashes
in the raw JSONL identify the measured code; the numbers below do not measure
the current implementation.

Measured on one Intel Xeon Platinum 8457C host with local disk. All cases use
128-dimensional clustered synthetic vectors, 8 IVF_FLAT segments, 32 partitions,
all 32 probes, L2, K=10, no filter, complete coverage and independent query vectors.
Linux CPU affinity confines the process tree to CPUs 0–3, except the explicitly
labelled 8-CPU scaling case. Daft 0.7.14, pylance 12.0.0, Python 3.11.2 and
Ray 2.54.0. Index training and Ray startup are outside the timers.

Five measured repetitions follow two warmups for batch runs; single-query runs
have ten repetitions. These are exploratory medians and percentiles, not a
production tail-latency guarantee. Ray cases run in fresh local clusters,
serially. First-run measurements are stored in JSONL but are not cold-storage
measurements. Ground-truth queries and warmups precede timed repetitions.

Single-task baselines receive the whole 4-thread budget. Distributed workers
receive budget/workers threads and share the same CPU affinity budget. The
Ray scheduler CPU budget is not an observed count of distinct Python processes.
All records passed the brute-force recall gate at 1.00. Source hashes, where
present, identify the measured files; earlier exploratory records identify the
Git base and dirty tree. Dataset roots were `/tmp/daft-vector-bench-20261009`
and `/tmp/daft-vector-bench-1m-20261009`, seed 42; queries use seed 43.

## 100K rows: batches of 5 queries

| Strategy | Runner | Ray CPU budget | CPU affinity budget | p50 ms | p95 ms | Recall@10 | Peak local aggregate RSS MiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| pylance | native | — | 4 | 35.73 | 39.57 | 1.00 | 400 |
| daft-single | native | — | 4 | 243.19 | 246.95 | 1.00 | 473 |
| segments | ray | 1 | 4 | 596.42 | 628.14 | 1.00 | 2027 |
| segments | ray | 2 | 4 | 413.50 | 453.53 | 1.00 | 2120 |
| segments | ray | 4 | 4 | 338.86 | 360.98 | 1.00 | 2150 |
| queries | ray | 4 | 4 | 159.25 | 163.91 | 1.00 | 2319 |
| daft-single | ray | 4 | 4 | 322.54 | 329.04 | 1.00 | 2054 |

Raw measurements: [local-100k.jsonl](local-100k.jsonl).

## 1M rows: batches of 5 queries

| Strategy | Runner | Ray CPU budget | CPU affinity budget | p50 ms | p95 ms | Recall@10 | Peak local aggregate RSS MiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| pylance | native | — | 4 | 173.15 | 178.56 | 1.00 | 1026 |
| segments | ray | 1 | 4 | 938.32 | 957.12 | 1.00 | 2773 |
| segments | ray | 2 | 4 | 540.92 | 559.21 | 1.00 | 2897 |
| segments | ray | 4 | 4 | 436.53 | 458.80 | 1.00 | 2981 |
| queries | ray | 4 | 4 | 266.66 | 271.12 | 1.00 | 4371 |
| daft-single | ray | 4 | 4 | 973.97 | 1004.66 | 1.00 | 2996 |

Raw measurements: [local-1m.jsonl](local-1m.jsonl).

## 1M rows: one query per run

| Strategy | Runner | Ray CPU budget | CPU affinity budget | p50 ms | p95 ms | Recall@10 | Peak local aggregate RSS MiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| pylance | native | — | 4 | 34.75 | 36.33 | 1.00 | 1022 |
| segments | ray | 1 | 4 | 180.47 | 182.43 | 1.00 | 2462 |
| segments | ray | 2 | 4 | 117.33 | 138.01 | 1.00 | 2454 |
| segments | ray | 4 | 4 | 91.58 | 100.47 | 1.00 | 2772 |
| segments | ray | 8 | 8 | 68.81 | 72.84 | 1.00 | 3098 |
| daft-single | ray | 4 | 4 | 205.47 | 223.65 | 1.00 | 2819 |

Raw measurements: [local-1m-single-query.jsonl](local-1m-single-query.jsonl).

## Interpretation

- At 1M rows and a fixed 4-CPU budget, one query takes about **205 ms** through
  the existing Ray single-task path and **92 ms** with segment tasks: about
  **2.24x** faster. Four segment workers are about **1.97x** faster than the
  new API's one-worker configuration (180 ms), which also benefits from worker
  dataset caching. Raising the budget to 8 CPUs gives about 69 ms, at higher
  resource cost.
- For batches of five at 1M rows, segment tasks take about **437 ms**, versus
  **974 ms** for the existing Ray single-task path. Independent-query tasks
  take about **267 ms**, making them the better strategy in this measured
  batch workload.
- At 100K rows, segment scheduling costs dominate: **339 ms** versus
  **323 ms** for the existing Ray single-task path. Sharding is not a general
  latency improvement for small datasets.
- Native pylance remains faster in every measured workload: about **35 ms**
  per query at 1M rows. This work improves the Daft execution path and adds
  distributed scheduling; these data do not establish an advantage over a
  native scanner on one machine.
- RSS includes shared pages more than once and includes local Ray services.
  It is an aggregate process-tree observation, not unique resident memory or
  remote-cluster cost. Physical read bytes are OS disk reads, not storage
  request counts or object-store transfer bytes.

Use the opt-in vector_search API when its workload-specific benchmark shows
benefit. For batches, compare query-level parallelism (`num_tasks=1` with a
query matrix) first. Small nprobes, compression, selective filters, uneven
segments, real embeddings, remote storage and multi-node execution need
separate measurements before choosing deployment settings. The benchmark
supports these controls, but the above table does not measure them.
