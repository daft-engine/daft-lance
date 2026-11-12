# Session performance observations

These are historical observations from the original PR environment, not API guarantees.
Process placement and cache accounting depend on the Daft/Lance version. In particular,
maintenance instances need not occupy distinct processes, and cache-byte counters are
not an RSS measurement. The old process-layout and cache-ratio assertions have been
removed from functional tests.

## Local append comparison

Daft 0.7.14, pylance 12.0.0, local Ray with 4 CPUs: read and append 10,000 rows
with scan batch size 16 into an existing target. Each setting ran once, sequentially.
Targets used distinct fragment IDs referencing one seed file to isolate manifest cost.
CPU was sampled over the process tree every 250 ms; 100% is one core. Timing excludes
preparation, Ray warmup, and validation. All appended values and total row counts passed.

| Target fragments | Manifest | Session | Wall seconds | CPU seconds |
| --- | --- | --- | --- | --- |
| 10,000 | 1.60 MiB | Off | 3.48 | 3.67 |
| 10,000 | 1.60 MiB | On | 2.03 | 2.04 |
| 100,000 | 16.18 MiB | Off | 25.41 | 26.64 |
| 100,000 | 16.18 MiB | On | 2.75 | 2.94 |

These measurements preceded the lifecycle simplification. They were not rerun for that
refactor and do not establish performance with five million fragments or remote storage.
The pipeline produced 625 additional fragments; this PR does not address small-file buffering.
Benchmark scripts are not included in this PR.

## Earlier cluster observations

The original KubeRay `read_lance` to `write_parquet` experiments reported:

| Input / execution | Stage seconds, unshared → shared | Worker peak RSS growth, unshared → shared |
| --- | --- | --- |
| 4,000 fragments × 10k rows, concurrency 24 | 11.3 → 10.4 | 206.5 → 157.4 MB |
| 100k fragments × 10k rows, 1% scan, groups of 10, concurrency 8 | 609.6 → 61.9 | 1052.2 → 573.3 MB |

Repeated nearest queries on a 200k-row IVF_PQ table reportedly improved 1.7–2.0×.
In that environment maintenance instances occupied separate processes and compaction
index-cache growth was concentrated in the commit. These are version-specific observations,
not assumptions used by the implementation or requirements imposed by its tests.
