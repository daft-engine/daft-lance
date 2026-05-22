## Many Fragments

TL;DR — Daft's Lance sink was writing one fragment per Daft micropartition
(~131K rows), producing ~8x more fragments than Lance's default
`max_rows_per_file=1_048_576` would imply. This caused slow writes and much
slower reads. The fix is now in `daft_lance.lance_data_sink.LanceDataSink`:
the in-sink buffer always accumulates across micropartitions within a single
worker's `write()` call, gated on both rows and bytes against the typed
`max_rows_per_file` / `max_bytes_per_file` parameters.

## Original investigation

The customer report compared three configurations on a 10M-row write.

#### Write performance

| Approach         | Fragment size  | Write time (s) | # Fragments | Dataset size (MB) |
|------------------|----------------|---------------:|------------:|------------------:|
| Daft (default)   | ~12K rows      |          22.9  |         804 |              4720 |
| Daft (patched)   | ~100K rows     |           6.0  |          98 |              4720 |
| Lance native     | ~100K rows     |           5.8  |          97 |              4720 |

#### Read performance (full scan)

| Approach         | Read time (s) |
|------------------|--------------:|
| Daft (default)   |          5.18 |
| Daft (patched)   |          1.12 |
| Lance native     |          1.11 |

The "patched" config was an external prototype that bumped Daft's morsel
size and added an in-sink row accumulator. That prototype is now obsolete —
the equivalent behavior is the default in `LanceDataSink`.

## Post-refactor verification

`tests/benchmarks/test_lance_data_sink_bench.py` (250K-row scale; results
in `docs/lance_data_sink/post_refactor.json`) confirms the in-worker fix:

| Test                                     | Setup                                            | n_fragments | Wall (s) |
|------------------------------------------|--------------------------------------------------|------------:|---------:|
| W2 `dsv-default`                         | 250K rows, single partition, default knobs       |           1 |    0.028 |
| W2 `dsv-2.0`                             | 250K rows, `data_storage_version="2.0"`          |           1 |    0.013 |
| W2 `dsv-2.1`                             | 250K rows, `data_storage_version="2.1"`          |           1 |    0.043 |
| W2 `dsv-2.2`                             | 250K rows, `data_storage_version="2.2"`          |           1 |    0.053 |
| W2 `dsv-2.3`                             | 250K rows, `data_storage_version="2.3"`          |           1 |    0.035 |
| W1 `mrpf-none-morsel-default`            | 250K rows split across 10 forced partitions      |          10 |    0.069 |
| W1 `mrpf-1M-morsel-default`              | same, `max_rows_per_file=1M` explicit             |          10 |    0.067 |
| W1 `mrpf-100k-morsel-default`            | same, `max_rows_per_file=100K`                   |          10 |    0.141 |

W2 (single-partition write) produces one fragment regardless of how many
micropartitions Daft's executor yields — the within-task buffer coalesces
them up to `max_rows_per_file`. This is the customer-pain scenario, fixed.

W1 (explicit `df.into_partitions(10)`) still produces one fragment per
task. This is by design for the current pass; cross-worker coalescing
would require executor-level coordination and is out of scope. Users who
explicitly partition for parallel writes should expect ≥ 1 fragment per
partition.

## Storage versions

Every value in the `data_storage_version` Literal (`"legacy"`, `"0.1"`,
`"stable"`, `"2.0"`, `"2.1"`, `"2.2"`, `"2.3"`, `"next"`) now round-trips
through the sink. Write times on a 250K-row default workload:

| Version    | Write (s) |
|------------|----------:|
| `"2.0"`    |     0.013 |
| `"default"`|     0.028 |
| `"2.3"`    |     0.035 |
| `"2.1"`    |     0.043 |
| `"2.2"`    |     0.053 |

`"2.0"` is fastest to write; `"2.1"+` are 3-4x slower for this random
workload but produce slightly smaller on-disk output and unlock features
(stable row IDs, Blob V2, etc.). The sink does not change the default —
that decision belongs to Lance.

## What landed

- `max_rows_per_file`, `max_rows_per_group`, `max_bytes_per_file`,
  `data_storage_version`, `use_legacy_format`, `enable_stable_row_ids`,
  and `storage_options` are first-class typed kwargs on `LanceDataSink`.
- `data_storage_version` is inherited from the existing dataset on
  `append` when the caller does not specify; an explicit mismatch raises
  at construction.
- `mode="merge"` is rejected at `__init__` (was silently broken at finalize).
- Blob V2 column wrapping, validation, and schema derivation are
  consolidated behind `BlobV2WritePolicy` in `daft_lance/_blob.py`.
- All review findings (see `docs/lance_data_sink/04_review.md`) are
  addressed; full requirements at `docs/lance_data_sink/03_requirements.md`.

## Limitations

- Cross-worker fragment coalescing is unaddressed (W1 result above).
- Blob V2 sidecar `.blob` files are not counted in
  `WriteResult.bytes_written` — deferred as NFR-7 in
  `docs/lance_data_sink/03_requirements.md`.
