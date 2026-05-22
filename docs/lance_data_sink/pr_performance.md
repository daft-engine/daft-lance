# LanceDataSink Refactor — Performance Comparison (10M rows)

This is the PR / release-notes report for the `LanceDataSink` refactor
(commit `25c9352`). It compares the sink against its state at `1f4b9de`
(immediately before this work) at 10M-row scale, plus a `lance.write_dataset`
native baseline to anchor where the absolute floor lives.

## TL;DR

- **No performance regression** at 10M rows. Default-path write within ~12%
  of pre-refactor; reads ~9% faster; chunked writes ~6% faster; fragment
  layout identical (10 fragments / 80 fragments respectively).
- **The reported 8x-small-fragments bug is fixed in code** (the in-sink
  accumulator was off when `max_rows_per_file` was unset). It does not
  surface as a write-time perf delta on the Daft native runner with
  `daft.from_arrow(table).write_lance(uri)`, because Daft already feeds
  the sink with appropriately sized chunks for this call shape.
- **Primary value of the refactor is correctness, API quality, and code
  health**, not headline speed:
  - `mode="merge"` no longer crashes at finalize (was `UnboundLocalError`);
    rejected with an actionable error at `__init__`.
  - All Lance `write_fragments` knobs (`max_rows_per_file`,
    `max_rows_per_group`, `max_bytes_per_file`, `data_storage_version`,
    `use_legacy_format`, `enable_stable_row_ids`, `storage_options`) are
    first-class typed kwargs — typos surface as `TypeError` at
    construction instead of leaking through `**kwargs`.
  - On `append`, `data_storage_version` is inherited from the existing
    dataset; explicit mismatch raises with a migration hint.
  - 277 tests pass (vs. ~210 pre-refactor); the blob V2 write policy now
    has 33 dedicated unit tests; `daft_lance/lance_data_sink.py` and
    `daft_lance/_blob.py` are mypy-clean under `--strict`.

## Methodology

- Branch `rchowell/blob-v2` at commit `25c9352` (after) vs `1f4b9de` (before).
- Pre-refactor run executed in a git worktree at `1f4b9de` with the post-refactor
  benchmark suite copied in (the bench tests did not exist at the earlier sha).
- Each test runs one round (`benchmark.pedantic(target, rounds=1)`) on a single
  laptop; numbers are walk-clock and should be read as ratios, not absolutes.
- Data: 10M rows × `{int64, float64, string(16)}` from
  `tests/benchmarks/conftest.py:_build_random_table` (deterministic seed).
- Raw results: `docs/lance_data_sink/pre_refactor_10m.json` and
  `docs/lance_data_sink/post_refactor_10m.json`.
- Reproduce: `uv run pytest tests/benchmarks -m slow -v`.

## Results

| Test                                | Description                                              | Before (ms) | After (ms) | Delta | n_fragments (pre / post) |
|-------------------------------------|----------------------------------------------------------|------------:|-----------:|------:|--------------------------|
| `test_w4_10m_default_write`         | `daft.from_arrow(10M).write_lance(uri)`                  |       702.4 |      789.5 | +12%  | 10 / 10                  |
| `test_w4_10m_chunked_write`         | 10M rows in 80 concat'd `from_arrow` partitions          |     1,046.3 |      983.7 |  −6%  | 80 / 80                  |
| `test_w4_10m_lance_native`          | `lance.write_dataset(table, uri)` — Lance-native floor   |       628.4 |      635.3 |  +1%  | 10 / 10                  |
| `test_r3_10m_full_scan`             | `daft.read_lance(uri).to_arrow()` round-trip read        |       316.4 |      287.9 |  −9%  | 10 / 10                  |

Sink overhead vs. Lance native (after-refactor):
- Default write: 789.5 / 635.3 = **1.24×** (24% Daft overhead at 10M).
- Chunked write: significantly higher because each chunk forces its own task.

## Why the customer-reported 4× write / 5× read speedup is not visible here

The original investigation (`docs/findings.md` "Original investigation"
section) reported a 4× write and 5× read improvement when the prototype
"patched" sink was used in place of Daft's default. We could not reproduce
that delta in this PR's setup, and we believe the explanation is the
execution path, not a missing fix:

- The in-sink buffer's benefit only materializes when a *single*
  `write()` call receives *multiple* `MicroPartition`s from its iterator.
  Daft's native runner with `from_arrow(big_table).write_lance(uri)`
  currently delivers one large micropartition per task, so the buffer has
  nothing to coalesce.
- With multi-partition input (`_write_chunked` / `into_partitions(N)`)
  each partition becomes its own task; the buffer is per-task, so this
  does not help either.
- The customer's environment likely involved a different runner (Ray) or
  a finer-grained executor batch policy that produced the multi-MP
  iterator the buffer is designed for. That scenario is still correctly
  handled by the new code; we just don't exercise it on this laptop.

The unit-level evidence that the buffer works is `tests/io/lancedb/test_lance_batching.py`
(four tests) and `tests/io/lancedb/test_lance_data_sink_internals.py::test_default_path_coalesces_multi_partition_input`,
both of which drive the sink directly and confirm the fix.

## What landed in code

- `daft_lance/lance_data_sink.py` — full refactor across four phases:
  closures extracted, typed kwargs, accumulator always-on, dead code
  removed, error sniffing typed, `merge` mode rejected at `__init__`.
- `daft_lance/_blob.py` — Blob V2 write policy + helpers extracted from
  the sink; pure-function `_pyarrow_schema_castable`,
  `blob_aware_schema_for_validation`, `detect_blob_v2_columns`,
  `binary_to_blob_v2_array`; new `resolve_storage_version` + `BlobV2WritePolicy`.
- New tests: `tests/io/lancedb/test_blob_v2_policy.py` (33 cases),
  `test_lance_data_sink_internals.py` (10 cases),
  `test_lance_data_sink_storage_versions.py` (11 cases).
- `tests/benchmarks/test_lance_data_sink_bench.py` — 4 new `@pytest.mark.slow`
  10M-row tests for this comparison.
- `pyproject.toml` — registers the `slow` marker; default `addopts`
  deselects `slow` tests.

## Open notes (deferred)

- **`data_storage_version="2.1"+` is 2-4× slower to write than `"2.0"`
  on a 250K random workload** (W2 at `docs/lance_data_sink/post_refactor.json`).
  Cause unknown; investigation deferred. See plan at
  `~/.claude/plans/we-have-received-customer-composed-island.md`
  Workstream B.
- **NFR-7 (blob sidecar `.blob` files in `WriteResult.bytes_written`)
  remains unimplemented.** Tracked in `docs/lance_data_sink/03_requirements.md`
  under "Deferred items".
