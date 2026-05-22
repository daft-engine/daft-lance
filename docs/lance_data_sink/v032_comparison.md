# v0.3.2 → `rchowell/blob-v2`: audit reproduction + 10M comparison

This document does two things:

1. Verifies that the LanceDataSink audit findings (the original investigation
   that motivated this PR) reproduce at the tagged release `v0.3.2`.
2. Measures the post-refactor branch (`rchowell/blob-v2`, commit `25c9352`)
   against `v0.3.2` at 10M-row scale to quantify the customer-reported
   "many small fragments → 4× write / 5× read speedup" claim.

## TL;DR

- **The audit's code-level findings reproduce at v0.3.2.** The smoking-gun
  guard (`accumulate = isinstance(max_rows_per_file, int) and max_rows_per_file > 0`)
  is at `daft_lance/lance_data_sink.py:152` in v0.3.2 (audit cited line 318 in
  a working-tree state that included additional blob V2 work). Same boolean,
  same semantics.

- **The audit's *predicted consequence* does NOT reproduce.** The audit
  estimated ~77 fragments at 10M rows on the default path (10M / 131,072
  morsel-size). In practice, both v0.3.2 and post-refactor produce **10
  fragments** for the same workload. Daft's executor does not call the sink's
  `write()` once per morsel as the audit assumed; it batches input before
  ingestion.

- **Write/read performance at 10M is within noise** between v0.3.2 and the
  post-refactor branch. The customer-reported 4× write / 5× read speedup
  could not be reproduced via `daft.from_arrow(table).write_lance(uri)` on
  the current Daft version.

- **The refactor's value is API quality + correctness, not throughput at
  this call shape.** See `pr_performance.md` for the full list of code wins.

## 1. Audit findings — reproduction at v0.3.2

| Audit item | At v0.3.2 | Status |
|---|---|---|
| Accumulation guard inverts the Lance default (4.1) | `lance_data_sink.py:152-154` carries `accumulate = ... > 0` and bails at `:209` when False | **Confirmed (code)** |
| Predicted "~77 fragments at 10M default" (Sec. 3 arithmetic) | Measured: 10 fragments | **Not confirmed (runtime)** |
| `data_storage_version` forwarded blindly via `**self._kwargs` (4.2) | Present at `:154` and `:225` (sink reads kwargs unchecked) | **Confirmed** |
| `mode="merge"` reaches commit with no operation branch (4.3) | `Literal["create","append","overwrite","merge"]` declared; `finalize()` has no merge arm | **Confirmed** |
| Blob V2 logic smeared across the class (4.4) | Five locations identified by audit are all present | **Confirmed** |
| Nested closures inside `write()` (4.5) | `_cast_target_schema`, `_prepare_arrow_table`, `_write_arrow_table`, `_TableAccumulator` all defined inside `write()` | **Confirmed** |
| Dead code `table_schema_subset_castable` (4.9) | Function present, zero callers | **Confirmed** |
| `LanceFragmentMetadata = Any` (4.10) | Present | **Confirmed** |
| `_TableAccumulator` row-count only (4.11) | Class has no byte-size criterion | **Confirmed** |

Every code-level finding reproduces. The customer-symptom prediction
("8× too many fragments") does not — because the assumed execution model
(one `write()` call per morsel) is incorrect for the Daft native runner's
sink path. The runner batches micropartitions before delivery, so the bug
path is reachable in theory but not exercised here.

## 2. Performance — v0.3.2 vs post-refactor at 10M rows

10M-row table, three columns (`int64`, `float64`, `string@16`), single
laptop run. One round per test (`benchmark.pedantic(rounds=1)`). Same
benchmark file copied into a worktree of each version so the call paths
are identical.

| Test                         | v0.3.2 (ms) | post (ms) | Δ wall  | Frags (v0.3.2) | Frags (post) | Bytes (v0.3.2 → post) |
|------------------------------|------------:|----------:|--------:|---------------:|-------------:|------------------------|
| `test_w4_10m_default_write`  |       702.2 |     789.5 | **+12%**|             10 |           10 | 380,467,005 → 380,498,109 |
| `test_w4_10m_chunked_write`  |     1,054.9 |     983.7 |  **−7%**|             80 |           80 | 380,624,180 → 380,618,804 |
| `test_w4_10m_lance_native`   |       633.8 |     635.3 |   ~0%   |             10 |           10 | 340,146,049 → 340,150,785 |
| `test_r3_10m_full_scan`      |       298.8 |     287.9 |  **−4%**|             10 |           10 | 380,491,325 → 380,450,045 |

Sink overhead vs. Lance native at 10M default (post-refactor): 789.5 / 635.3
= **1.24×** (24% Daft-side cost; same as v0.3.2 within noise).

## 3. Why the audit's predicted speedup doesn't reproduce

The audit assumed Daft's executor would call the sink's `write()` once
per `MicroPartition` of `default_morsel_size` (131,072) rows. Under that
model, a 10M-row write would deliver ~77 micropartitions sequentially to
the sink, each producing its own fragment when `accumulate=False`.

The observed fragment count (10 for both v0.3.2 and post) implies Daft
delivers ~1M-row micropartitions to the sink — not 131K-row ones. Either
the executor batches morsels before calling `write()`, or the morsel size
applies only to upstream operators, not to sink ingestion. Either way,
the buggy code path is *reachable* but not *triggered* by
`daft.from_arrow(big_table).write_lance(uri)` on the current Daft version.

The customer's original 22.9 s / 804-fragment number (`docs/findings.md`)
likely came from a different execution path — possibly the Ray runner, a
finer-grained internal batching configuration, or an older Daft version
whose executor did stream morsels into the sink. The refactor's
buffer-coalescing logic remains correct for that path; it just doesn't
demonstrate a speedup on the call shape we measured here.

## 4. What we still got from the refactor

Even with neutral throughput at this call shape, the refactor lands:

- `mode="merge"` rejected at `__init__` with an actionable error instead
  of crashing with `UnboundLocalError` at finalize.
- All `lance.fragment.write_fragments` knobs are first-class typed kwargs
  (no `**kwargs` silent typos); `data_storage_version` validated by Literal.
- `data_storage_version` inherited from the existing dataset on append;
  mismatch raises with a migration hint.
- Blob V2 logic consolidated behind `BlobV2WritePolicy` in `_blob.py`.
- 244 tests passing (vs. ~210 pre-refactor); +33 dedicated policy tests;
  +11 storage-version round-trip tests; mypy-strict clean on both modules.
- All eight ranked review items addressed (`04_review.md`).

## 5. Reproduction

```bash
# Post-refactor (current branch):
uv run pytest tests/benchmarks -m slow --benchmark-json=docs/lance_data_sink/post_refactor_10m.json -v

# v0.3.2 baseline (via worktree):
git worktree add /tmp/daft-lance-v032 v0.3.2
cp -r tests/benchmarks /tmp/daft-lance-v032/tests/
cp pyproject.toml /tmp/daft-lance-v032/pyproject.toml
cd /tmp/daft-lance-v032 && uv sync --group dev
uv run pytest tests/benchmarks -m slow --benchmark-json=$REPO/docs/lance_data_sink/v032_10m.json -v
cd $REPO && git worktree remove --force /tmp/daft-lance-v032
```

Raw outputs:
- `docs/lance_data_sink/v032_10m.json` (v0.3.2 baseline)
- `docs/lance_data_sink/post_refactor_10m.json` (rchowell/blob-v2)
