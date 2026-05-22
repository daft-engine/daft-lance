# LanceDataSink Refactor — Critical Review

`uv run pytest tests/io/lancedb -x -q` → **210 passed, 5 skipped, 2 xfailed, 2 xpassed**.
`uv run --with mypy mypy daft_lance/lance_data_sink.py daft_lance/_blob.py` → **0 errors in the sink, 14 errors in `_blob.py`** (all generic-param / `metadata=` arg-type — see Issue 2).

## TL;DR

- The customer-visible fix landed and is correct: `_LanceFragmentBuffer` always accumulates, gates on rows **and** bytes, and `write()` reads cleanly. Audit's #1 priority resolved.
- `_blob.py` is the highlight — `BlobV2WritePolicy` is a real boundary, and `binary_to_blob_v2_array` preserves NFR-2's memory-bounded path.
- `__init__` (`lance_data_sink.py:35-142`) is still doing too much. Storage-version inheritance/mismatch is open-coded at lines 110-126, and `_validate_dataset` mutates state — it's a misnamed `_absorb_existing_dataset`.
- **No regression test for the customer fix at realistic scale.** `test_accumulation_default_batches_across_micropartitions` covers 30 rows. The benchmark (`tests/benchmarks/test_lance_data_sink_bench.py:155-156`) still asserts the OLD buggy behavior (`n_fragments >= 5`).
- `BlobV2WritePolicy` has zero direct unit tests. Same testability complaint Phase 3 solved, repeated one level down.
- NFR-7 (`.blob` sidecar bytes in `bytes_written`) is **not implemented**; lines 212-216 explicitly punt on it.

## Strengths

- **`BlobV2WritePolicy` (`_blob.py:91-212`)** is a clean five-method boundary, each with a `not self.enabled` fast-path. The right pattern for "policy applies to a subset of writes."
- **`_LanceFragmentBuffer` (`lance_data_sink.py:271-302`)** has the right API: `add()` returns a bool, `drain()` resets, threshold is `rows >= max_rows OR bytes >= max_bytes`. Matches NFR-1 exactly.
- **`write()` (`lance_data_sink.py:219-241`)** is 23 lines for the loop body vs. ~110 of nested closures in `HEAD:daft_lance/lance_data_sink.py:152-219`. Oversized-input fast path flushes the buffer first — ordering preserved.
- **`mode="merge"` rejection (`:52-58`)** points at both `merge_columns_df` and `merge_columns`. `test_lance_merge_evolution.py` migrated cleanly.
- **Typed `LanceStorageVersion` (`_blob.py:19`)** is verbatim from Lance; `LANCE_BLOB_V2_STORAGE_VERSION: LanceStorageVersion = "2.2"` is a typed constant.
- **`binary_to_blob_v2_array` (`_blob.py:39-67`)** still builds the storage struct directly from Arrow buffers; no `list[bytes]` round-trip.

## Issues to fix before commit

### 1. `__init__` mixes init, validation, mutation, and Lance gymnastics in 100 lines

**Where**: `lance_data_sink.py:35-142`.
**Why it matters**: Reading `__init__` requires holding the kwargs contract, the policy lifecycle, the storage-version rules, and the load/validate flow simultaneously. Lines 83-131 are six conceptually distinct steps with no separators.
**Suggested fix**: Extract helpers — `_reject_unsupported_modes`, `_init_basic_state`, `_absorb_existing_dataset`, `_resolve_data_storage_version`. The constructor becomes ~15 lines of orchestration.

### 2. mypy is dirty in `_blob.py` (14 errors, all annotation-level)

**Where**: `daft_lance/_blob.py:26, 39, 51, 65, 79, 88, 175, 181-182, 192, 212`.
**Why it matters**: The sink is mypy-clean; leaving `_blob.py` at 14 errors leaves the net half-broken at the module boundary. `binary_length(arr)` at `:51` is a real overload mismatch (mypy can't see the cast to `large_binary`); the `metadata=` errors are one-character fixes.
**Suggested fix**: Annotate `combine_chunks()` result, parametrize `pa.Field` / `pa.Array` / `pa.ChunkedArray`, widen the `metadata` dict literal type to `dict[bytes | str, bytes | str]`. No runtime changes.

### 3. `_load_existing_dataset` + `_validate_dataset` are entangled and have a dead branch

**Where**: `lance_data_sink.py:144-180`.
**Why it matters**: The append-missing check is duplicated (line 151 raises before line 161 can ever be hit on append). `_validate_dataset` mutates `self._table_schema` and `self._version` (lines 167-168) — that is not validation. Names lie.
**Suggested fix**: Collapse into one `_absorb_existing_dataset(self) -> lance.LanceDataset | None` that does the open, raises on append-missing, sets `_table_schema` / `_version`, runs castability check, and returns the dataset. Delete the dead branch at line 161.

### 4. Storage-version inheritance + mismatch belongs in a dedicated helper

**Where**: `lance_data_sink.py:110-126` — two back-to-back `if self._mode == "append"` blocks, each doing `getattr(table, "data_storage_version", None)`.
**Why it matters**: 17 lines of business logic inlined in `__init__`. Same predicate (`self._mode == "append" and self._table_schema is not None`) repeated. The `getattr` double-read is a smell — the same dataset is queried twice for the same attribute.
**Suggested fix**: Extract:

```python
def resolve_storage_version(
    requested: LanceStorageVersion | None,
    existing: LanceStorageVersion | None,
    mode: Literal["create", "append", "overwrite"],
) -> LanceStorageVersion | None: ...
```

…with mismatch detection. Unit-testable in isolation. Lives in `_blob.py` or a new `_storage_version.py`.

### 5. `BlobV2WritePolicy` has zero direct unit tests

**Where**: `_blob.py:91-212`; `grep BlobV2WritePolicy tests/` → no hits.
**Why it matters**: Phase 3 lifted closures into methods so they could be unit-tested. Phase 5 then put policy logic in a new class — but didn't test it. Every method is exercised end-to-end through `LanceDataSink.write()`. The edge cases (empty schema, already-blob columns in `wrap_table`, `add_columns` dedup, `validate_columns_present` rejecting non-binary) have no direct coverage.
**Suggested fix**: Add `tests/io/lancedb/test_blob_v2_policy.py` with one test per public method. Also add direct tests for `detect_blob_v2_columns`, `is_blob_v2_field`, `blob_aware_schema_for_validation` (currently zero hits) and `binary_to_blob_v2_array`.

### 6. No regression test for the customer fix at realistic scale

**Where**: `tests/io/lancedb/test_lance_batching.py:95-110` (30 rows total); `tests/benchmarks/test_lance_data_sink_bench.py:155-156` (still asserts old behavior).
**Why it matters**: The bench has `assert info["n_fragments"] >= 5` for the default-path 250K-row write — the **failure** condition, not the fix. Shipping the fix without a locking test is how the bug returns the next time someone touches `_LanceFragmentBuffer`.
**Suggested fix**: Update the benchmark assertion to the new expected count (1 fragment per ~1M rows, or whatever the post-fix number is), OR add `test_default_path_coalesces_multi_partition_input` that drives `daft.from_arrow(...).into_partitions(10).write_lance(...)` on ≥ 1M rows and asserts the resulting fragment count is `<= ceil(rows / 1_048_576) + 1`.

### 7. `pyarrow_schema_castable` is a module-public orphan in the sink

**Where**: `lance_data_sink.py:306-314`.
**Why it matters**: Called from exactly one place (line 173). No leading underscore — `from daft_lance.lance_data_sink import pyarrow_schema_castable` works. Sits at the bottom of the file, far from its caller.
**Suggested fix**: Rename to `_pyarrow_schema_castable` and move into `_blob.py` (its only caller is the blob-aware validation path). Or `_schema.py` if you prefer separation.

### 8. `required_storage_version` reads as a requirement check, but applies a default

**Where**: `_blob.py:138-148`; call at `lance_data_sink.py:86`.
**Why it matters**: `self._data_storage_version = self._blob.required_storage_version(self._data_storage_version)` makes it look like the policy is enforcing a requirement, but it's defaulting to 2.2 when `enabled and current is None`. Mental model breaks.
**Suggested fix**: Rename to `apply_blob_v2_default(current)` or `default_storage_version(current)`. The first is more honest.

## Test coverage gaps

| Requirement / Component | Status | Notes |
|---|---|---|
| FR1: modes = {create, append, overwrite} | covered | `test_mode_merge_rejected_at_construction` |
| FR3: castable append validation (blob-aware) | partial | int32→int64 covered; no test for the blob-aware demotion specifically |
| FR4: typed kwargs | covered by construction | no negative test for "misspelled kwarg" |
| FR5: blob auto-detect on append | covered | `test_blob_columns_append` |
| FR5: auto-2.2 when blob present | covered | `test_blob_columns_default_storage_version` |
| FR7: `bytes_written` for `.blob` sidecars | **missing** | NFR-7 says "must walk `.blob` files"; code at `:212-216` skips them |
| FR9: every `data_storage_version` value | covered | parametrized `test_round_trip_each_storage_version` |
| NFR1: default path produces large fragments | **partial** | unit-mocked at 30 rows; bench asserts old broken value — Issue 6 |
| NFR4: no string-sniffed errors | partial | sink still uses `"was not found" in str(e)` at `:149`; `test_lance_message_format_unchanged` pins the format |
| NFR6: testable units | partial | buffer + `_prepare_arrow_table` tested; `BlobV2WritePolicy` not — Issue 5 |
| Edge: empty input | **missing** | zero-row / empty-iterator write |
| Edge: single MP > `max_bytes_per_file` | **missing** | byte-fast-path at `:231` not directly tested |
| Edge: storage version mismatch on append | covered | `test_storage_version_mismatch_on_append_rejected` |
| Edge: inherit version on non-blob append | covered | `test_append_inherits_storage_version` |
| `BlobV2WritePolicy` direct tests | **missing** | Issue 5 |
| `detect_blob_v2_columns` direct test | **missing** | only exercised end-to-end |
| `blob_aware_schema_for_validation` direct test | **missing** | only exercised end-to-end |
| `use_legacy_format` deprecation | covered | `test_use_legacy_format_emits_deprecation_warning` |

Highest-impact gap: **NFR1 at realistic scale** (Issue 6). Second: **`BlobV2WritePolicy` has no direct tests** (Issue 5).

## Nice-to-haves (not blocking)

- `LanceStorageVersion` could move to `_types.py` (it isn't blob-specific).
- `_LanceFragmentBuffer.has_rows()` is read three times in `write()`; a `try_drain() -> pa.Table | None` would halve call sites. Cosmetic.
- `_prepare_arrow_table` / `_write_arrow_table` → `_prepare_table` / `_write_table` (arrow is the only flavor).
- `BlobV2WritePolicy` is a configuration object, not a Strategy-pattern policy. `BlobV2Columns` or `BlobV2Schema` is more honest. Don't churn if other things land first.
- Diligence docs (`01_audit.md`, `02_benchmarks.md`) reference line numbers that no longer exist. Add a "see implementation in `lance_data_sink.py`" footer or mark them historical baseline.
- `LanceDataSink.__init__` has no docstring. Document the relationship between `max_rows_per_file`, `max_bytes_per_file`, and the inheritance behavior of `data_storage_version` on append.
- Two trailing blank lines between `_LanceFragmentBuffer.drain()` and `pyarrow_schema_castable` (`:303-305`) — minor formatting.

## Verdict

The core refactor is the right shape and executed cleanly in `_LanceFragmentBuffer`, `write()`, and `BlobV2WritePolicy`. The constructor and the test layer are not done. The 100-line `__init__` is now the biggest source of complexity in the file; `_load_existing_dataset` / `_validate_dataset` is a state-mutation hazard behind a naming mistake; and the benchmark suite still asserts the broken behavior the refactor was meant to fix — which is worse than no benchmark.

Apply Issues 1, 3, 5, 6 before committing. Issues 2, 4, 7, 8 can land as a follow-up to keep this PR scoped — but Issue 6 (the missing regression test at scale) is non-negotiable. Once those land, the refactor is ready.
