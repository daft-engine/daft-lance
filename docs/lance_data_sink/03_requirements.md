# LanceDataSink Requirements

## Context

A customer reports that writing a Daft DataFrame to Lance with default
options produces "many small fragments" — roughly one fragment per Daft
micropartition instead of one fragment per ~1M rows as Lance documents.
The redesign of `daft_lance/lance_data_sink.py` must (a) fix the default
path, (b) make `data_storage_version` a first-class typed input with
proper inheritance on append, (c) drop the half-broken `mode="merge"`
path that today raises `UnboundLocalError` at finalize, and (d) clean up
accumulated complexity (nested closures, `Any` surfaces, string-sniffed
exception handling). See `../lance_data_sink/01_audit.md` (current state)
and `../lance_data_sink/02_benchmarks.md` (target numbers).

## 1. Functional requirements

1. **Supported write modes are exactly `create`, `append`, `overwrite`.**
   - Rationale: these are the modes that map directly to
     `lance.LanceOperation.Overwrite` and `lance.LanceOperation.Append`; today
     `"merge"` is on the public Literal but `finalize()` has no `merge` branch.
   - Reference: `daft_lance/lance_data_sink.py:131` (the Literal type),
     `:286-287` (validate_dataset error message),
     `:347-348` (merge-only `_filtered_schema` path),
     `:436-439` (finalize branches, no `merge` arm).

2. **`mode="merge"` is rejected at `__init__` with a clear, actionable error.**
   - Rationale: removing the broken path is a breaking change vs. today's
     latent `UnboundLocalError` at finalize. The error must point users at the
     real merge implementation (`daft_lance.merge_columns_df` /
     `daft_lance.merge_columns`).
   - Reference: error today is raised only at finalize via missing branch at
     `daft_lance/lance_data_sink.py:436-439`; redirect targets exist as
     `daft_lance/_lance.py:163` (`merge_columns`) and
     `daft_lance/_lance.py:253` (`merge_columns_df`), backed by
     `daft_lance/lance_merge_column.py:53` (`merge_columns_internal`) and
     `:350` (`merge_columns_from_df`).

3. **Schema validation on `append` accepts a castable superset of the existing
   table schema, with blob-aware normalization.** For each field present in
   the existing dataset, the input must contain a field of the same name
   whose Arrow type is castable to the table's type; `lance.blob.v2` and
   `large_binary` are treated as interchangeable for the validation check.
   - Rationale: the existing dataset is the source of truth on append; the
     input may carry extra columns (which will be projected out) but every
     existing column must be present and castable.
   - Reference: today's full equality-style check at
     `daft_lance/lance_data_sink.py:33-42` (`pyarrow_schema_castable`),
     blob normalization helper at `:86-102`
     (`_blob_aware_schema_for_validation`), and use site at `:299-306`. Also
     `:105-121` (`table_schema_subset_castable`) is a stricter superset-style
     check defined but currently unused — fold one of these into the new
     validator.

4. **All `lance.fragment.write_fragments` knobs are first-class typed kwargs
   on `LanceDataSink.__init__`.** No more `**kwargs` → `**self._kwargs`
   pass-through. The exposed parameters must match the upstream Lance
   signature exactly:
   - `max_rows_per_file: int = 1024 * 1024`
   - `max_rows_per_group: int = 1024`
   - `max_bytes_per_file: int = DEFAULT_MAX_BYTES_PER_FILE` (`90 * 1024**3`)
   - `data_storage_version: Literal["legacy", "0.1", "stable", "2.0", "2.1", "2.2", "2.3", "next"] | None = None`
   - `use_legacy_format: bool | None = None` (deprecated upstream; accept for
     parity, warn on use)
   - `enable_stable_row_ids: bool = False`
   - `storage_options: dict[str, str] | None = None`
   - Rationale: typed kwargs surface typos at construction (today,
     misspelling `max_bytes_per_file` as `max_file_bytes` would silently
     leak through `**self._kwargs` and either be ignored or rejected deep
     inside Lance).
   - Reference: upstream signature at
     `.venv/lib/python3.12/site-packages/lance/fragment.py:1036-1054`;
     the valid `data_storage_version` Literal set is verbatim from
     `.venv/lib/python3.12/site-packages/lance/dataset.py:6415-6417`. Today
     these parameters arrive via `**kwargs` at
     `daft_lance/lance_data_sink.py:133`, are stashed in `self._kwargs` at
     `:140`, and are splatted at `:367`.

5. **Blob V2 is opt-in via `blob_columns: list[str] | None`.** When supplied,
   the sink auto-promotes `data_storage_version` to `"2.2"` only if the user
   did not pass an explicit version. On `append`, the sink auto-detects
   existing `lance.blob.v2` columns from the dataset schema so callers do not
   need to repeat themselves.
   - Rationale: blob writes are an opt-in encoding; the existing dataset is
     authoritative on append.
   - Reference: today at `daft_lance/lance_data_sink.py:145-186` —
     `blob_columns` is popped from kwargs at `:147`, normalized at `:148`,
     storage version auto-applied at `:149-150` / `:213-220`, and existing
     blob columns auto-detected on append at `:179-186`.

6. **Atomic commit semantics: `write_fragments` per worker, single
   `LanceOperation.{Overwrite,Append}.commit` in `finalize()`.** Workers
   produce `FragmentMetadata`; the driver issues exactly one `commit` with
   the accumulated fragments and the `read_version` captured at sink
   construction.
   - Rationale: this is the contract the existing sink already provides and
     the only safe pattern for distributed writes.
   - Reference: `daft_lance/lance_data_sink.py:431-457` (finalize) and the
     upstream operation classes at
     `.venv/lib/python3.12/site-packages/lance/dataset.py:4827-4912`
     (`BaseOperation`, `Overwrite`, `Append`).

7. **`WriteResult` reports accurate `bytes_written` and `rows_written`.**
   `bytes_written` reflects on-disk Lance data files via
   `FragmentMetadata.files[*].file_size_bytes` when available. The redesign
   must additionally walk Blob V2 sidecar `.blob` files (which the current
   implementation explicitly skips) and account for them where the metadata
   surface allows it. `rows_written` is the number of rows passed to
   `write_fragments`. (**Deferred**: `.blob` sidecar accounting is not
   implemented in the current refactor pass; blob-internal cleanups stay
   out of scope. Tracked as a follow-up — see "Deferred items" below.)
   - Rationale: downstream telemetry (Daft engine accounting, customer
     dashboards) depends on these being meaningful, not the in-RAM
     `arrow_table.nbytes` fallback.
   - Reference: the current fallback logic at
     `daft_lance/lance_data_sink.py:372-380`; the note that
     `FragmentMetadata.files` does not currently track `.blob` sidecars is at
     `:369-371`.

8. **`finalize()` returns a single-row `MicroPartition` with fields
   `num_fragments`, `num_deleted_rows`, `num_small_files`, `version`,
   all `Int64`.** This is the schema declared by `LanceDataSink.schema()`.
   - Rationale: contract with `DataFrame.write_lance(...)` callers; the
     returned DataFrame is documented to expose these.
   - Reference: `daft_lance/lance_data_sink.py:449-456` (table
     construction), `:190-197` (declared schema).

9. **Every valid `data_storage_version` value reachable end-to-end.** The
   redesign must add coverage for each of
   `"legacy"`, `"0.1"`, `"stable"`, `"2.0"`, `"2.1"`, `"2.2"`, `"2.3"`,
   `"next"`. Today only `"2.2"` is exercised, via the blob V2 tests.
   - Rationale: typed plumbing only matters if it is actually exercised; a
     parametrized test per version is the cheapest way to lock the contract.
   - Reference: upstream Literal at
     `.venv/lib/python3.12/site-packages/lance/dataset.py:6415-6417`;
     today's only exercising test:
     `tests/io/lancedb/test_lance_blob_v2_write.py:67-73`
     (`test_blob_columns_default_storage_version`).

## 2. Non-functional requirements

1. **Default-path correctness: out-of-the-box writes must not produce one
   fragment per Daft micropartition.** With Daft's default morsel size
   (131,072 rows) and Lance's default `max_rows_per_file` (1,048,576), a
   10M-row write should produce roughly 10 fragments, not ~77. The target
   is fragments of `>= 256K rows OR >= 64 MiB on-disk, whichever comes
   first`. See `../lance_data_sink/02_benchmarks.md` for measured numbers.
   - Rationale: this is the customer complaint and the primary motivation
     for the redesign.
   - Reference: `daft_lance/lance_data_sink.py:317-319` (the inverted
     `accumulate` guard), `:412-414` (the early `yield` that emits one
     fragment per morsel when accumulation is off), `:428-429` (final flush
     emits a small fragment regardless of size).

2. **Bounded memory for large blob writes.** Promoting a binary column to a
   `lance.blob.v2` array must not require a `list[bytes]` Python-level
   round-trip; the storage struct is built directly from the Arrow array.
   The redesign preserves this; it is a regression risk if the helper is
   inlined or rewritten.
   - Rationale: customers write multi-GB blob columns; the
     `lance.blob.blob_array` convenience helper is not memory-bounded.
   - Reference: `daft_lance/lance_data_sink.py:53-83`
     (`_binary_to_blob_v2_array`).

3. **`WriteResult.result` must be picklable for distributed execution.**
   Daft's swordfish engine ships `WriteResult` across worker boundaries.
   `lance.fragment.FragmentMetadata` is picklable today; the redesign must
   not wrap it in anything that breaks pickling (e.g., closures over the
   sink, lambdas, non-dataclass containers).
   - Rationale: the Ray runner explicitly needs this; a regression here
     surfaces as opaque serialization errors at runtime.
   - Reference: result type alias at `daft_lance/lance_data_sink.py:25`,
     used as `WriteResult[list[LanceFragmentMetadata]]` at `:124`, `:315`.

4. **No string-sniffed exception detection.** Replace the
   `"not found" in str(e).lower()` check with type-based detection. Lance's
   Python API surfaces a "dataset not found" condition as `OSError` /
   `ValueError` from `lance.dataset()`; the redesign must catch the typed
   exception(s) and re-raise anything that is not specifically a missing
   dataset. Where no typed exception exists upstream, document the
   fallback explicitly with a code comment and a regression test.
   - Rationale: substring matching is brittle across Lance versions; a
     localization change or message tweak silently re-classifies errors.
   - Reference: `daft_lance/lance_data_sink.py:270-281` (the
     `_load_existing_dataset` method, including the `"not found" in str(e)`
     check at `:275`).

5. **No `Any` on the public surface where a Lance type exists.**
   - `LanceFragmentMetadata = Any` at `daft_lance/lance_data_sink.py:25`
     becomes `lance.fragment.FragmentMetadata` (importable from
     `.venv/lib/python3.12/site-packages/lance/__init__.py:28`).
   - `_load_existing_dataset(self) -> Any | None` at `:270` becomes
     `-> lance.LanceDataset | None`.
   - `_validate_dataset(self, table: Any) -> None` at `:283` becomes
     `table: lance.LanceDataset | None`.
   - Rationale: removes the `# type: ignore` foot-gun at the call sites,
     and lets `mypy` catch any future drift.
   - Reference: the three sites listed above.

6. **Testable units.** The four nested constructs inside `write()` —
   `_cast_target_schema` (`:323`), `_prepare_arrow_table` (`:346`),
   `_write_arrow_table` (`:360`), and the nested class `_TableAccumulator`
   (`:382`) — must be lifted to instance methods or module-level helpers
   so they can be tested in isolation without driving the whole `write()`
   loop.
   - Rationale: today the only way to test these is via end-to-end writes;
     the unit-level edge cases (empty input, blob-aware schema cast, byte
     accounting) are correspondingly hard to cover.
   - Reference: the line citations above point to the closures inside
     `write()`.

## 3. Public API contract

The redesigned `LanceDataSink.__init__` signature:

```python
class LanceDataSink(DataSink[list[lance.fragment.FragmentMetadata]]):
    def __init__(
        self,
        uri: str | pathlib.Path,
        schema: Schema | pa.Schema,
        mode: Literal["create", "append", "overwrite"] = "create",
        io_config: IOConfig | None = None,
        *,
        blob_columns: list[str] | None = None,
        max_rows_per_file: int = 1024 * 1024,
        max_rows_per_group: int = 1024,
        max_bytes_per_file: int = 90 * 1024 * 1024 * 1024,
        data_storage_version: (
            Literal["legacy", "0.1", "stable", "2.0", "2.1", "2.2", "2.3", "next"]
            | None
        ) = None,
        use_legacy_format: bool | None = None,
        enable_stable_row_ids: bool = False,
        storage_options: dict[str, str] | None = None,
    ) -> None: ...
```

Defaults are taken verbatim from
`.venv/lib/python3.12/site-packages/lance/fragment.py:1036-1054`. The
keyword-only marker (`*,`) after `io_config` enforces named arguments for
all Lance knobs.

The Daft DataFrame entrypoint
(`daft.DataFrame.write_lance(...)`, at
`.venv/lib/python3.12/site-packages/daft/dataframe/dataframe.py:1941-2042`)
constructs the sink via `LanceDataSink(uri, schema, mode, io_config,
**sanitized_kwargs)`. Its public signature does not change: callers continue
to pass blob/Lance kwargs through positional + `**kwargs`, and the sink
binds them to the typed parameters above.

Behavior when `mode="merge"` is passed:

- Raise `ValueError` (not `UnboundLocalError`) at `__init__` with the
  message:

      mode="merge" is no longer supported by LanceDataSink. Use
      daft_lance.merge_columns_df(df, uri, ...) for row-level merges keyed by
      _rowaddr/fragment_id, or daft_lance.merge_columns(uri, transform=...)
      for column merges driven by a UDF.

  Both redirect targets exist: `daft_lance/_lance.py:163` (`merge_columns`)
  and `daft_lance/_lance.py:253` (`merge_columns_df`).

### Backwards-compat test contracts

The following test files must keep passing without modification (except as
called out below):

- `tests/io/lancedb/test_lance_batching.py` — exercises the accumulator
  contract by patching `daft_lance.lance_data_sink.lance` and asserting
  `len(fake.calls)` for various morsel patterns. **Likely intentional
  break:** `test_no_accumulation_when_param_missing` (`:95-106`) asserts
  that omitting `max_rows_per_file` produces one write per micropartition.
  Under the new default — `max_rows_per_file=1_048_576` always — both
  micropartitions accumulate into one write of 15 rows. This is the
  customer-visible fix and the assertion must be updated. The other three
  tests in this file (which explicitly pass `max_rows_per_file=25`) keep
  the same expected call counts.

- `tests/io/lancedb/test_lance_blob_v2_write.py` — exercises blob V2 opt-in,
  default storage version promotion, unknown/non-binary column rejection,
  large_binary acceptance, and append auto-detection. All paths exercise
  the typed `blob_columns` parameter; the redesign keeps all assertions
  satisfied. No expected breaks.

- `tests/io/lancedb/test_lance_blob_v2_e2e.py` — end-to-end round-trip
  including blob descriptor read → derive → write. No expected breaks.

- `tests/io/lancedb/test_lancedb_writes.py` — primary write tests
  (round-trip, schema with metadata, blob via metadata encoding, schema
  mismatch, append/create error paths). **Likely intentional break:** none
  expected for the mainline tests, but the merge-evolution path covered in
  `tests/io/lancedb/test_lance_merge_evolution.py` (which today calls
  `df.write_lance(..., mode="merge")` at `:50`, `:101`) will need to be
  updated to call `daft_lance.merge_columns_df(...)` directly, since the
  sink will refuse `mode="merge"` at construction. Document this as part
  of the migration story in the design discussion.

## 4. Edge cases the redesign must handle

- **Empty input.** Zero micropartitions, or all-empty micropartitions, must
  still produce a valid (empty) commit. Lance's `write_fragments` accepts
  zero rows; the redesign must skip the `write_fragments` call when there
  is nothing to write and produce an empty fragment list at `finalize()`.
  For `mode="create"`, this still creates the dataset with the declared
  schema and zero fragments.

- **Single micropartition that exceeds `max_bytes_per_file`.** Lance's
  `write_fragments` will split the input into multiple fragments
  internally; the sink must propagate every `FragmentMetadata` it gets
  back, not just the first.

- **Append with schema drift.**
  - Extra columns in input (not present in the existing table): allowed.
    The sink projects them out by casting to the existing table schema.
  - Missing columns in input (present in table, absent from input): the
    redesign rejects at sink construction with a clear error
    (`"missing required column 'foo' in input"`). Today's behavior is to
    error during the cast inside `_prepare_arrow_table` with a less
    actionable message.
  - Type widening (e.g., `int32` → `int64`): allowed if Arrow's cast
    accepts it (which `pyarrow_schema_castable` already approximates by
    cast-testing an empty array).
  - Type narrowing that may lose data (e.g., `int64` → `int32`): allowed
    if Arrow accepts the cast, with a documented caveat that values
    outside the narrower range will raise at cast time.

- **Append into a dataset with existing blob columns when caller did not
  pass `blob_columns`.** Today: auto-detected from the existing dataset
  schema at `daft_lance/lance_data_sink.py:179-186`. The redesign
  preserves this. The append path also auto-applies
  `data_storage_version="2.2"` if the caller did not specify one.

- **Storage version mismatch on append.** Caller passes
  `data_storage_version="2.2"` but existing dataset is `"2.1"`. Recommend:
  raise `ValueError` at `__init__` with a message pointing at
  `daft_lance.compact_files(...)` or a future migration helper. Do not
  silently upgrade — version changes are dataset-wide and should be an
  explicit user action.

- **Concurrent writes.** `LanceDataset.commit` uses `read_version`, so
  concurrent writes can race and raise `lance.commit.CommitConflictError`
  (`.venv/lib/python3.12/site-packages/lance/commit.py:10`). Recommend:
  surface the error to the caller; do not retry silently. Document the
  retry pattern in the public docstring of
  `DataFrame.write_lance(...)` instead. Higher-level retry logic belongs
  to the caller or a separate compaction job.

## 5. Out of scope for this redesign

- Index creation, including scalar and vector indices — handled by
  `daft_lance.create_scalar_index` and the underlying
  `daft_lance/lance_scalar_index.py`.
- Compaction of small fragments after the fact — handled by
  `daft_lance.compact_files` and `daft_lance/lance_compaction.py`. The
  redesign aims to *avoid producing* small fragments in the first place;
  remediation of existing small fragments stays a separate operation.
- Deletes / updates (no current sink support).
- Merge / column merge — handled by `daft_lance.merge_columns_df` and
  `daft_lance.merge_columns`, backed by `daft_lance/lance_merge_column.py`.
- REST-namespace / `lance-namespace` integration — the `rest://` URI
  scheme was removed earlier (see `daft_lance/_lance.py:127-133` in
  `read_lance`); the write sink follows suit and does not attempt to
  re-introduce it.

## 6. Open questions for the design discussion

- **Default `max_rows_per_file`.** Keep Lance's `1_048_576` (matches
  upstream, lets the customer's "1M rows per fragment" mental model just
  work)? Or pick something else (e.g., `2_097_152`) tuned to the
  benchmark numbers in `../lance_data_sink/02_benchmarks.md`?

- **Default `data_storage_version`.** Stay `None` (Lance interprets as
  `"2.0"`), or explicitly default to `"stable"` for forward-compat? The
  Lance docstring at
  `.venv/lib/python3.12/site-packages/lance/dataset.py:6473-6476` says
  the default is `None` which uses "the latest stable version" — but the
  same docstring on `write_fragments`
  (`.venv/lib/python3.12/site-packages/lance/fragment.py:1093-1096`)
  says `None` maps to `"2.0"`. Pin one explicitly so we are not relying
  on the upstream default drifting.

- **Implementation of the "fragments ≥ 256K rows OR ≥ 64 MiB" target.**
  Two viable approaches: (a) keep the in-sink accumulator but extend it
  to track bytes and flush on whichever threshold hits first; or (b)
  drop the accumulator and rely on Lance's own `max_rows_per_file` /
  `max_bytes_per_file` enforcement by always passing the full input
  through `write_fragments` in one call per worker. Option (b) is
  simpler but requires understanding how the swordfish engine batches
  morsels into a single `write()` invocation.

- **`use_legacy_format`.** The upstream parameter is deprecated; do we
  re-expose it for parity (as currently planned), drop it entirely, or
  re-expose it with a `DeprecationWarning` at construction?

- **Sink-side `bytes_written` accounting for Blob V2 sidecars.** Lance's
  `FragmentMetadata.files` does not track `.blob` sidecar files. Options:
  (a) accept the undercount; (b) stat the blob directory after commit and
  add it ourselves; (c) push an upstream change. Option (b) is feasible
  today.

## Deferred items

- **NFR-7 (`.blob` sidecar bytes in `WriteResult.bytes_written`)** —
  deferred pending a future blob-V2-focused pass. `FragmentMetadata.files`
  does not expose sidecar paths and walking the dataset blob directory
  adds I/O cost to every write. The current implementation sums only
  `.lance` data files and notes this in `_write_arrow_table`. The original
  FR7 requirement above remains the long-term target.
