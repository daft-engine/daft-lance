# daft-lance

Lance integration for [Daft](https://github.com/Eventual-Inc/Daft).

## Install

```
# Install just the daft-lance extension
pip install daft-lance

# Install daft with the daft-lance extension
pip install 'daft[lance]'
```

## Usage

### Compaction

```python
from daft_lance import compact_files

compact_files("s3://bucket/my_dataset")
```

### Scalar Indexing

Every supported index type is built distributed: Daft workers build
independent index segments and the coordinator commits them atomically with
complete metadata. Supported types: `BITMAP`, `BTREE`, `INVERTED`, `FTS`,
`ZONEMAP`, `NGRAM`, `LABEL_LIST`, `BLOOMFILTER`.

```python
from daft_lance import create_scalar_index

create_scalar_index("s3://bucket/my_dataset", column="name", index_type="INVERTED")
create_scalar_index("s3://bucket/my_dataset", column="ts", index_type="ZONEMAP")
```

Types without a distributed path (e.g. `RTREE`) raise `ValueError` — call
pylance directly (`lance.dataset(uri).create_scalar_index(...)`) for
single-node indexing.

`replace=True` (the default) atomically swaps an existing index of the same
name: the segment commit retires the old overlapped segments in the same
transaction as the new ones (the index type may change on a full rebuild;
the column may not — drop the index or use a different name for that).
`replace=False` rejects an existing name.

#### Partial Builds and Incremental Backfill

Pass `fragment_ids` to index only a subset of fragments today and backfill
the rest later. Fragments already covered by the existing same-name index
are skipped, and committed segments for other fragments are preserved
untouched:

```python
# Index the first two fragments now.
create_scalar_index("s3://bucket/my_dataset", column="name", index_type="INVERTED", fragment_ids=[0, 1])

# After appending data, backfill only the new fragments.
create_scalar_index("s3://bucket/my_dataset", column="name", index_type="INVERTED", fragment_ids=[2, 3])
```

Unknown fragment IDs and an empty list raise `ValueError`; duplicate IDs are
ignored. Indexing an empty dataset raises `ValueError` (no silent no-op), and
generated index names follow pylance's convention (`<column>_idx`); before
committing, the driver validates that the built segments cover every
scheduled fragment exactly once and reference no fragments that a concurrent
compaction removed. A same-name index keeps its column and, on backfill, its
index type — mismatches are rejected by Lance's build/commit APIs. Backfill appends and never replaces existing segments, so
`replace=False` does not apply to it; a fully covered request is a no-op
that leaves the dataset version unchanged (rebuild with `replace=True` and
no `fragment_ids` instead).


> **Breaking change (from 0.5.0):** the `segmented` parameter was removed —
> the distributed segment-index workflow is now the only code path, and
> the single-node fallbacks are gone. `replace` now defaults to `True` (matching
> pylance). Indexes created by older versions of
> `create_scalar_index` on `INVERTED` columns may carry empty index metadata
> (see #69); rebuilding them with `replace=True` records full metadata.

#### Index Maintenance

Appended data is not indexed automatically — queries stay correct (uncovered
fragments fall back to scans) but slow down as the unindexed share grows.
`optimize_indices` restores index health on the dataset's latest version:
it indexes newly appended fragments, merges small segments, and heals stale
fragment IDs left inside mixed segments by deletes as part of a commit that
indexes or merges new data. It commits no new version when there is no new
data to index and no segments to merge.

```python
from daft_lance import optimize_indices

updated = optimize_indices("s3://bucket/my_dataset")
updated = optimize_indices("s3://bucket/my_dataset", indices=["name_idx"], num_indices_to_merge=4)
```

`optimize_indices` delegates to pylance's
`DatasetOptimizer.optimize_indices`, runs in the coordinator process, and
returns the updated dataset; for a distributed rebuild use
`create_scalar_index(..., replace=True)`. Unknown or empty `indices` raise
`ValueError`.


### Vector Indexing

Vector indexes (`IVF_FLAT`, `IVF_PQ`, `IVF_SQ`, `IVF_HNSW_FLAT`,
`IVF_HNSW_PQ`, `IVF_HNSW_SQ`) are built distributed in three phases: the
driver trains the global model once — IVF centroids (and the PQ codebook for
PQ variants) via pylance's `IndicesBuilder`, unless pre-trained
`ivf_centroids`/`pq_codebook` are supplied — then Daft workers each build one
uncommitted index segment for their fragment batch with the shared model, and
the coordinator commits all segments atomically. Segments built against the
same centroids and codebook commit as one logical index.

```python
from daft_lance import create_vector_index

create_vector_index("s3://bucket/my_dataset", column="vector", index_type="IVF_PQ", num_partitions=16)
create_vector_index("s3://bucket/my_dataset", column="embedding", index_type="IVF_FLAT", metric="cosine")
```

`replace` defaults to `False`, matching pylance's `create_index` — which,
unlike its scalar counterpart `create_scalar_index` (default `True` above),
defaults `replace` to `False`. An existing index name is refused unless
`replace=True`, which rebuilds atomically (one manifest transaction retires
the old segments and lands the new ones).

`fragment_ids` partial builds and incremental backfill work like the scalar
workflow, with one vector-specific requirement: appending segments requires
the same `ivf_centroids` (and `pq_codebook` for PQ variants) the existing
segments were built with, since every segment of a logical vector index must
share one IVF model — a backfill without the original model raises instead of
silently training a divergent one.
Backfill must also use the existing index's `metric`; a different metric is
rejected because distances from different metrics cannot be ranked together.
If another task changes the same index during a new build or backfill, the
operation fails rather than replacing that task's result; retry against the
latest version. Full rebuilds with `replace=True` retain replacement semantics.

HNSW builds accept `m`, `max_level`, and `ef_construction`. Each worker generates
its own segment UUID, so `index_uuid` cannot be supplied. For pre-trained IVF
models, pass `ivf_centroids`; `ivf_centroids_file` is not supported by this API.

The SQ variants (`IVF_SQ`, `IVF_HNSW_SQ`) support multi-segment builds and
`fragment_ids` backfill. Each segment uses its own SQ quantization metadata
for queries. Independently built segments may have different quantization
metadata even when they share IVF centroids; Lance rejects attempts to merge
incompatible segments through `optimize_indices` with "vector index segments
do not share quantizer metadata". Use `fragment_group_size` large enough to
cover all selected fragments when a single segment is preferred. To consolidate
an existing index, rebuild with that grouping, `replace=True`, and no
`fragment_ids`.

Training samples `sample_rate` rows per IVF partition (and per PQ centroid)
and runs in the coordinator process, so its memory footprint grows with
`num_partitions * sample_rate * dimension`; `sample_rate` is clamped down
automatically (with a warning) to what the dataset size supports
(`num_partitions * sample_rate` rows, `256 * sample_rate` for the 8-bit PQ
codebook).


### Column Merging

```python
from daft_lance import merge_columns_df

merge_columns_df(df, "s3://bucket/my_dataset")
```

### Conditional Overwrite

Replace just the rows matching a predicate. One Lance commit deletes them from the existing
table and adds the new data, so readers see either the whole replacement or none of it.

```python
import daft_lance

daft_lance.write_lance(
    df,
    "s3://bucket/events",
    mode="insert_overwrite",
    overwrite_where="dt = DATE '2026-08-25'",
).collect()
```

The table must already exist. `overwrite_where` determines which existing rows are removed;
the input DataFrame is appended as-is. Rows outside `overwrite_where` are not replaced by a
later re-run, so callers should filter the input first when they need idempotent replacement.

> **Warning:** Lance does not treat a concurrent append or update as conflicting with this
> commit, so rows another writer adds while the overwrite runs survive it even when they match
> `overwrite_where`, without any error. Make sure no other writer touches the table during a
> conditional overwrite.

### Column Updates

Use a prepared Daft DataFrame to overwrite existing Lance columns while
preserving row addresses and untouched column files:

```python
import daft
from daft_lance import read_lance, update_columns_df

source = read_lance(
    "s3://bucket/my_dataset",
    default_scan_options={"with_row_address": True},
    include_fragment_id=True,
)
source = source.where(daft.col("date") >= "2026-07-01")
source = source.with_column("label", recompute_label(source["value"]))

result = update_columns_df(
    source.select("_rowaddr", "fragment_id", "label"),
    "s3://bucket/my_dataset",
    columns=["label"],
)
print(result.version, result.rows_updated)
```

Struct columns are not supported: a source struct that omits one of the
target's fields casts cleanly with that field set to null, which would silently
drop data the caller never meant to overwrite.

The source `_rowaddr` values must be unique and must identify live rows of the
pinned target snapshot. Neither is checked, because the rewrite is a
left-outer join on `_rowaddr` and both failures are expressible in it: an
address matching no live row updates nothing, and a repeated address updates
its row once with one of the submitted values, chosen by row order rather than
by any rule. Both are silent, and both still count towards `rows_updated`.
Read the source from the snapshot you are updating (pass the same `version` if
you pin one) rather than replaying an address list produced against an older
snapshot, and make sure an upstream join cannot fan a row address out. The
update itself is committed atomically using Lance `RewriteColumns`.

Stable-row-ID datasets are rejected before any fragments are written because
current pylance bindings cannot propagate the offsets required to advance
`_row_last_updated_at_version` and keep CDF metadata correct.

Fragments are rewritten in parallel, so a per-fragment failure can surface
after other fragments have already written their new column files. Nothing is
committed and the dataset version does not move, but those unreferenced files
stay on storage until Lance cleans them up via
`LanceDataset.cleanup_old_versions`.

### Namespace Tables

Address Lance tables through a [Lance Namespace](https://lancedb.github.io/lance-namespace/)
(catalog) instead of a raw URI. Pass `namespace_impl` + `namespace_properties` + `table_id`
in place of `uri` — the namespace resolves the table's storage location and vends any storage
credentials. This works across `read_lance`, `write_lance`, `merge_columns_df`,
`update_columns_df`, `create_scalar_index`, and `compact_files`.

```python
import daft
import daft_lance

table_id = ["my_table"]
namespace = {"namespace_impl": "dir", "namespace_properties": {"root": "/tmp/lance_tables"}}

daft_lance.write_lance(
    daft.from_pydict({"id": [1, 2, 3]}),
    table_id=table_id,
    mode="create",
    **namespace,
).collect()

df = daft_lance.read_lance(table_id=table_id, **namespace)
```

`uri` and the namespace parameters are mutually exclusive: provide exactly one of `uri` or
(`namespace_impl` + `table_id`).

#### Using a REST namespace (e.g. Gravitino Lance REST server)

```python
namespace = {
    "namespace_impl": "rest",
    "namespace_properties": {"uri": "http://127.0.0.1:9101/lance"},
}
table_id = ["lance_catalog", "sales", "orders"]

daft_lance.write_lance(df, table_id=table_id, mode="create", **namespace).collect()
daft_lance.read_lance(table_id=table_id, **namespace).show()
```

When the catalog holds the storage configuration (bucket, endpoint, credentials), the
`describe_table` response vends `storage_options` to the client, so you do not need to pass
object-store credentials yourself. If the namespace does not vend credentials, your
`io_config` (or explicit `storage_options`) is applied to the resolved location; when both
are present, namespace-vended options take precedence.

Namespace clients are cached per (implementation, properties) pair. The cache size defaults
to 16 and can be tuned with the `DAFT_LANCE_NAMESPACE_CACHE_SIZE` environment variable
(read once at import time).

#### Daft's own entry points

Native namespace support in `daft.read_lance` / `DataFrame.write_lance` is tracked in
[Eventual-Inc/Daft#7282](https://github.com/Eventual-Inc/Daft/issues/7282); until that lands,
use the `daft_lance` entry points shown above for namespace-addressed tables.

## Migration

The migration only requires replacing `daft.io.lance` with `daft_lance`.

```sh
# See changes in current directory and all subdirectories
find . -type f -name "*.py" -exec sed 's/daft\.io\.lance/daft_lance/g' {} +

# Apply the changes
find . -type f -name "*.py" -exec sed -i 's/daft\.io\.lance/daft_lance/g' {} +
```

## Blob Support

The daft_lance extension supports Lance BLOB V2 by reading descriptors
into the following daft datatype. Note that `daft.read_lance` will NOT
materialize Lance BLOB V2 bytes.

```
{
  kind: uint8,
  position: uint64,
  size: uint64,
  blob_id: uint32,
  blob_uri: string,
}
```

To materialize blobs, read the dataset with row IDs enabled and call `take_blobs`:

```python
import lance
import daft
from daft_lance import take_blobs

ds = lance.dataset("s3://bucket/my_dataset")
df = daft.read_lance(ds.uri, default_scan_options={"with_row_id": True})
df = take_blobs(df, ds, "blob_column")

# each value is a lance.Blob — call .read() to fetch bytes
blobs = df.select("blob_column").to_pydict()["blob_column"]
data = blobs[0].read()
```

To write binary columns as Lance Blob V2, use the `blob_columns` opt-in:

```python
import daft

df = daft.from_pydict({"id": [1, 2, 3], "data": [b"...", b"...", b"..."]})
df.write_lance("s3://bucket/my_dataset", blob_columns=["data"]).collect()
```

## Development

Requires [uv](https://docs.astral.sh/uv/).

```sh
# Sync the development environment
make sync

# Run tests
make test

# Run linting and type checks
make lint
make typecheck

# Format code
make format

# Run all pre-commit hooks
make precommit

# Build sdist and wheel packages
make build
```

## License

Apache-2.0
