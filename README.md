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

```python
from daft_lance import create_scalar_index

create_scalar_index("s3://bucket/my_dataset", column="name", index_type="INVERTED")
```

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

The source `_rowaddr` values must all be unique live rows in the pinned target
snapshot. The update is committed atomically using Lance `RewriteColumns`.
Stable-row-ID datasets are rejected before any fragments are written because
current pylance bindings cannot propagate the offsets required to advance
`_row_last_updated_at_version` and keep CDF metadata correct.

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
