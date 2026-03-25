# daft-lance

Lance integration for [Daft](https://github.com/Eventual-Inc/Daft). Provides distributed compaction, scalar indexing, column merging, and REST catalog operations for Lance datasets.

## Install

```
pip install daft-lance
```

For REST catalog support:

```
pip install daft-lance[rest]
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

### REST Catalog

```python
from daft_lance import LanceRestConfig, write_lance_rest

config = LanceRestConfig(base_url="https://api.lancedb.com", api_key="...")
write_lance_rest(mp, config, namespace="default", table_name="my_table")
```

## Development

Requires [uv](https://docs.astral.sh/uv/).

```
uv sync
uv run pytest tests/ -v
```

## License

Apache-2.0
