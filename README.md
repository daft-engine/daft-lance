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

## Migration

The migration only requires replacing `daft.io.lance` with `daft_lance`.

```sh
# See changes in current directory and all subdirectories
find . -type f -name "*.py" -exec sed 's/daft\.io\.lance/daft_lance/g' {} +

# Apply the changes
find . -type f -name "*.py" -exec sed -i 's/daft\.io\.lance/daft_lance/g' {} +
```

## Development

Requires [uv](https://docs.astral.sh/uv/).

```
uv sync
uv run pytest tests/ -v
```

## License

Apache-2.0
