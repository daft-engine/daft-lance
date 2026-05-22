try:
    import daft
except ImportError:
    raise ImportError("daft-lance requires daft to be installed. Install it with: pip install 'daft[lance]'") from None

from ._blob import take_blobs
from ._lance import (
    compact_files,
    create_scalar_index,
    merge_columns,
    merge_columns_df,
    read_lance,
)

__all__ = [
    "compact_files",
    "create_scalar_index",
    "merge_columns",
    "merge_columns_df",
    "read_lance",
    "take_blobs",
]
