try:
    import daft
except ImportError:
    raise ImportError("daft-lance requires daft to be installed. Install it with: pip install 'daft[lance]'") from None

from ._blob import take_blobs
from ._lance import (
    compact_files,
    create_scalar_index,
    delete,
    merge_columns,
    merge_columns_df,
    merge_insert,
    read_lance,
    update,
)

__all__ = [
    "compact_files",
    "create_scalar_index",
    "delete",
    "merge_columns",
    "merge_columns_df",
    "merge_insert",
    "read_lance",
    "take_blobs",
    "update",
]
