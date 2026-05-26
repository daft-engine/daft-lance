try:
    import daft
except ImportError:
    raise ImportError("daft-lance requires daft to be installed. Install it with: pip install 'daft[lance]'") from None

from typing import Any

from ._blob import take_blobs
from ._lance import (
    compact_files,
    create_scalar_index,
    merge_columns,
    merge_columns_df,
    read_lance,
)

try:
    import daft.io.lance._lance as _daft_lance_mod

    _original_read_lance = _daft_lance_mod.read_lance

    def _patched_read_lance(*args: Any, **kwargs: Any) -> Any:
        return read_lance(*args, **kwargs)

    _daft_lance_mod.read_lance = _patched_read_lance
    if hasattr(daft, "read_lance"):
        daft.read_lance = _patched_read_lance
except Exception:
    pass

__all__ = [
    "compact_files",
    "create_scalar_index",
    "merge_columns",
    "merge_columns_df",
    "read_lance",
    "take_blobs",
]
