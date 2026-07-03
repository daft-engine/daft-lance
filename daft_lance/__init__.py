from __future__ import annotations

from typing import Any, Literal

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
    write_lance,
)
from .lance_data_sink import LanceDataSink


def patch_daft() -> None:
    """Route ``daft.read_lance`` and ``DataFrame.write_lance`` through daft-lance.

    Optional convenience for drop-in use of namespace parameters with Daft's own
    APIs, e.g. ``df.write_lance(namespace_impl="dir", ...)``. Idempotent.
    """
    daft.read_lance = read_lance  # type: ignore[assignment]

    from daft.dataframe import DataFrame

    if getattr(DataFrame.write_lance, "_daft_lance_patch", False):
        return

    def _write_lance(
        self: DataFrame,
        uri: Any = None,
        mode: Literal["create", "append", "overwrite", "merge"] = "create",
        io_config: Any | None = None,
        schema: Any | None = None,
        left_on: str | None = None,
        right_on: str | None = None,
        **kwargs: Any,
    ) -> DataFrame:
        return write_lance(
            self,
            uri,
            mode=mode,
            io_config=io_config,
            schema=schema,
            left_on=left_on,
            right_on=right_on,
            **kwargs,
        )

    _write_lance._daft_lance_patch = True  # type: ignore[attr-defined]
    DataFrame.write_lance = _write_lance  # type: ignore[method-assign]


__all__ = [
    "LanceDataSink",
    "compact_files",
    "create_scalar_index",
    "merge_columns",
    "merge_columns_df",
    "patch_daft",
    "read_lance",
    "take_blobs",
    "write_lance",
]
