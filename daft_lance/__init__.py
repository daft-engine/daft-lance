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
)
from .lance_data_sink import LanceDataSink


def _patch_daft_lance_api() -> None:
    """Expose the daft-lance implementation through Daft's convenience APIs."""
    daft.read_lance = read_lance  # type: ignore[assignment]

    from daft.dataframe import DataFrame

    original_write_lance = getattr(DataFrame, "write_lance")
    if getattr(original_write_lance, "_daft_lance_namespace_patch", False):
        return

    def write_lance(
        self: DataFrame,
        uri: Any = None,
        mode: Literal["create", "append", "overwrite", "merge"] = "create",
        io_config: Any | None = None,
        schema: Any | None = None,
        left_on: str | None = None,
        right_on: str | None = None,
        **kwargs: Any,
    ) -> DataFrame:
        if mode == "merge":
            if any(k in kwargs for k in ("namespace_impl", "namespace_properties", "table_id")):
                raise ValueError("write_lance(mode='merge') does not support namespace parameters yet.")
            return original_write_lance(
                self,
                uri,
                mode=mode,
                io_config=io_config,
                schema=schema,
                left_on=left_on,
                right_on=right_on,
                **kwargs,
            )

        if schema is None:
            schema = self.schema()
        sanitized_kwargs = {k: v for k, v in kwargs.items() if k not in ("left_on", "right_on")}
        sink = LanceDataSink(uri, schema, mode, io_config, **sanitized_kwargs)
        return self.write_sink(sink)

    write_lance._daft_lance_namespace_patch = True  # type: ignore[attr-defined]
    DataFrame.write_lance = write_lance  # type: ignore[assignment]


_patch_daft_lance_api()

__all__ = [
    "compact_files",
    "create_scalar_index",
    "merge_columns",
    "merge_columns_df",
    "read_lance",
    "take_blobs",
]
