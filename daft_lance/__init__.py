# ruff: noqa: I002
# isort: dont-add-import: from __future__ import annotations
import pathlib
import warnings
from typing import TYPE_CHECKING, Any, Optional, Union

from daft import context
from daft.daft import IOConfig
from daft.io.object_store_options import io_config_to_storage_options

from daft_lance.merge import merge_columns_from_df, merge_columns_internal
from daft_lance.rest_config import LanceRestConfig
from daft_lance.rest_write import create_lance_table_rest, register_lance_table_rest, write_lance_rest
from daft_lance.utils import construct_lance_dataset

if TYPE_CHECKING:
    from collections.abc import Callable

    from daft.dataframe import DataFrame
    from daft.dependencies import pa

    try:
        from lance.dataset import LanceDataset
        from lance.udf import BatchUDF
    except ImportError:
        BatchUDF = None
        LanceDataset = None

LanceDataset = Any


def merge_columns(
    uri: str | pathlib.Path,
    io_config: IOConfig | None = None,
    *,
    transform: Union[dict[str, str], "BatchUDF", Callable[["pa.lib.RecordBatch"], "pa.lib.RecordBatch"]] = None,
    read_columns: list[str] | None = None,
    reader_schema: Optional["pa.Schema"] = None,
    storage_options: dict[str, Any] | None = None,
    daft_remote_args: dict[str, Any] | None = None,
    concurrency: int | None = None,
    version: int | str | None = None,
    asof: str | None = None,
    block_size: int | None = None,
    commit_lock: Any | None = None,
    index_cache_size: int | None = None,
    default_scan_options: dict[str, Any] | None = None,
    metadata_cache_size_bytes: int | None = None,
) -> LanceDataset:
    """Merge new columns into a LanceDB table using a transformation function."""
    warnings.warn(
        "merge_columns is deprecated and will be removed in a future release. "
        "Please use merge_columns_df instead.",
        category=DeprecationWarning,
        stacklevel=2,
    )

    if transform is None:
        raise ValueError(
            "merge_columns requires a `transform` function; prefer using merge_columns_df with a prepared DataFrame if no transform is needed."
        )

    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config
    storage_options = storage_options or io_config_to_storage_options(io_config, uri)

    lance_ds = construct_lance_dataset(
        uri,
        storage_options=storage_options,
        version=version,
        asof=asof,
        block_size=block_size,
        commit_lock=commit_lock,
        index_cache_size=index_cache_size,
        default_scan_options=default_scan_options,
        metadata_cache_size_bytes=metadata_cache_size_bytes,
    )

    return merge_columns_internal(
        lance_ds,
        uri,
        transform=transform,
        read_columns=read_columns,
        reader_schema=reader_schema,
        storage_options=storage_options,
        daft_remote_args=daft_remote_args,
        concurrency=concurrency,
    )


def merge_columns_df(
    df: "DataFrame",
    uri: str | pathlib.Path,
    io_config: IOConfig | None = None,
    *,
    read_columns: list[str] | None = None,
    reader_schema: Optional["pa.Schema"] = None,
    storage_options: dict[str, Any] | None = None,
    daft_remote_args: dict[str, Any] | None = None,
    concurrency: int | None = None,
    version: int | str | None = None,
    asof: str | None = None,
    block_size: int | None = None,
    commit_lock: Any | None = None,
    index_cache_size: int | None = None,
    default_scan_options: dict[str, Any] | None = None,
    metadata_cache_size_bytes: int | None = None,
    batch_size: int | None = None,
    left_on: str | None = "_rowaddr",
    right_on: str | None = "_rowaddr",
) -> None:
    """Row-level merge columns entrypoint using a DataFrame."""
    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config
    storage_options = storage_options or io_config_to_storage_options(io_config, uri)

    lance_ds = construct_lance_dataset(
        uri,
        storage_options=storage_options,
        version=version,
        asof=asof,
        block_size=block_size,
        commit_lock=commit_lock,
        index_cache_size=index_cache_size,
        default_scan_options=default_scan_options,
        metadata_cache_size_bytes=metadata_cache_size_bytes,
    )

    effective_right_on = right_on or left_on
    effective_batch_size = (
        batch_size if batch_size is not None else daft_remote_args.get("batch_size", None) if daft_remote_args else None
    )

    return merge_columns_from_df(
        df,
        lance_ds=lance_ds,
        uri=uri,
        read_columns=read_columns,
        reader_schema=reader_schema,
        storage_options=storage_options,
        daft_remote_args=daft_remote_args,
        concurrency=concurrency,
        left_on=left_on,
        right_on=effective_right_on,
        batch_size=effective_batch_size,
    )


def create_scalar_index(
    uri: str | pathlib.Path,
    io_config: IOConfig | None = None,
    *,
    column: str,
    index_type: str = "INVERTED",
    name: str | None = None,
    replace: bool = True,
    storage_options: dict[str, Any] | None = None,
    version: int | str | None = None,
    asof: str | None = None,
    block_size: int | None = None,
    commit_lock: Any | None = None,
    index_cache_size: int | None = None,
    default_scan_options: dict[str, Any] | None = None,
    metadata_cache_size_bytes: int | None = None,
    fragment_group_size: int | None = None,
    num_partitions: int | None = None,
    max_concurrency: int | None = None,
    **kwargs: Any,
) -> None:
    """Build a distributed scalar index using Daft's distributed execution."""
    try:
        import lance
        from packaging import version as packaging_version

        from daft_lance.scalar_index import create_scalar_index_internal

        lance_version = packaging_version.parse(lance.__version__)
        min_required_version = packaging_version.parse("0.37.0")
        if lance_version < min_required_version:
            raise RuntimeError(
                f"Distributed indexing requires pylance >= 0.37.0, but found {lance.__version__}. "
                "The distributed indexing interfaces are not available in older versions. "
                "Please upgrade lance by running: pip install --upgrade pylance"
            )
    except ImportError as e:
        raise ImportError(
            "Unable to import the `lance` package, please install: `pip install pylance`"
        ) from e

    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config
    storage_options = storage_options or io_config_to_storage_options(io_config, str(uri))

    lance_ds = construct_lance_dataset(
        uri,
        storage_options=storage_options,
        version=version,
        asof=asof,
        block_size=block_size,
        commit_lock=commit_lock,
        index_cache_size=index_cache_size,
        default_scan_options=default_scan_options,
        metadata_cache_size_bytes=metadata_cache_size_bytes,
    )

    create_scalar_index_internal(
        lance_ds=lance_ds,
        uri=uri,
        column=column,
        index_type=index_type,
        name=name,
        replace=replace,
        storage_options=storage_options,
        fragment_group_size=fragment_group_size,
        num_partitions=num_partitions,
        max_concurrency=max_concurrency,
        **kwargs,
    )


def compact_files(
    uri: str | pathlib.Path,
    io_config: IOConfig | None = None,
    *,
    storage_options: dict[str, Any] | None = None,
    version: int | str | None = None,
    asof: str | None = None,
    block_size: int | None = None,
    commit_lock: Any | None = None,
    index_cache_size: int | None = None,
    default_scan_options: dict[str, Any] | None = None,
    metadata_cache_size_bytes: int | None = None,
    compaction_options: dict[str, Any] | None = None,
    partition_num: int | None = None,
    concurrency: int | None = None,
) -> Any:
    """Compact Lance dataset files using Daft UDF-style distributed execution."""
    try:
        import lance

        from daft_lance.compaction import compact_files_internal
    except ImportError as e:
        raise ImportError(
            "Unable to import the `lance` package, please install: `pip install pylance`"
        ) from e

    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config
    storage_options = storage_options or io_config_to_storage_options(
        io_config, str(uri) if isinstance(uri, pathlib.Path) else uri
    )

    lance_ds = lance.dataset(
        uri,
        storage_options=storage_options,
        version=version,
        asof=asof,
        block_size=block_size,
        commit_lock=commit_lock,
        index_cache_size=index_cache_size,
        default_scan_options=default_scan_options,
        metadata_cache_size_bytes=metadata_cache_size_bytes,
    )

    return compact_files_internal(
        lance_ds=lance_ds,
        compaction_options=compaction_options,
        partition_num=partition_num,
        concurrency=concurrency,
    )


__all__ = [
    "LanceRestConfig",
    "compact_files",
    "create_lance_table_rest",
    "create_scalar_index",
    "merge_columns",
    "merge_columns_df",
    "register_lance_table_rest",
    "write_lance_rest",
]
