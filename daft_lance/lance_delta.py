from __future__ import annotations

import pathlib
from collections.abc import Iterator
from typing import Any, Literal

import lance

from daft import context
from daft.api_annotations import PublicAPI
from daft.daft import IOConfig, PyPartitionField, PyPushdowns, PyRecordBatch, ScanOperatorHandle, ScanTask
from daft.dataframe import DataFrame
from daft.dependencies import pa
from daft.io.scan import ScanOperator
from daft.logical.builder import LogicalPlanBuilder
from daft.logical.schema import Schema
from daft.recordbatch import RecordBatch

from ._metadata import convert_lance_schema
from .namespace import open_dataset_from_open_kwargs
from .utils import LanceDatasetHandle, construct_lance_dataset_handle

ChangeMode = Literal["inserted", "updated", "deleted"]

_CHANGE_MODES: tuple[ChangeMode, ...] = ("inserted", "updated", "deleted")

# Row metadata the Lance readers append to the dataset columns. ``_rowid`` is
# encoded as uint64; the row-version columns are only present on inserted and
# updated rows, which both return the full set.
_ROW_META_FIELDS = (
    pa.field("_rowid", pa.uint64()),
    pa.field("_row_created_at_version", pa.uint64()),
    pa.field("_row_last_updated_at_version", pa.uint64()),
)


def _delta_result_schema(ds: lance.LanceDataset, mode: ChangeMode) -> pa.Schema:
    if mode == "deleted":
        return pa.schema([_ROW_META_FIELDS[0]])
    return pa.schema([*ds.schema, *_ROW_META_FIELDS])


def _lance_delta_factory_function(
    ds_uri: str,
    open_kwargs: dict[str, Any] | None,
    begin_version: int,
    end_version: int,
    mode: ChangeMode,
) -> Iterator[PyRecordBatch]:
    """Stream one change mode for a pinned version range on a worker.

    The dataset is reopened at the pinned end version, so a commit landing
    after planning cannot change either endpoint or the result.
    """
    ds = open_dataset_from_open_kwargs(ds_uri, open_kwargs)
    if begin_version == end_version:
        return
    delta = ds.delta(begin_version=begin_version, end_version=end_version)
    if mode == "inserted":
        reader = delta.get_inserted_rows()
    elif mode == "updated":
        reader = delta.get_updated_rows()
    else:
        reader = delta.get_deleted_row_ids()
    for batch in reader:
        yield RecordBatch.from_arrow_record_batches([batch], batch.schema)._recordbatch


class LanceDeltaScanOperator(ScanOperator):
    """Streams the changes between two Lance versions as a single source task."""

    def __init__(
        self,
        ds: lance.LanceDataset,
        result_schema: pa.Schema,
        begin_version: int,
        end_version: int,
        mode: ChangeMode,
        open_kwargs: dict[str, Any] | None = None,
    ):
        self._ds = ds
        self._open_kwargs = dict(open_kwargs or {})
        self._begin_version = begin_version
        self._end_version = end_version
        self._mode = mode
        self._schema = convert_lance_schema(result_schema)

    def name(self) -> str:
        return "LanceDeltaScanOperator"

    def display_name(self) -> str:
        return (
            f"LanceDeltaScanOperator({self._ds.uri}, mode={self._mode}, "
            f"versions={self._begin_version}..{self._end_version})"
        )

    def schema(self) -> Schema:
        return self._schema

    def partitioning_keys(self) -> list[PyPartitionField]:
        return []

    def can_absorb_filter(self) -> bool:
        return False

    def can_absorb_limit(self) -> bool:
        return False

    def can_absorb_select(self) -> bool:
        return False

    def multiline_display(self) -> list[str]:
        return [
            self.display_name(),
            f"Schema = {self.schema()}",
        ]

    def to_scan_tasks(self, pushdowns: PyPushdowns) -> Iterator[ScanTask]:
        yield ScanTask.python_factory_func_scan_task(
            module=_lance_delta_factory_function.__module__,
            func_name=_lance_delta_factory_function.__name__,
            func_args=(
                self._ds.uri,
                self._open_kwargs,
                self._begin_version,
                self._end_version,
                self._mode,
            ),
            schema=self.schema()._schema,
            num_rows=None,
            size_bytes=None,
            pushdowns=pushdowns,
            stats=None,
            source_name=self.display_name(),
        )


def _validate_version_range(begin_version: int, end_version: int) -> None:
    if isinstance(begin_version, bool) or isinstance(end_version, bool):
        raise TypeError("begin_version and end_version must be integers")
    if not isinstance(begin_version, int) or not isinstance(end_version, int):
        raise TypeError("begin_version and end_version must be integers")
    if begin_version < 1:
        raise ValueError(
            f"begin_version must be >= 1, got {begin_version}; the PyLance delta reader has no empty-snapshot endpoint"
        )
    if end_version < 1:
        raise ValueError(f"end_version must be >= 1, got {end_version}")
    if begin_version > end_version:
        raise ValueError(
            f"begin_version ({begin_version}) must be <= end_version ({end_version}); "
            "the range is (begin_version, end_version]"
        )


def _require_stable_row_ids(ds: lance.LanceDataset, version: int) -> None:
    if not ds.has_stable_row_ids:
        raise ValueError(
            f"version {version} does not use stable row ids; change readers require them. "
            "Create the dataset with enable_stable_row_ids=True."
        )


@PublicAPI
def read_lance_changes(
    uri: str | pathlib.Path | None = None,
    *,
    begin_version: int,
    end_version: int,
    mode: ChangeMode = "inserted",
    io_config: IOConfig | None = None,
    block_size: int | None = None,
    index_cache_size: int | None = None,
    metadata_cache_size_bytes: int | None = None,
    table_id: list[str] | None = None,
    namespace_impl: str | None = None,
    namespace_properties: dict[str, str] | None = None,
) -> DataFrame:
    """Read the rows that changed between two Lance versions as a lazy DataFrame.

    The version range is ``(begin_version, end_version]``: ``begin_version`` is
    exclusive and ``end_version`` is inclusive and must exist. Rows are
    compared by stable row id, so the dataset must use stable row ids at both
    endpoints unless the range is empty.

    Args:
        uri: The URI of the Lance table to read from. Accepts a local path or an
            object-store URI like "s3://bucket/path".
        begin_version: Exclusive start of the version range. Must be at least 1;
            PyLance's delta reader has no empty-snapshot endpoint.
        end_version: Inclusive end of the version range.
        mode: Which change to stream:
            ``"inserted"`` returns rows created inside the range, ``"updated"``
            returns existing rows whose values were rewritten inside it, and
            ``"deleted"`` returns only ``_rowid`` values that were live at
            ``begin_version`` and are absent at ``end_version``.
        io_config: A custom IOConfig to use when accessing Lance data. Defaults to None.
        block_size: Block size in bytes. Provide a hint for the size of the minimal I/O request.
        index_cache_size: Index cache size.
        metadata_cache_size_bytes: Size of the metadata cache in bytes.
        table_id: Table identifier within the namespace, e.g. ["catalog", "schema", "table"].
            Mutually exclusive with ``uri``.
        namespace_impl: Lance Namespace implementation, e.g. "dir" or "rest".
        namespace_properties: Properties for connecting to the namespace.

    Returns:
        DataFrame: A DataFrame with the change rows and the Lance row-metadata
        columns. Inserted and updated rows carry ``_rowid``,
        ``_row_created_at_version``, and ``_row_last_updated_at_version`` after
        the dataset columns; deleted rows carry ``_rowid`` only.

    Note:
        This is a version-range result, not an ordered change log: it does not
        report intermediate versions, and deletions only identify rows, so
        consumers that need business keys must keep their own mapping. Compaction
        that leaves row visibility unchanged does not appear as a deletion.

    Examples:
        Read the rows appended between versions 1 and 2:

        >>> import daft_lance
        >>> df = daft_lance.read_lance_changes("/path/to/lance/data/", begin_version=1, end_version=2)
        >>> df.show()

        Read deleted row ids as a lazy DataFrame:

        >>> df = daft_lance.read_lance_changes("/path/to/lance/data/", begin_version=1, end_version=2, mode="deleted")
    """
    if mode not in _CHANGE_MODES:
        raise ValueError(f"mode must be one of {_CHANGE_MODES}, got {mode!r}")
    _validate_version_range(begin_version, end_version)

    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config

    def _open(version: int) -> LanceDatasetHandle:
        return construct_lance_dataset_handle(
            uri,
            version=version,
            io_config=io_config,
            block_size=block_size,
            index_cache_size=index_cache_size,
            metadata_cache_size_bytes=metadata_cache_size_bytes,
            namespace_impl=namespace_impl,
            namespace_properties=namespace_properties,
            table_id=table_id,
        )

    # Pin the end version on the driver; workers reopen exactly this snapshot.
    end_handle = _open(end_version)
    end_ds = end_handle.dataset

    # Resolve the begin endpoint too, so an unavailable or non-stable version
    # fails while planning rather than on a worker.
    begin_ds = None
    if begin_version < end_version:
        begin_ds = _open(begin_version).dataset

    # An empty range compares nothing, so it needs no stable row ids; otherwise
    # both endpoints must carry them or the comparison is meaningless.
    if begin_version < end_version:
        _require_stable_row_ids(end_ds, end_version)
        if begin_ds is not None:
            _require_stable_row_ids(begin_ds, begin_version)

    result_schema = _delta_result_schema(end_ds, mode)
    operator = LanceDeltaScanOperator(
        end_ds,
        result_schema,
        begin_version=begin_version,
        end_version=end_version,
        mode=mode,
        open_kwargs=end_handle.open_kwargs,
    )
    handle = ScanOperatorHandle.from_python_scan_operator(operator)
    builder = LogicalPlanBuilder.from_tabular_scan(scan_operator=handle)
    return DataFrame(builder)
