from __future__ import annotations

import pathlib
import warnings
from itertools import chain
from typing import TYPE_CHECKING, Literal

import lance
from lance.fragment import FragmentMetadata

from daft.context import get_context
from daft.datatype import DataType
from daft.dependencies import pa
from daft.io import DataSink
from daft.io.object_store_options import io_config_to_storage_options
from daft.io.sink import WriteResult
from daft.recordbatch import MicroPartition
from daft.schema import Schema
from daft_lance._blob import (
    BlobV2WritePolicy,
    LanceStorageVersion,
    _pyarrow_schema_castable,
    blob_aware_schema_for_validation,
    detect_blob_v2_columns,
    resolve_storage_version,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

    from daft.daft import IOConfig


class LanceDataSink(DataSink[list[FragmentMetadata]]):
    """WriteSink for writing data to a Lance dataset."""

    def __init__(
        self,
        uri: str | pathlib.Path,
        schema: Schema | pa.Schema,
        mode: Literal["create", "append", "overwrite"] = "create",
        io_config: IOConfig | None = None,
        *,
        blob_columns: list[str] | None = None,
        max_rows_per_file: int = 1024 * 1024,
        max_rows_per_group: int = 1024,
        max_bytes_per_file: int = 90 * 1024 * 1024 * 1024,
        data_storage_version: LanceStorageVersion | None = None,
        use_legacy_format: bool | None = None,
        enable_stable_row_ids: bool = False,
        storage_options: dict[str, str] | None = None,
    ) -> None:
        self._reject_unsupported_modes(mode, use_legacy_format)
        if not isinstance(uri, (str, pathlib.Path)):
            raise TypeError(f"Expected URI to be str or pathlib.Path, got {type(uri)}")

        self._table_uri = str(uri)
        self._mode = mode
        self._io_config = get_context().daft_planning_config.default_io_config if io_config is None else io_config
        self._storage_options = (
            storage_options
            if storage_options is not None
            else io_config_to_storage_options(self._io_config, self._table_uri)
        )
        self._init_lance_knobs(
            max_rows_per_file=max_rows_per_file,
            max_rows_per_group=max_rows_per_group,
            max_bytes_per_file=max_bytes_per_file,
            use_legacy_format=use_legacy_format,
            enable_stable_row_ids=enable_stable_row_ids,
        )
        self._pyarrow_schema = self._normalize_schema(schema)
        self._init_blob_policy(blob_columns)

        self._version: int = 0
        self._table_schema: pa.Schema | None = None
        existing = self._absorb_existing_dataset()
        existing_version = getattr(existing, "data_storage_version", None) if existing is not None else None
        self._data_storage_version = resolve_storage_version(
            self._blob.apply_blob_v2_default(data_storage_version),
            existing_version,
            self._mode,
        )

        # Auto-pick up any existing lance.blob.v2 columns when appending so the
        # write path wraps the matching daft binary columns.
        if self._mode == "append" and self._table_schema is not None:
            self._blob.add_columns(detect_blob_v2_columns(self._table_schema))

        # Schema actually written to the dataset (blob columns retyped to lance.blob.v2).
        self._effective_pyarrow_schema = self._blob.build_effective_schema(self._pyarrow_schema)
        self._schema = Schema._from_field_name_and_types(
            [
                ("num_fragments", DataType.int64()),
                ("num_deleted_rows", DataType.int64()),
                ("num_small_files", DataType.int64()),
                ("version", DataType.int64()),
            ]
        )

    @staticmethod
    def _reject_unsupported_modes(
        mode: Literal["create", "append", "overwrite"], use_legacy_format: bool | None
    ) -> None:
        # This mode was never functional and customers must use merge_columns_df.
        if mode == "merge":  # type: ignore[comparison-overlap]
            raise ValueError(
                'mode="merge" is no longer supported by LanceDataSink. Use '
                "daft_lance.merge_columns_df(df, uri, ...) for row-level merges keyed by "
                "_rowaddr/fragment_id, or daft_lance.merge_columns(uri, transform=...) "
                "for column merges driven by a UDF."
            )
        # Remove this warning when use_legacy_format is removed upstream.
        if use_legacy_format is not None:
            warnings.warn(
                "use_legacy_format is deprecated upstream in Lance and will be removed; "
                "use data_storage_version instead.",
                DeprecationWarning,
                stacklevel=3,
            )

    def _init_lance_knobs(
        self,
        *,
        max_rows_per_file: int,
        max_rows_per_group: int,
        max_bytes_per_file: int,
        use_legacy_format: bool | None,
        enable_stable_row_ids: bool,
    ) -> None:
        self._max_rows_per_file = max_rows_per_file
        self._max_rows_per_group = max_rows_per_group
        self._max_bytes_per_file = max_bytes_per_file
        self._use_legacy_format = use_legacy_format
        self._enable_stable_row_ids = enable_stable_row_ids

    @staticmethod
    def _normalize_schema(schema: Schema | pa.Schema) -> pa.Schema:
        if isinstance(schema, Schema):
            return schema.to_pyarrow_schema()
        if isinstance(schema, pa.Schema):
            return schema
        raise TypeError(f"Expected schema to be Schema or pa.Schema, got {type(schema)}")

    def _init_blob_policy(self, blob_columns: list[str] | None) -> None:
        self._blob = BlobV2WritePolicy(blob_columns)
        self._blob.validate_columns_present(self._pyarrow_schema)

    def _absorb_existing_dataset(self) -> lance.LanceDataset | None:
        """Open the existing dataset (if any), set table-state, and validate the requested mode.

        - Returns the dataset when one exists.
        - Raises ``ValueError`` if appending to a missing dataset.
        - Raises ``ValueError`` if creating where a dataset already exists.
        - Raises ``FileExistsError`` if creating where a regular file already lives.
        """
        dataset: lance.LanceDataset | None
        try:
            dataset = lance.dataset(self._table_uri, storage_options=self._storage_options)
        except (ValueError, FileNotFoundError, OSError) as e:
            # Pinned to the Rust message format; lance has no typed exception. See test_lance_message_format_unchanged.
            if "was not found" in str(e):
                dataset = None
            else:
                # Re-raise other errors (permissions, network, etc.)
                raise

        if dataset is None:
            if self._mode == "append":
                raise ValueError("Cannot append to non-existent Lance dataset.")
            if self._mode == "create" and self._storage_options is None:
                p = pathlib.Path(self._table_uri)
                if p.is_file():
                    raise FileExistsError("Target path points to a file, cannot create a dataset here.")
            return None

        self._table_schema = dataset.schema
        self._version = dataset.latest_version

        if self._mode == "create":
            raise ValueError("Cannot create a Lance dataset at a location where one already exists.")

        if self._mode == "append" and not _pyarrow_schema_castable(
            blob_aware_schema_for_validation(self._pyarrow_schema, self._table_schema),
            blob_aware_schema_for_validation(self._table_schema, self._table_schema),
        ):
            raise ValueError(
                "Schema of data does not match table schema\n"
                f"Data schema:\n{self._pyarrow_schema}\nTable Schema:\n{self._table_schema}"
            )
        return dataset

    def name(self) -> str:
        """Optional custom sink name."""
        return "Lance Write"

    def schema(self) -> Schema:
        return self._schema

    def _prepare_arrow_table(self, input_table: pa.Table) -> pa.Table:
        target_schema = self._table_schema if self._table_schema is not None else self._pyarrow_schema
        target_schema = self._blob.cast_target_schema(target_schema)
        if self._table_schema is not None:
            return input_table.cast(target_schema)
        if not pa.Schema.equals(target_schema, input_table.schema):
            return input_table.cast(target_schema)
        return pa.Table.from_batches(input_table.to_batches(), target_schema)

    def _write_arrow_table(self, table: pa.Table) -> WriteResult[list[FragmentMetadata]]:
        wrapped = self._blob.wrap_table(table)
        fragments = lance.fragment.write_fragments(
            wrapped,
            dataset_uri=self._table_uri,
            mode=self._mode,
            storage_options=self._storage_options,
            max_rows_per_file=self._max_rows_per_file,
            max_rows_per_group=self._max_rows_per_group,
            max_bytes_per_file=self._max_bytes_per_file,
            data_storage_version=self._data_storage_version,
            use_legacy_format=self._use_legacy_format,
            enable_stable_row_ids=self._enable_stable_row_ids,
        )
        # Sum on-disk sizes from fragment metadata. Lance Blob V2 sidecar .blob
        # files are not tracked in FragmentMetadata.files (out of scope here).
        bytes_written = sum(
            (f.file_size_bytes or 0) for frag in fragments for f in (getattr(frag, "files", None) or ())
        )
        return WriteResult(result=fragments, bytes_written=bytes_written, rows_written=wrapped.num_rows)

    def write(self, micropartitions: Iterator[MicroPartition]) -> Iterator[WriteResult[list[FragmentMetadata]]]:
        """Writes fragments from the given micropartitions."""
        buffer = _LanceFragmentBuffer(
            max_rows=self._max_rows_per_file,
            max_bytes=self._max_bytes_per_file,
        )

        for micropartition in micropartitions:
            arrow_table = self._prepare_arrow_table(micropartition.to_arrow())

            # Oversized inputs flush whatever we already have, then write directly
            # so Lance can split internally.
            if arrow_table.num_rows >= self._max_rows_per_file or arrow_table.nbytes >= self._max_bytes_per_file:
                if buffer.has_rows():
                    yield self._write_arrow_table(buffer.drain())
                yield self._write_arrow_table(arrow_table)
                continue

            if buffer.add(arrow_table):
                yield self._write_arrow_table(buffer.drain())

        if buffer.has_rows():
            yield self._write_arrow_table(buffer.drain())

    def finalize(self, write_results: list[WriteResult[list[FragmentMetadata]]]) -> MicroPartition:
        """Commits the fragments to the Lance dataset. Returns a DataFrame with the stats of the dataset."""
        fragments = list(chain.from_iterable(write_result.result for write_result in write_results))

        operation: lance.LanceOperation.BaseOperation
        if self._mode == "create" or self._mode == "overwrite":
            operation = lance.LanceOperation.Overwrite(self._effective_pyarrow_schema, fragments)
        elif self._mode == "append":
            operation = lance.LanceOperation.Append(fragments)

        dataset = lance.LanceDataset.commit(
            self._table_uri,
            operation,
            read_version=self._version,
            storage_options=self._storage_options,
        )
        stats = dataset.stats.dataset_stats()
        stats_dict = MicroPartition.from_pydict(
            {
                "num_fragments": pa.array([stats["num_fragments"]], type=pa.int64()),
                "num_deleted_rows": pa.array([stats["num_deleted_rows"]], type=pa.int64()),
                "num_small_files": pa.array([stats["num_small_files"]], type=pa.int64()),
                "version": pa.array([dataset.version], type=pa.int64()),
            }
        )
        return stats_dict


class _LanceFragmentBuffer:
    """Accumulates pyarrow tables until a row-count or byte-size threshold is hit."""

    def __init__(self, max_rows: int, max_bytes: int) -> None:
        self._max_rows = max_rows
        self._max_bytes = max_bytes
        self._batches: list[pa.RecordBatch] = []
        self._rows = 0
        self._bytes = 0
        self._schema: pa.Schema | None = None

    def add(self, table: pa.Table) -> bool:
        """Add a table to the buffer, returning True if the buffer has reached its row or byte threshold."""
        if self._schema is None:
            self._schema = table.schema
        for b in table.to_batches():
            self._batches.append(b)
        self._rows += table.num_rows
        self._bytes += table.nbytes
        return self._rows >= self._max_rows or self._bytes >= self._max_bytes

    def has_rows(self) -> bool:
        return self._rows > 0

    def drain(self) -> pa.Table:
        """Build the accumulated table and reset internal state."""
        assert self._schema is not None
        table = pa.Table.from_batches(self._batches, schema=self._schema)
        self._batches = []
        self._rows = 0
        self._bytes = 0
        return table
