from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import lance
from lance import LanceDataset
from lance.optimize import Compaction, CompactionMetrics, CompactionOptions, CompactionTask, RewriteResult

import daft
from daft.dependencies import pa
from daft_lance._blob import detect_blob_v2_columns

logger = logging.getLogger(__name__)

_VALID_COMPACTION_OPTION_KEYS = set(CompactionOptions.__annotations__) | {"materialize_deletions_threshold"}


class CompactionTaskUDF:
    """UDF to execute a batch of Lance CompactionTasks on remote workers and return execution result dictionaries."""

    def __init__(
        self,
        lance_ds: LanceDataset,
    ) -> None:
        self.lance_ds = lance_ds

    def __call__(self, task: CompactionTask) -> RewriteResult:
        rewrite = task.execute(self.lance_ds)
        return rewrite


@dataclass(frozen=True)
class BlobV2CompactionMetrics:
    fragments_removed: int
    fragments_added: int
    files_removed: int
    files_added: int


def _compact_blob_v2_dataset(
    lance_ds: LanceDataset,
    blob_v2_columns: list[str],
    compaction_options: dict[str, Any] | None,
) -> BlobV2CompactionMetrics | None:
    """Compact Blob V2 datasets by materializing visible rows and rewriting them."""
    options = compaction_options or {}
    unknown_options = set(options) - _VALID_COMPACTION_OPTION_KEYS
    if unknown_options:
        raise ValueError(f"Invalid compaction options: {sorted(unknown_options)}")

    if lance_ds.version != lance_ds.latest_version:
        raise ValueError("Blob V2 compaction fallback only supports compacting the latest dataset version")

    fragments_before = len(lance_ds.get_fragments())
    deletions_before = sum(fragment.num_deletions for fragment in lance_ds.get_fragments())
    materialize_deletions = options.get("materialize_deletions", True)
    if fragments_before <= 1 and (not materialize_deletions or deletions_before == 0):
        logger.info("No Blob V2 compaction needed")
        return None

    blob_column_set = set(blob_v2_columns)
    non_blob_columns = [field.name for field in lance_ds.schema if field.name not in blob_column_set]
    visible_rows = lance_ds.to_table(columns=non_blob_columns, with_row_id=True)
    row_ids = visible_rows.column("_rowid").to_pylist()

    arrays: list[pa.Array[Any] | pa.ChunkedArray[Any]] = []
    fields: list[pa.Field[Any]] = []
    for field in lance_ds.schema:
        if field.name in blob_column_set:
            blobs = lance_ds.take_blobs(field.name, row_ids)
            blob_array = lance.blob_array([blob.read() if blob is not None else None for blob in blobs])
            arrays.append(blob_array)
            fields.append(pa.field(field.name, blob_array.type, nullable=field.nullable, metadata=field.metadata))
        else:
            arrays.append(visible_rows.column(field.name))
            fields.append(field)

    table = pa.Table.from_arrays(arrays, schema=pa.schema(fields, metadata=lance_ds.schema.metadata))
    files_before = sum(len(fragment.metadata.files) for fragment in lance_ds.get_fragments())
    compacted = lance.write_dataset(
        table,
        lance_ds,
        mode="overwrite",
        data_storage_version=getattr(lance_ds, "data_storage_version", None) or "2.2",
        max_rows_per_file=options.get("target_rows_per_fragment") or options.get("max_rows_per_file") or 1024 * 1024,
        max_rows_per_group=options.get("max_rows_per_group") or 1024,
        max_bytes_per_file=options.get("max_bytes_per_file") or 90 * 1024 * 1024 * 1024,
    )
    fragments_after = len(compacted.get_fragments())
    files_after = sum(len(fragment.metadata.files) for fragment in compacted.get_fragments())
    return BlobV2CompactionMetrics(
        fragments_removed=fragments_before,
        fragments_added=fragments_after,
        files_removed=files_before,
        files_added=files_after,
    )


def compact_files_internal(
    lance_ds: LanceDataset,
    *,
    compaction_options: dict[str, Any] | None = None,
    partition_num: int | None = None,
    concurrency: int | None = None,
) -> CompactionMetrics | BlobV2CompactionMetrics | None:
    """Execute Lance file compaction in distributed environment using Daft UDF style."""
    logger.info("Starting UDF-style distributed compaction")
    blob_v2_columns = detect_blob_v2_columns(lance_ds.schema)
    if blob_v2_columns:
        return _compact_blob_v2_dataset(lance_ds, blob_v2_columns, compaction_options)

    plan = Compaction.plan(
        lance_ds,
        CompactionOptions(
            **(compaction_options or {}),  # type: ignore[typeddict-item]
        ),
    )
    num_tasks = plan.num_tasks()
    logger.info("Compaction plan created with %d tasks", num_tasks)

    if num_tasks == 0:
        logger.info("No compaction tasks needed")
        return None

    effective_partition_num = partition_num or 1
    effective_partition_num = min(num_tasks, effective_partition_num)
    assert effective_partition_num > 0
    if effective_partition_num == 1:
        df = daft.from_pydict({"task": plan.tasks})
    else:
        df = daft.from_pydict({"task": plan.tasks}).repartition(effective_partition_num)

    WrappedRunner = daft.cls(
        CompactionTaskUDF,
        max_concurrency=concurrency,
    )
    df = df.select(WrappedRunner(lance_ds)(df["task"]).alias("rewrite"))
    results = df.to_pandas()

    metrics = Compaction.commit(lance_ds, results["rewrite"].to_list())
    logger.info("Compaction completed successfully. Metrics: %s", metrics)
    return metrics
