from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from lance import LanceDataset
from lance.optimize import Compaction, CompactionMetrics, CompactionOptions, CompactionTask, RewriteResult

import daft

if TYPE_CHECKING:
    from daft_lance.namespace import DatasetOpenContext

logger = logging.getLogger(__name__)


class CompactionTaskUDF:
    """UDF to execute a batch of Lance CompactionTasks on remote workers and return execution result dictionaries."""

    def __init__(
        self,
        open_context: DatasetOpenContext,
    ) -> None:
        self.open_context = open_context
        self._lance_ds: LanceDataset | None = None

    def _dataset(self) -> LanceDataset:
        # Opened once per UDF instance, not per task: the reopen costs a pinned
        # manifest read, so it must not sit on the per-row path.
        if self._lance_ds is None:
            self._lance_ds = self.open_context.open_pinned()
        return self._lance_ds

    def __call__(self, task: CompactionTask) -> RewriteResult:
        rewrite = task.execute(self._dataset())
        return rewrite


def compact_files_internal(
    lance_ds: LanceDataset,
    open_context: DatasetOpenContext,
    *,
    compaction_options: dict[str, Any] | None = None,
    partition_num: int | None = None,
    concurrency: int | None = None,
) -> CompactionMetrics | None:
    """Execute Lance file compaction in distributed environment using Daft UDF style.

    ``lance_ds`` is the driver's live dataset and stays on the driver for
    planning and the final commit; ``open_context`` is what workers reopen from.
    """
    logger.info("Starting UDF-style distributed compaction")
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
    df = df.select(WrappedRunner(open_context)(df["task"]).alias("rewrite"))
    results = df.to_pandas()

    metrics = Compaction.commit(lance_ds, results["rewrite"].to_list())
    logger.info("Compaction completed successfully. Metrics: %s", metrics)
    return metrics
