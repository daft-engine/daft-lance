from __future__ import annotations

import pickle
from collections.abc import Iterator
from functools import lru_cache
from typing import Any

import lance

from daft.daft import PyPartitionField, PyPushdowns, PyRecordBatch, ScanOperatorHandle, ScanTask
from daft.dataframe import DataFrame
from daft.dependencies import pa
from daft.io.scan import ScanOperator
from daft.logical.builder import LogicalPlanBuilder
from daft.recordbatch import RecordBatch
from daft.schema import Schema

from ._metadata import convert_lance_schema
from .namespace import DatasetOpenContext


@lru_cache(maxsize=1)
def _worker_dataset(serialized_context: bytes) -> lance.LanceDataset:
    context: DatasetOpenContext = pickle.loads(serialized_context)
    return context.open_pinned()


def _search_segments(
    serialized_context: bytes,
    segments: list[str],
    nearest: dict[str, Any],
    columns: list[str] | None,
    predicate: str | None,
    query_id: int,
) -> Iterator[PyRecordBatch]:
    ds = _worker_dataset(serialized_context)
    k = nearest["k"]
    table = ds.scanner(
        columns=columns,
        nearest=nearest,
        index_segments=segments,
        filter=predicate,
        prefilter=True,
        fast_search=True,
        with_row_id=True,
        blob_handling="blobs_descriptions",
    ).to_table()
    table = table.sort_by([("_distance", "ascending"), ("_rowid", "ascending")])
    table = table.slice(0, k)
    table = table.append_column("_query_id", pa.array([query_id] * table.num_rows, type=pa.int64()))
    for batch in table.to_batches():
        yield RecordBatch.from_arrow_record_batches([batch], batch.schema)._recordbatch


class VectorSearchScanOperator(ScanOperator):
    def __init__(
        self,
        open_context: DatasetOpenContext,
        groups: list[list[str]],
        nearest: dict[str, Any],
        columns: list[str] | None,
        predicate: str | None,
        query_id: int,
        schema: pa.Schema,
    ) -> None:
        self.open_context = open_context
        self.groups = groups
        self.nearest = nearest
        self.columns = columns
        self.predicate = predicate
        self.query_id = query_id
        self._schema = convert_lance_schema(schema)

    def schema(self) -> Schema:
        return self._schema

    def name(self) -> str:
        return "LanceVectorSearch"

    def as_pushdown_filter(self) -> None:
        return None

    def display_name(self) -> str:
        return f"LanceVectorSearch(version={self.open_context.version}, query={self.query_id})"

    def partitioning_keys(self) -> list[PyPartitionField]:
        return []

    def can_absorb_filter(self) -> bool:
        return False

    def can_absorb_limit(self) -> bool:
        return False

    def can_absorb_select(self) -> bool:
        return False

    def multiline_display(self) -> list[str]:
        return [self.display_name(), f"Index segment groups = {len(self.groups)}"]

    def to_scan_tasks(self, pushdowns: PyPushdowns) -> Iterator[ScanTask]:
        for segments in self.groups:
            yield ScanTask.python_factory_func_scan_task(
                module=__name__,
                func_name="_search_segments",
                func_args=(
                    pickle.dumps(self.open_context),
                    segments,
                    self.nearest,
                    self.columns,
                    self.predicate,
                    self.query_id,
                ),
                schema=self._schema._schema,
                num_rows=None,
                size_bytes=None,
                pushdowns=pushdowns,
                stats=None,
                source_name=self.display_name(),
            )


def vector_search_internal(
    ds: lance.LanceDataset,
    open_context: DatasetOpenContext,
    *,
    nearest: dict[str, Any],
    index_name: str | None,
    columns: list[str] | None,
    predicate: str | None,
    num_tasks: int | None,
) -> DataFrame:
    import numpy as np

    if num_tasks is not None and (not isinstance(num_tasks, int) or isinstance(num_tasks, bool) or num_tasks <= 0):
        raise ValueError("num_tasks must be a positive integer")
    k = nearest.get("k")
    if not isinstance(k, int) or isinstance(k, bool) or k <= 0:
        raise ValueError("nearest['k'] must be a positive integer")
    column = nearest.get("column")
    if columns is not None:
        columns = list(dict.fromkeys([*columns, "_distance"]))
    vectors = np.array(nearest.get("q"), dtype=np.float32, copy=True)
    if vectors.ndim == 1:
        vectors = vectors.reshape(1, -1)
    if vectors.ndim != 2 or not all(vectors.shape) or not np.isfinite(vectors).all():
        raise ValueError("nearest['q'] must be a finite vector or a non-empty matrix of query vectors")
    if "_query_id" in ds.schema.names:
        raise ValueError("_query_id is reserved for vector search results")
    field = ds.lance_schema.field(column) if isinstance(column, str) else None
    indices = [
        idx
        for idx in ds.describe_indices()
        if str(idx.index_type).startswith("IVF_")
        and field is not None
        and field.id() in idx.fields
        and (index_name is None or idx.name == index_name)
    ]
    if len(indices) != 1:
        raise ValueError("Select exactly one vector index for nearest['column']; specify index_name if ambiguous")
    index = indices[0]
    fragments = ds.get_fragments()
    live = {fragment.fragment_id for fragment in fragments}
    covered = {fid for segment in index.segments for fid in segment.fragment_ids}
    if live - covered:
        raise ValueError("vector_search requires complete index coverage; run optimize_indices or rebuild the index")
    segments = [str(segment.uuid) for segment in index.segments if live.intersection(segment.fragment_ids)]
    if not segments:
        raise ValueError("vector_search requires a non-empty vector index")
    count = min(num_tasks or len(segments), len(segments))
    if count == 1:
        groups = [segments]
    elif count == len(segments):
        groups = [[segment] for segment in segments]
    else:
        groups = [[] for _ in range(count)]
        rows = {fragment.fragment_id: fragment.metadata.num_rows for fragment in fragments}
        weights = {
            str(segment.uuid): sum(rows.get(fid, 0) for fid in segment.fragment_ids) for segment in index.segments
        }
        group_weights = [0] * count
        for segment in sorted(segments, key=lambda uuid: weights[uuid], reverse=True):
            group = min(range(count), key=lambda i: (group_weights[i], len(groups[i])))
            groups[group].append(segment)
            group_weights[group] += weights[segment]
    results: list[DataFrame] = []
    for query_id, vector in enumerate(vectors):
        query = {**nearest, "q": vector}
        # Building the scanner validates Lance-owned types, dimensions and
        # parameters without executing a query on the coordinator.
        schema = ds.scanner(
            columns=columns,
            nearest=query,
            index_segments=segments,
            filter=predicate,
            prefilter=True,
            fast_search=True,
            with_row_id=True,
            blob_handling="blobs_descriptions",
        ).projected_schema
        schema = schema.append(pa.field("_query_id", pa.int64()))
        operator = VectorSearchScanOperator(open_context, groups, query, columns, predicate, query_id, schema)
        handle = ScanOperatorHandle.from_python_scan_operator(operator)
        candidates = DataFrame(LogicalPlanBuilder.from_tabular_scan(scan_operator=handle))
        results.append(candidates.sort(["_distance", "_rowid"]).limit(k))
    result = results[0]
    for other in results[1:]:
        result = result.concat(other)
    return result.sort(["_query_id", "_distance", "_rowid"])
