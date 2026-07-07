from __future__ import annotations

import logging
import pickle
import uuid
from typing import TYPE_CHECKING, Any, cast

import daft
from daft import execution_config_ctx, from_pylist

if TYPE_CHECKING:
    import pathlib

import lance

from daft.dependencies import pa
from daft_lance.utils import distribute_fragments_balanced

logger = logging.getLogger(__name__)

# Segmented index types whose worker-built segments must be merged before commit.
MERGED_SEGMENTED_INDEX_TYPES = {"INVERTED"}


class FragmentIndexHandler:
    """Handler for distributed scalar index creation on fragment batches."""

    def __init__(
        self,
        lance_ds: lance.LanceDataset,
        column: str,
        index_type: str,
        name: str,
        fragment_uuid: str,
        replace: bool,
        **kwargs: Any,
    ) -> None:
        self.lance_ds = lance_ds
        self.column = column
        self.index_type = index_type
        self.name = name
        self.fragment_uuid = fragment_uuid
        self.replace = replace
        self.kwargs = kwargs

    def __call__(self, fragment_ids: list[int]) -> bool:
        """Process a batch of fragment IDs for scalar index creation."""
        logger.info(
            "Building distributed scalar index for fragments %s using create_scalar_index",
            fragment_ids,
        )

        self.lance_ds.create_scalar_index(
            column=self.column,
            index_type=self.index_type,  # type: ignore[arg-type]
            name=self.name,
            replace=self.replace,
            index_uuid=self.fragment_uuid,
            fragment_ids=fragment_ids,
            **self.kwargs,
        )
        return True


class SegmentedFragmentIndexHandler:
    """Handler for segmented scalar index creation on fragment batches.

    Each Daft worker receives a subset of Lance fragment IDs and builds an
    uncommitted index segment for just those fragments using Lance's public
    ``create_index_uncommitted`` API.  The worker returns the segment metadata
    to the coordinator, which commits all segments into the dataset manifest
    with ``commit_existing_index_segments``.
    """

    def __init__(
        self,
        lance_ds: lance.LanceDataset,
        column: str,
        index_type: str,
        name: str,
        **kwargs: Any,
    ) -> None:
        self.lance_ds = lance_ds
        self.column = column
        self.index_type = index_type
        self.name = name
        self.kwargs = kwargs

    def __call__(self, fragment_ids: list[int]) -> bytes:
        """Build an independent index segment and return its pickled metadata."""
        logger.info(
            "Building segmented index segment for fragments %s (column=%s, type=%s)",
            fragment_ids,
            self.column,
            self.index_type,
        )

        # Create one uncommitted index segment. ``pylance 8.0.0`` supports
        # scalar index segments through this public API. Segment creation always
        # uses ``replace=False`` because replacement, if supported, must happen
        # in the final manifest commit rather than independently in each worker.
        index_meta = self.lance_ds.create_index_uncommitted(
            column=self.column,
            index_type=self.index_type,
            name=self.name,
            replace=False,
            train=True,
            fragment_ids=fragment_ids,
            **self.kwargs,
        )

        return pickle.dumps(index_meta)


def _existing_index_names(lance_ds: lance.LanceDataset) -> set[str]:
    """Return existing index names, falling back for legacy indexes with bad details."""
    try:
        return {idx.name for idx in lance_ds.describe_indices()}
    except Exception:
        pass

    try:
        return {cast(dict[str, Any], idx)["name"] for idx in lance_ds.list_indices()}
    except Exception:
        return set()


def create_scalar_index_internal(
    lance_ds: lance.LanceDataset,
    uri: str | pathlib.Path,
    *,
    column: str,
    index_type: str = "INVERTED",
    name: str | None = None,
    replace: bool = False,
    storage_options: dict[str, Any] | None = None,
    fragment_group_size: int | None = None,
    num_partitions: int | None = None,
    max_concurrency: int | None = None,
    segmented: bool = False,
    **kwargs: Any,
) -> None:
    """Internal implementation of distributed scalar index creation.

    When ``segmented=True``, ``BTREE`` and ``INVERTED`` use Lance's public
    segment-index workflow: each worker builds a fully independent index segment,
    and the coordinator commits them atomically with
    ``commit_existing_index_segments``. ``FTS`` is normalized to ``INVERTED``
    (same Lance index); see Lance Rust/Python bindings: ``INVERTED`` and ``FTS``
    map to the same inverted full-text index type.
    """
    if not column:
        raise ValueError("Column name cannot be empty")

    index_type = index_type.upper()
    if index_type == "FTS":
        logger.info(
            "index_type FTS maps to INVERTED for scalar index creation (equivalent Lance index type).",
        )
        index_type = "INVERTED"

    # Validate column exists and has correct type
    try:
        field = lance_ds.schema.field(column)
    except KeyError as e:
        available_columns = [field.name for field in lance_ds.schema]
        raise ValueError(f"Column '{column}' not found. Available: {available_columns}") from e

    # Check column type
    value_type = field.type
    if pa.types.is_list(field.type) or pa.types.is_large_list(field.type):
        value_type = field.type.value_type

    match index_type:
        case "INVERTED":
            if not pa.types.is_string(value_type) and not pa.types.is_large_string(value_type):
                raise TypeError(f"Column {column} must be string type for INVERTED index, got {value_type}")
        case "BTREE":
            if (
                not pa.types.is_integer(value_type)
                and not pa.types.is_floating(value_type)
                and not pa.types.is_string(value_type)
            ):
                raise TypeError(f"Column {column} must be numeric or string type for BTREE index, got {value_type}")
        case _:
            logger.warning(
                "Distributed indexing currently only supports 'INVERTED' and 'BTREE' index types, not '%s'. So we are falling back to single-threaded index creation.",
                index_type,
            )
            lance_ds.create_scalar_index(
                column=column,
                index_type=index_type,  # type: ignore[arg-type]
                name=name,
                replace=replace,
                **kwargs,
            )
            return

    # Generate index name if not provided
    if name is None:
        name = f"{column}_{index_type.lower()}_idx"

    use_segmented_workflow = segmented

    # Handle replace parameter - check for existing index with same name
    if not replace or use_segmented_workflow:
        existing_names = _existing_index_names(lance_ds)
        if name in existing_names and use_segmented_workflow:
            raise ValueError(
                f"Index with name '{name}' already exists and cannot atomically replace existing index "
                "with Lance's public segmented index API. Drop the existing index first or use a different name."
            )
        if name in existing_names:
            raise ValueError(f"Index with name '{name}' already exists. Set replace=True to replace it.")

    if index_type == "BTREE" and not use_segmented_workflow:
        logger.info(
            "Falling back to Lance scalar index creation for non-segmented BTREE index %s.",
            name,
        )
        lance_ds.create_scalar_index(
            column=column,
            index_type=index_type,
            name=name,
            replace=replace,
            **kwargs,
        )
        return

    # Get available fragment IDs to use
    fragments = lance_ds.get_fragments()
    fragment_ids_to_use = [fragment.fragment_id for fragment in fragments]

    # Adjust fragment grouping size
    if fragment_group_size is None:
        fragment_group_size = 10
    elif fragment_group_size <= 0:
        raise ValueError("fragment_group_size must be positive")

    if fragment_group_size > len(fragment_ids_to_use) and fragment_ids_to_use:
        fragment_group_size = len(fragment_ids_to_use)
        logger.info(
            "Adjusted fragment_group_size to %d to match fragment count",
            fragment_group_size,
        )

    logger.info("Starting fragment-parallel processing and creating DataFrame with fragment batches")
    fragment_data = distribute_fragments_balanced(fragments, fragment_group_size)

    # Configure maximum concurrency for fragment batches
    if not fragment_data:
        logger.info("No fragments found for dataset at %s; skipping scalar index creation.", uri)
        return

    logger.info(
        "Starting distributed scalar index creation: column=%s, type=%s, name=%s, fragment_group_size=%s, max_concurrency=%s, segmented=%s",
        column,
        index_type,
        name,
        fragment_group_size,
        max_concurrency,
        segmented,
    )

    # Use segment-index creation for Lance scalar index types that expose the
    # public uncommitted segment API.  The legacy path is kept as a fallback for
    # older/unsupported distributed scalar index types.
    if use_segmented_workflow:
        _create_segmented_index(
            lance_ds=lance_ds,
            uri=uri,
            column=column,
            index_type=index_type,
            name=name,
            storage_options=storage_options,
            fragment_data=fragment_data,
            fragment_ids_to_use=fragment_ids_to_use,
            num_partitions=num_partitions,
            max_concurrency=max_concurrency,
            **kwargs,
        )
    else:
        _create_partitioned_index(
            lance_ds=lance_ds,
            uri=uri,
            column=column,
            index_type=index_type,
            name=name,
            replace=replace,
            storage_options=storage_options,
            fragment_data=fragment_data,
            fragment_ids_to_use=fragment_ids_to_use,
            num_partitions=num_partitions,
            max_concurrency=max_concurrency,
            **kwargs,
        )


def _create_segmented_index(
    lance_ds: lance.LanceDataset,
    uri: str | pathlib.Path,
    *,
    column: str,
    index_type: str,
    name: str,
    storage_options: dict[str, Any] | None,
    fragment_data: list[dict[str, list[int]]],
    fragment_ids_to_use: list[int],
    num_partitions: int | None,
    max_concurrency: int | None,
    **kwargs: Any,
) -> None:
    """Segmented index workflow: each worker builds an independent segment.

    Workers call Lance's uncommitted index segment API, pickle the returned
    ``lance.Index`` metadata so it can traverse Daft serialisation boundaries,
    and return it.  The coordinator unpickles all segments and commits them
    atomically via ``commit_existing_index_segments``.
    """
    handler_cls = daft.cls(
        SegmentedFragmentIndexHandler,
        max_concurrency=max_concurrency,
    )
    handler = handler_cls(
        lance_ds=lance_ds,
        column=column,
        index_type=index_type,
        name=name,
        **kwargs,
    )

    with execution_config_ctx(maintain_order=False):
        if num_partitions is not None and num_partitions > 1:
            df = from_pylist(fragment_data).repartition(num_partitions)
        else:
            df = from_pylist(fragment_data)

        df = df.select(handler(df["fragment_ids"]).alias("index_meta"))
        collected = df.collect()

    # Deserialise the Index metadata returned by each worker.
    index_metas: list[lance.Index | lance.indices.IndexSegment] = [
        pickle.loads(raw) for raw in collected.to_pydict()["index_meta"]
    ]

    # Reload dataset to pick up the latest version (segment files were written
    # by workers against the version that was current at their invocation time).
    lance_ds = lance.LanceDataset(uri, storage_options=storage_options)
    index_metas = _prepare_index_segments_for_commit(lance_ds, index_type, index_metas)

    logger.info(
        "Collected %d index segments; committing as segmented index %s",
        len(index_metas),
        name,
    )
    lance_ds.commit_existing_index_segments(name, column, index_metas)

    logger.info("Segmented index %s committed successfully", name)


def _prepare_index_segments_for_commit(
    lance_ds: lance.LanceDataset,
    index_type: str,
    index_metas: list[lance.Index | lance.indices.IndexSegment],
) -> list[lance.Index | lance.indices.IndexSegment]:
    """Prepare worker-built segments for the final manifest commit."""
    if index_type not in MERGED_SEGMENTED_INDEX_TYPES or len(index_metas) <= 1:
        return index_metas

    merged = lance_ds.merge_existing_index_segments([cast(lance.Index, segment) for segment in index_metas])
    return [merged]


def _create_partitioned_index(
    lance_ds: lance.LanceDataset,
    uri: str | pathlib.Path,
    *,
    column: str,
    index_type: str,
    name: str,
    replace: bool,
    storage_options: dict[str, Any] | None,
    fragment_data: list[dict[str, list[int]]],
    fragment_ids_to_use: list[int],
    num_partitions: int | None,
    max_concurrency: int | None,
    **kwargs: Any,
) -> None:
    """Legacy partitioned-and-merged index workflow.

    Workers build partial index files sharing the same UUID, then the
    coordinator merges them with ``merge_index_metadata`` and commits via a
    manual ``CreateIndex`` transaction.
    """
    # Generate unique index ID (shared across all partitions)
    index_id = str(uuid.uuid4())

    handler_cls = daft.cls(
        FragmentIndexHandler,
        max_concurrency=max_concurrency,
    )
    handler = handler_cls(
        lance_ds=lance_ds,
        column=column,
        index_type=index_type,
        name=name,
        fragment_uuid=index_id,
        replace=replace,
        **kwargs,
    )

    with execution_config_ctx(maintain_order=False):
        if num_partitions is not None and num_partitions > 1:
            df = from_pylist(fragment_data).repartition(num_partitions)
        else:
            df = from_pylist(fragment_data)

        df = df.select(handler(df["fragment_ids"]))
        df.collect()

    logger.info("Starting index metadata merging by reloading dataset to get latest state")
    lance_ds = lance.LanceDataset(uri, storage_options=storage_options)
    lance_ds.merge_index_metadata(index_id, index_type)

    logger.info("Starting atomic index creation and commit")
    field_id = lance_ds.schema.get_field_index(column)
    index = lance.Index(
        uuid=index_id,
        name=name,
        fields=[field_id],
        dataset_version=lance_ds.version,
        fragment_ids=set(fragment_ids_to_use),
        index_version=0,
    )
    removed_indices = []
    if replace:
        # NOTE: kept on list_indices() until the distributed-index commit path is
        # rewritten to populate index_details (e.g. via commit_existing_index_segments).
        # describe_indices() raises on indices produced by this flow because their
        # index_details field is empty.
        for idx_info_raw in lance_ds.list_indices():
            idx_info = cast(dict[str, Any], idx_info_raw)
            if idx_info["name"] == name:
                field_ids = [lance_ds.schema.get_field_index(f) for f in idx_info["fields"]]
                removed_indices.append(
                    lance.Index(
                        uuid=idx_info["uuid"],
                        name=idx_info["name"],
                        fields=field_ids,
                        dataset_version=lance_ds.version,
                        fragment_ids=idx_info["fragment_ids"],
                        index_version=idx_info["version"],
                        base_id=idx_info.get("base_id"),
                    )
                )

    create_index_op = lance.LanceOperation.CreateIndex(
        new_indices=[index],
        removed_indices=removed_indices,
    )

    # Commit the index operation atomically
    lance.LanceDataset.commit(
        uri,
        create_index_op,
        read_version=lance_ds.version,
        storage_options=storage_options,
    )

    logger.info("Index %s created successfully with ID %s", name, index_id)
