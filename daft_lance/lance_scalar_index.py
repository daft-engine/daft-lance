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

    Unlike ``FragmentIndexHandler``, which writes partial index files sharing
    a single UUID, this handler builds a fully independent index segment per
    worker via the low-level ``_ds.create_index`` binding.  The returned
    ``lance.Index`` metadata (including ``index_details``) is serialised
    (pickled) so it can cross Daft process/serialisation boundaries.  The
    coordinator then commits all segments atomically with
    ``commit_existing_index_segments``.
    """

    def __init__(
        self,
        lance_ds: lance.LanceDataset,
        column: str,
        index_type: str,
        name: str,
        replace: bool,
        **kwargs: Any,
    ) -> None:
        self.lance_ds = lance_ds
        self.column = column
        self.index_type = index_type
        self.name = name
        self.replace = replace
        self.kwargs = kwargs

    def __call__(self, fragment_ids: list[int]) -> bytes:
        """Build an independent index segment and return its pickled metadata."""
        logger.info(
            "Building segmented index segment for fragments %s (column=%s, type=%s)",
            fragment_ids,
            self.column,
            self.index_type,
        )

        # _ds.create_index returns a lance.Index dataclass when fragment_ids
        # is provided (uncommitted segment mode).
        index_meta: lance.Index = self.lance_ds._ds.create_index(  # type: ignore[call-arg]
            [self.column],
            self.index_type,
            name=self.name,
            replace=self.replace,
            train=True,
            storage_options=None,
            kwargs={"fragment_ids": fragment_ids, **self.kwargs},
        )

        return pickle.dumps(index_meta)


def create_scalar_index_internal(
    lance_ds: lance.LanceDataset,
    uri: str | pathlib.Path,
    *,
    column: str,
    index_type: str = "INVERTED",
    name: str | None = None,
    replace: bool = True,
    storage_options: dict[str, Any] | None = None,
    fragment_group_size: int | None = None,
    num_partitions: int | None = None,
    max_concurrency: int | None = None,
    segmented: bool = False,
    **kwargs: Any,
) -> None:
    """Internal implementation of distributed scalar index creation.

    INVERTED and BTREE use a 3-phase distributed workflow (fragment-parallel build,
    merge_index_metadata, then commit). ``FTS`` is normalized to ``INVERTED`` (same Lance
    index); see Lance Rust/Python bindings: ``INVERTED`` and ``FTS`` map to the same
    inverted full-text index type.

    When ``segmented=True`` and ``index_type`` is ``BTREE``, a cleaner segmented workflow
    is used instead: each worker builds a fully independent index segment via the low-level
    ``_ds.create_index`` binding (which returns ``lance.Index`` metadata including
    ``index_details``), and the coordinator commits them atomically with
    ``commit_existing_index_segments``.  This resolves a known issue where ``index_details``
    was left empty in the legacy path, preventing ``describe_indices()`` from working.
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
    # Handle replace parameter - check for existing index with same name
    if not replace:
        existing_indices = []
        try:
            existing_indices = lance_ds.describe_indices()
        except Exception:
            # If we can't check existing indices, continue
            pass
        existing_names = {idx.name for idx in existing_indices}
        if name in existing_names:
            raise ValueError(f"Index with name '{name}' already exists. Set replace=True to replace it.")

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

    # Choose between the segmented workflow and the legacy partitioned-and-merged
    # workflow.  Segmented mode produces proper ``index_details`` so
    # ``describe_indices()`` works correctly.
    if segmented and index_type == "BTREE":
        _create_segmented_index(
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
    replace: bool,
    storage_options: dict[str, Any] | None,
    fragment_data: list[dict[str, list[int]]],
    fragment_ids_to_use: list[int],
    num_partitions: int | None,
    max_concurrency: int | None,
    **kwargs: Any,
) -> None:
    """Segmented index workflow: each worker builds an independent segment.

    Workers call the low-level ``_ds.create_index`` binding (which returns
    ``lance.Index`` metadata with ``index_details`` populated), pickle the
    result so it can traverse Daft serialisation boundaries, and return it.
    The coordinator unpickles all segments and commits them atomically via
    ``commit_existing_index_segments``.
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
        replace=replace,
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

    logger.info(
        "Collected %d index segments; committing as segmented index %s",
        len(index_metas),
        name,
    )

    # Reload dataset to pick up the latest version (segment files were written
    # by workers against the version that was current at their invocation time).
    lance_ds = lance.LanceDataset(uri, storage_options=storage_options)
    lance_ds.commit_existing_index_segments(name, column, index_metas)

    logger.info("Segmented index %s committed successfully", name)


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
