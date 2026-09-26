from __future__ import annotations

import logging
import pickle
from typing import TYPE_CHECKING, Any

import daft
from daft import execution_config_ctx, from_pylist

if TYPE_CHECKING:
    from daft_lance.namespace import DatasetOpenContext

import lance

from daft.dependencies import pa
from daft_lance.utils import distribute_fragments_balanced

logger = logging.getLogger(__name__)

# Scalar index types built with the distributed segment workflow. pylance 11
# reports every scalar index type as segment-native; RTREE additionally
# requires GeoArrow extension columns and stays unsupported until the test
# suite can create those columns.
DISTRIBUTED_INDEX_TYPES = frozenset({"BTREE", "BITMAP", "INVERTED", "ZONEMAP", "NGRAM", "LABEL_LIST", "BLOOMFILTER"})


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
        open_context: DatasetOpenContext,
        column: str,
        index_type: str,
        name: str,
        replace: bool = False,
        **kwargs: Any,
    ) -> None:
        self.open_context = open_context
        self.column = column
        self.index_type = index_type
        self.name = name
        self.replace = replace
        self.kwargs = kwargs
        self._lance_ds: lance.LanceDataset | None = None

    def _dataset(self) -> lance.LanceDataset:
        if self._lance_ds is None:
            self._lance_ds = self.open_context.open_pinned()
        return self._lance_ds

    def __call__(self, fragment_ids: list[int]) -> bytes:
        """Build an independent index segment and return its pickled metadata."""
        logger.info(
            "Building segmented index segment for fragments %s (column=%s, type=%s)",
            fragment_ids,
            self.column,
            self.index_type,
        )
        segment_kwargs = self.kwargs.copy()

        # Create one uncommitted index segment.  Segment creation normally
        # uses ``replace=False`` because replacement must happen in the final
        # manifest commit rather than independently in each worker.  The one
        # exception is a replace=True rebuild: the worker's pinned snapshot
        # still contains the same-named index (whether or not the driver
        # dropped it), so Lance rejects building against that name with
        # ``replace=False``.  The driver opts workers into ``replace=True``
        # in exactly that case, and the coordinator's commit lands the new
        # segments atomically — retiring overlapped old segments, or creating
        # the index fresh on a post-drop manifest.
        index_meta = self._dataset().create_index_uncommitted(
            column=self.column,
            index_type=self.index_type,
            name=self.name,
            replace=self.replace,
            train=True,
            fragment_ids=fragment_ids,
            **segment_kwargs,
        )

        return pickle.dumps(index_meta)


def _existing_index_coverage(lance_ds: lance.LanceDataset, name: str) -> set[int] | None:
    """Return the fragment IDs covered by an existing index, or None if absent.

    The coverage is the union of the fragment IDs covered by the index's
    committed segments. Returns ``None`` when no index with that name exists
    or when the indices cannot be described. Column/type compatibility is not
    checked here: Lance's build and commit APIs reject incompatible
    combinations, and duplicating those rules in string space has caused false
    rejections before (e.g. 'LabelList' vs 'LABEL_LIST').
    """
    try:
        descriptions = lance_ds.describe_indices()
    except Exception:
        logger.warning("describe_indices() failed; treating index '%s' as absent", name, exc_info=True)
        return None

    for desc in descriptions:
        if desc.name != name:
            continue
        covered: set[int] = set()
        for segment in desc.segments or []:
            covered.update(segment.fragment_ids or ())
        return covered
    return None


def create_scalar_index_internal(
    lance_ds: lance.LanceDataset,
    open_context: DatasetOpenContext,
    *,
    column: str,
    index_type: str = "INVERTED",
    name: str | None = None,
    replace: bool = True,
    fragment_group_size: int | None = None,
    num_partitions: int | None = None,
    max_concurrency: int | None = None,
    fragment_ids: list[int] | None = None,
    **kwargs: Any,
) -> None:
    """Internal implementation of distributed scalar index creation.

    ``lance_ds`` is the driver's live dataset (planning, validation, commits);
    ``open_context`` is the serializable handle workers reopen from and the
    single source of uri, storage options and namespace kwargs.

    Every supported index type is built with the distributed segment-index
    workflow: each worker builds a fully independent index segment with
    ``create_index_uncommitted``, and the coordinator commits them atomically
    with ``commit_existing_index_segments``, which records complete index
    metadata (no more empty ``index_details``). ``FTS`` is normalized to
    ``INVERTED`` (same Lance index).

    ``replace=True`` (the default) relies on Lance core's atomic overlap
    replacement: ``commit_existing_index_segments`` removes committed segments
    whose fragments overlap the incoming ones in the same CreateIndex
    transaction, so a full-coverage rebuild swaps the old index in one
    transaction. ``replace=False`` refuses to touch an existing index; the
    default matches pylance's own ``replace`` default. Types without a distributed
    path raise ``ValueError`` instead of silently falling back to single-node
    Lance indexing — callers wanting single-node execution should call pylance
    directly.

    ``fragment_ids`` restricts the build to a subset of the dataset's
    fragments. When the named index already exists, already-covered fragments
    are skipped and only the remaining ones are built and appended; untouched
    committed segments are preserved. This is the incremental backfill path
    for newly appended fragments. Column/type compatibility of a same-name
    index is validated by Lance's build/commit APIs, not duplicated here.
    """
    if not column:
        raise ValueError("Column name cannot be empty")

    if "segmented" in kwargs:
        # Removed parameter: **kwargs would otherwise forward it to Lance,
        # which fails deep inside a worker with a confusing index-parameter
        # error instead of at the API boundary.
        raise TypeError(
            "The 'segmented' parameter was removed: the distributed segment-index "
            "workflow is now the only code path. Remove the argument."
        )

    index_type = index_type.upper()
    if index_type == "FTS":
        logger.info(
            "index_type FTS maps to INVERTED for scalar index creation (equivalent Lance index type).",
        )
        index_type = "INVERTED"

    if index_type not in DISTRIBUTED_INDEX_TYPES:
        raise ValueError(
            f"Unsupported distributed index type '{index_type}'. Supported types: "
            f"{sorted(DISTRIBUTED_INDEX_TYPES)} (plus 'FTS'). For other types call pylance "
            f"directly: lance.dataset(<uri>).create_scalar_index(...)."
        )

    # Validate column exists and has correct type
    try:
        field = lance_ds.schema.field(column)
    except KeyError as e:
        available_columns = [field.name for field in lance_ds.schema]
        raise ValueError(f"Column '{column}' not found. Available: {available_columns}") from e

    # Check column type for the types with an obvious Python-side rule; the
    # rest are validated by Lance during the distributed build.
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
            pass

    # Generate index name if not provided (matches pylance's convention)
    if name is None:
        name = f"{column}_idx"

    fragments = lance_ds.get_fragments()
    available_fragment_ids = {fragment.fragment_id for fragment in fragments}

    # Validate and normalize the requested fragment subset, if any.
    requested_fragment_ids: set[int] | None = None
    if fragment_ids is not None:
        if len(fragment_ids) == 0:
            raise ValueError("fragment_ids must be a non-empty list of fragment IDs; pass None to index all fragments.")
        unique_ids = list(dict.fromkeys(fragment_ids))
        duplicates = sorted({fid for fid in unique_ids if fragment_ids.count(fid) > 1})
        if duplicates:
            logger.warning("Duplicate fragment_ids %s were given; each fragment is scheduled once.", duplicates)
        unknown_ids = sorted(fid for fid in unique_ids if fid not in available_fragment_ids)
        if unknown_ids:
            raise ValueError(
                f"fragment_ids {unknown_ids} do not exist in the dataset. "
                f"Available fragment IDs: {sorted(available_fragment_ids)}"
            )
        requested_fragment_ids = set(unique_ids)

    existing_coverage = _existing_index_coverage(lance_ds, name)
    if existing_coverage is not None:
        # Column/type compatibility of a same-name index is validated by
        # Lance itself: the build API rejects a different column ("already
        # exists with different fields") and the commit API rejects appending
        # segments of a different type. Duplicating those rules here would be
        # a string-space copy that can drift from the real type system (it
        # already caused false rejections once), so no pre-check is kept.
        if not replace and requested_fragment_ids is None:
            raise ValueError(f"Index with name '{name}' already exists. Set replace=True to replace it.")

    # Replacement rides on Lance core's atomic overlap replacement:
    # commit_existing_index_segments retires committed segments whose fragments
    # overlap the incoming ones in the same CreateIndex transaction, so a
    # full-coverage rebuild swaps the old index atomically. Segments that no
    # longer overlap any live fragment cannot be retired this way, but normal
    # operations never produce them (compaction rewrites coverage; delete
    # retires fully-dead segments) and any that appear are healed by
    # ``optimize_indices``. Workers open the pinned snapshot where a
    # same-name index still exists; building against that name always
    # requires replace=True.
    handler_replace = existing_coverage is not None
    if existing_coverage is not None and requested_fragment_ids is not None:
        # Incremental backfill: skip fragments already covered by committed
        # segments; only the remainder is built and appended (non-overlapping
        # segments are appended, not swapped).
        covered = existing_coverage & available_fragment_ids
        already_covered = requested_fragment_ids & covered
        to_build = requested_fragment_ids - covered
        if already_covered:
            logger.info(
                "Fragments %s are already covered by index '%s'; skipping them.",
                sorted(already_covered),
                name,
            )
        if not to_build:
            logger.info("All requested fragments are already covered by index '%s'; nothing to build.", name)
            return
        requested_fragment_ids = to_build

    if requested_fragment_ids is not None:
        fragments = [fragment for fragment in fragments if fragment.fragment_id in requested_fragment_ids]
    fragment_ids_to_use = sorted(
        requested_fragment_ids if requested_fragment_ids is not None else (f.fragment_id for f in fragments)
    )

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
        raise ValueError(f"Dataset at {open_context.uri} contains no fragments")

    logger.info(
        "Starting distributed scalar index creation: column=%s, type=%s, name=%s, fragment_group_size=%s, max_concurrency=%s",
        column,
        index_type,
        name,
        fragment_group_size,
        max_concurrency,
    )

    _create_segmented_index(
        open_context=open_context,
        column=column,
        index_type=index_type,
        name=name,
        fragment_data=fragment_data,
        expected_fragment_ids=fragment_ids_to_use,
        num_partitions=num_partitions,
        max_concurrency=max_concurrency,
        handler_replace=handler_replace,
        **kwargs,
    )


def _create_segmented_index(
    open_context: DatasetOpenContext,
    *,
    column: str,
    index_type: str,
    name: str,
    fragment_data: list[dict[str, list[int]]],
    expected_fragment_ids: list[int] | None = None,
    num_partitions: int | None,
    max_concurrency: int | None,
    handler_replace: bool = False,
    **kwargs: Any,
) -> None:
    """Segmented index workflow: each worker builds an independent segment.

    Workers call Lance's uncommitted index segment API, pickle the returned
    ``lance.Index`` metadata so it can traverse Daft serialisation boundaries,
    and return it.  The coordinator unpickles all segments and commits them
    as-is via ``commit_existing_index_segments``: committed segments whose
    fragments overlap the incoming ones are retired in the same transaction
    (atomic replacement), and non-overlapping ones are appended.  Multi-segment
    indexes are fully functional without merging (verified: split segments are
    loaded and pruned at query time with identical scores), so no merge is
    performed; compaction is left to ``optimize_indices``.  When
    ``handler_replace`` is set the workers' pinned snapshot still contains a
    same-named index; they must build with ``replace=True`` for Lance to
    accept the name.
    """
    handler_cls = daft.cls(
        SegmentedFragmentIndexHandler,
        max_concurrency=max_concurrency,
    )
    handler = handler_cls(
        open_context=open_context,
        column=column,
        index_type=index_type,
        name=name,
        replace=handler_replace,
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
    lance_ds = open_context.open_latest()
    _validate_segments_against_manifest(lance_ds, index_metas, expected_fragment_ids)

    logger.info(
        "Collected %d index segments; committing as segmented index %s",
        len(index_metas),
        name,
    )
    lance_ds.commit_existing_index_segments(name, column, index_metas)

    logger.info("Segmented index %s committed successfully", name)


def _validate_segments_against_manifest(
    lance_ds: lance.LanceDataset,
    index_metas: list[lance.Index | lance.indices.IndexSegment],
    expected_fragment_ids: list[int] | None = None,
) -> None:
    """Validate worker-built segments before the commit.

    Three checks, each failing loudly instead of committing a broken index:

    - Dead fragments: a segment references a fragment ID that no longer exists
      in the manifest (a concurrent compaction rewrote it while the index was
      being built). Lance would still accept the commit and the index would
      permanently reference dead fragment IDs.
    - Overlapping coverage: two segments cover the same fragment. Every
      fragment must be covered exactly once.
    - Incomplete coverage (when ``expected_fragment_ids`` is given): the union
      of the segments' coverage must equal the scheduled fragment set, so a
      silently lost worker result cannot produce a partial index.
    """
    live_fragment_ids = {fragment.fragment_id for fragment in lance_ds.get_fragments()}
    covered: set[int] = set()
    duplicate_ids: set[int] = set()
    dead_ids: set[int] = set()
    for meta in index_metas:
        for fragment_id in getattr(meta, "fragment_ids", None) or ():
            if fragment_id in covered:
                duplicate_ids.add(fragment_id)
            covered.add(fragment_id)
            if fragment_id not in live_fragment_ids:
                dead_ids.add(fragment_id)
    if dead_ids:
        raise RuntimeError(
            f"Cannot commit index segments: fragments {sorted(dead_ids)} no longer exist in the "
            "dataset (they were most likely rewritten by a concurrent compaction while the index "
            "was being built). Re-run the index build against the current dataset version."
        )
    if duplicate_ids:
        raise RuntimeError(
            f"Cannot commit index segments: fragments {sorted(duplicate_ids)} are covered by more "
            "than one segment; every fragment must be covered exactly once."
        )
    if expected_fragment_ids is not None:
        missing = set(expected_fragment_ids) - covered
        if missing:
            raise RuntimeError(
                f"Cannot commit index segments: fragments {sorted(missing)} were scheduled but are "
                "not covered by any built segment (a worker result was likely lost). Re-run the "
                "index build."
            )
