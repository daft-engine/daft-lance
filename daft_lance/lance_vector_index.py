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
from daft_lance.lance_scalar_index import (
    _existing_index_coverage,
    _validate_segments_against_manifest,
)
from daft_lance.utils import distribute_fragments_balanced

logger = logging.getLogger(__name__)

# Vector index types built with the distributed segment workflow. IVF and
# IVF_HNSW families over FLAT/PQ/SQ storage.
VECTOR_INDEX_TYPES = frozenset({"IVF_FLAT", "IVF_PQ", "IVF_SQ", "IVF_HNSW_FLAT", "IVF_HNSW_PQ", "IVF_HNSW_SQ"})

# PQ variants need a codebook trained alongside the IVF centroids.
_PQ_INDEX_TYPES = frozenset({"IVF_PQ", "IVF_HNSW_PQ"})


class VectorFragmentIndexHandler:
    """Handler for distributed vector index segment creation on fragment batches.

    Each Daft worker receives a subset of Lance fragment IDs and builds one
    uncommitted vector index segment for just those fragments with Lance's
    public ``create_index_uncommitted`` API, using the IVF centroids (and PQ
    codebook, for PQ variants) trained once on the driver so every segment
    shares the same model. The worker pickles the returned ``lance.Index``
    metadata back to the coordinator, which commits all segments into the
    dataset manifest atomically with ``commit_existing_index_segments``.
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
        """Build one uncommitted vector index segment and return pickled metadata."""
        logger.info(
            "Building vector index segment for fragments %s (column=%s, type=%s)",
            fragment_ids,
            self.column,
            self.index_type,
        )
        # Segment creation uses ``replace=False`` unless the driver opts workers
        # into ``replace=True`` for a same-name rebuild: the worker's pinned
        # snapshot still contains the index, and Lance rejects building against
        # an existing name with ``replace=False``. Replacement itself happens
        # in the coordinator's single manifest commit, which retires overlapped
        # old segments atomically.
        index_meta = self._dataset().create_index_uncommitted(
            column=self.column,
            index_type=self.index_type,
            name=self.name,
            replace=self.replace,
            train=True,
            fragment_ids=fragment_ids,
            **self.kwargs,
        )
        return pickle.dumps(index_meta)


def create_vector_index_internal(
    lance_ds: lance.LanceDataset,
    open_context: DatasetOpenContext,
    *,
    column: str,
    index_type: str = "IVF_PQ",
    name: str | None = None,
    replace: bool = False,
    metric: str = "L2",
    num_partitions: int | None = None,
    num_sub_vectors: int | None = None,
    sample_rate: int = 256,
    ivf_centroids: pa.Array | None = None,
    pq_codebook: pa.Array | None = None,
    fragment_group_size: int | None = None,
    max_concurrency: int | None = None,
    fragment_ids: list[int] | None = None,
    **kwargs: Any,
) -> None:
    """Internal implementation of distributed vector index creation.

    ``lance_ds`` is the driver's live dataset (planning, training, commits);
    ``open_context`` is the serializable handle workers reopen from and the
    single source of uri, storage options and namespace kwargs.

    The build has three phases:

    1. The driver trains the global model once — IVF centroids via
       ``IndicesBuilder.train_ivf`` and, for PQ variants, the PQ codebook via
       ``IndicesBuilder.train_pq`` — unless the caller supplies pre-trained
       ``ivf_centroids`` / ``pq_codebook``. One shared model across segments is
       what lets independently built segments commit as one logical index.
    2. Fragment batches are distributed across Daft workers; each worker calls
       ``create_index_uncommitted`` with the shared model for its fragments and
       pickles the segment metadata back.
    3. The coordinator validates the collected segments against the live
       manifest (dead/overlapping/missing coverage) and commits them all in one
       ``commit_existing_index_segments`` transaction, which retires overlapped
       old segments atomically when replacing.

    ``replace`` defaults to ``False``, matching pylance's ``create_index``:
    an existing index name is refused unless ``replace=True``. Column type
    compatibility is validated by Lance's training and build APIs, not
    duplicated here. ``fragment_ids`` restricts the build to a subset of
    fragments; already-covered fragments are skipped, so a partial build
    followed by a backfill is the incremental path (same semantics as the
    scalar index workflow).
    """
    if not column:
        raise ValueError("Column name cannot be empty")

    index_type = index_type.upper()
    if index_type not in VECTOR_INDEX_TYPES:
        raise ValueError(
            f"Unsupported distributed vector index type '{index_type}'. Supported types: "
            f"{sorted(VECTOR_INDEX_TYPES)}. For scalar index types use "
            f"daft_lance.create_scalar_index."
        )

    if sample_rate <= 0:
        raise ValueError(f"sample_rate must be positive, got {sample_rate}")

    # Validate column exists; whether it is a vector column is Lance's rule,
    # enforced by the training and build APIs with clear errors.
    try:
        lance_ds.schema.field(column)
    except KeyError as e:
        available_columns = [field.name for field in lance_ds.schema]
        raise ValueError(f"Column '{column}' not found. Available: {available_columns}") from e

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
        # Column/model compatibility of a same-name index is validated by
        # Lance's build and commit APIs, not duplicated here.
        if not replace and requested_fragment_ids is None:
            raise ValueError(f"Index with name '{name}' already exists. Set replace=True to replace it.")

    # Workers open the pinned snapshot where a same-name index still exists;
    # building against that name always requires replace=True. The actual
    # replacement happens in the coordinator's atomic commit.
    handler_replace = existing_coverage is not None
    if existing_coverage is not None and requested_fragment_ids is not None:
        # Incremental backfill: skip fragments already covered by committed
        # segments; only the remainder is built and appended.
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
    if not fragment_ids_to_use:
        raise ValueError(f"Dataset at {open_context.uri} contains no fragments")

    # Phase 1: train the global model once on the driver so all segments
    # share the same centroids (and codebook) and commit as one logical index.
    builder = lance.indices.IndicesBuilder(lance_ds, column)
    ivf_model: lance.indices.IvfModel | None = None
    if ivf_centroids is None:
        logger.info(
            "Phase 1: training IVF centroids (index_type=%s, metric=%s, num_partitions=%s, sample_rate=%s)",
            index_type,
            metric,
            num_partitions,
            sample_rate,
        )
        ivf_model = builder.train_ivf(
            num_partitions=num_partitions,
            distance_type=metric.lower(),
            sample_rate=sample_rate,
        )
        ivf_centroids = ivf_model.centroids
        num_partitions = ivf_model.num_partitions
        logger.info("IVF training completed: num_partitions=%d", num_partitions)

    if index_type in _PQ_INDEX_TYPES and pq_codebook is None:
        logger.info("Phase 1: training PQ codebook (num_sub_vectors=%s, sample_rate=%s)", num_sub_vectors, sample_rate)
        if ivf_model is None:
            # Caller-supplied centroids: wrap them so train_pq can partition
            # its samples with the same model the segments will be built with.
            ivf_model = lance.indices.IvfModel(ivf_centroids, metric.lower())
        pq_model = builder.train_pq(
            ivf_model,
            num_subvectors=num_sub_vectors,
            sample_rate=sample_rate,
        )
        pq_codebook = pq_model.codebook
        num_sub_vectors = pq_model.num_subvectors
        logger.info("PQ training completed: num_sub_vectors=%d", num_sub_vectors)

    if ivf_centroids is None:
        raise ValueError("ivf_centroids must be provided or trainable for IVF-based distributed vector indices")
    if index_type in _PQ_INDEX_TYPES and pq_codebook is None:
        raise ValueError("pq_codebook must be provided or trainable for PQ-based distributed vector indices")

    model_kwargs: dict[str, Any] = {"metric": metric}
    if num_partitions is not None:
        model_kwargs["num_partitions"] = num_partitions
    if num_sub_vectors is not None:
        model_kwargs["num_sub_vectors"] = num_sub_vectors
    if ivf_centroids is not None:
        model_kwargs["ivf_centroids"] = ivf_centroids
    if pq_codebook is not None:
        model_kwargs["pq_codebook"] = pq_codebook

    # Phase 2: distribute fragment batches across Daft workers.
    if fragment_group_size is None:
        fragment_group_size = 10
    elif fragment_group_size <= 0:
        raise ValueError("fragment_group_size must be positive")

    if fragment_group_size > len(fragment_ids_to_use) and fragment_ids_to_use:
        fragment_group_size = len(fragment_ids_to_use)
        logger.info("Adjusted fragment_group_size to %d to match fragment count", fragment_group_size)

    fragment_data = distribute_fragments_balanced(fragments, fragment_group_size)
    if not fragment_data:
        raise ValueError(f"Dataset at {open_context.uri} contains no fragments")

    logger.info(
        "Phase 2: building vector index segments across %d fragment batches "
        "(column=%s, type=%s, name=%s, fragments=%d)",
        len(fragment_data),
        column,
        index_type,
        name,
        len(fragment_ids_to_use),
    )
    handler_cls = daft.cls(
        VectorFragmentIndexHandler,
        max_concurrency=max_concurrency,
    )
    handler = handler_cls(
        open_context=open_context,
        column=column,
        index_type=index_type,
        name=name,
        replace=handler_replace,
        **model_kwargs,
        **kwargs,
    )

    with execution_config_ctx(maintain_order=False):
        df = from_pylist(fragment_data)
        df = df.select(handler(df["fragment_ids"]).alias("index_meta"))
        collected = df.collect()

    index_metas = [pickle.loads(raw) for raw in collected.to_pydict()["index_meta"]]

    # Phase 3: validate against the live manifest and commit atomically.
    lance_ds = open_context.open_latest()
    _validate_segments_against_manifest(lance_ds, index_metas, fragment_ids_to_use)

    logger.info("Collected %d vector index segments; committing as segmented index %s", len(index_metas), name)
    lance_ds.commit_existing_index_segments(name, column, index_metas)
    logger.info("Vector index %s committed successfully", name)
