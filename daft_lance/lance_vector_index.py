from __future__ import annotations

import inspect
import logging
import math
import pickle
from functools import lru_cache
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

# The 8-bit PQ codebook trains one centroid per 2^8 codes, so its sample
# requirement is 256 rows per sampled codebook entry.
_PQ_CODEBOOK_SIZE = 256

# Keyword arguments this workflow sets explicitly on the worker's
# ``create_index_uncommitted`` call; they are not valid user kwargs here.
_HANDLER_MANAGED_KWARGS = frozenset(
    {
        "column",
        "index_type",
        "name",
        "replace",
        "train",
        "fragment_ids",
        "metric",
        "num_partitions",
        "num_sub_vectors",
        "ivf_centroids",
        "pq_codebook",
        "storage_options",
    }
)


@lru_cache(maxsize=1)
def _accepted_worker_kwargs() -> frozenset[str]:
    """Keyword arguments Lance's segment-build API actually understands.

    ``create_index_uncommitted`` ends in ``**kwargs`` and silently drops
    unknown keys (verified: a misspelled or unsupported parameter builds an
    index with the wrong configuration and no warning), so the driver
    validates user kwargs against its signature instead of forwarding blind.
    """
    params = inspect.signature(lance.LanceDataset.create_index_uncommitted).parameters
    return frozenset(params) - _HANDLER_MANAGED_KWARGS


def _validate_worker_kwargs(kwargs: dict[str, Any]) -> None:
    """Reject kwargs Lance's segment build would silently ignore."""
    if "segmented" in kwargs:
        raise TypeError(
            "The 'segmented' parameter was removed: the distributed segment-index "
            "workflow is the only code path. Remove the argument."
        )
    unknown = sorted(set(kwargs) - _accepted_worker_kwargs())
    if not unknown:
        return
    hints = {
        "distance_type": " (create_vector_index uses 'metric'; pylance's training "
        "API calls it distance_type, but this API does not accept that spelling)",
        "train": " (the distributed workflow always trains segment builds)",
    }
    detail = "; ".join(f"'{name}'{hints.get(name, '')}" for name in unknown)
    raise TypeError(
        f"Unknown keyword argument(s) for create_vector_index: {detail}. Lance's "
        f"index segment build silently ignores unknown arguments, so they are "
        f"rejected here. Accepted: {sorted(_accepted_worker_kwargs())}."
    )


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
       ``sample_rate`` is clamped down to what the dataset size supports
       (training needs ``num_partitions * sample_rate`` rows, and the 8-bit PQ
       codebook needs ``256 * sample_rate``); ``num_sub_vectors`` and
       ``num_partitions`` are derived from a supplied codebook / centroids when
       not given explicitly.
    2. Fragment batches are distributed across Daft workers, one partition per
       batch; each worker calls ``create_index_uncommitted`` with the shared
       model for its fragments and pickles the segment metadata back.
    3. The coordinator validates the collected segments against the live
       manifest (dead/overlapping/missing coverage) and commits them all in one
       ``commit_existing_index_segments`` transaction, which retires overlapped
       old segments atomically when replacing.

    ``replace`` defaults to ``False``, matching pylance's ``create_index``:
    an existing index name is refused unless ``replace=True``. Column type
    compatibility is validated by Lance's training and build APIs, not
    duplicated here. ``fragment_ids`` restricts the build to a subset of
    fragments; already-covered fragments are skipped and the remainder
    appended. Appending requires the same ``ivf_centroids`` (and
    ``pq_codebook`` for PQ variants) the existing segments were built with:
    every segment of a logical vector index must share one IVF model, or
    Lance cannot merge the segments later, so a backfill without the original
    model raises instead of silently training a divergent one.
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

    # Reject kwargs Lance's segment build would silently swallow (misspelled
    # or unsupported parameters must not build a misconfigured index).
    _validate_worker_kwargs(kwargs)

    # Validate column exists; whether it is a vector column is Lance's rule,
    # enforced by the training and build APIs with clear errors.
    try:
        field = lance_ds.schema.field(column)
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
                "Fragments %s are already covered by index '%s'; skipping them%s",
                sorted(already_covered),
                name,
                " (replace does not rebuild covered fragments; use replace=True without "
                "fragment_ids for a full rebuild)"
                if replace
                else "",
            )
        if not to_build:
            logger.info("All requested fragments are already covered by index '%s'; nothing to build.", name)
            return
        # Appended segments join a live logical index, and every segment of a
        # logical vector index must share the same IVF model: Lance's segment
        # merge refuses segments trained on different centroids (the error
        # ``optimize_indices`` would later raise). Training here would sample
        # non-deterministically and silently produce a divergent model, so the
        # original model must be supplied.
        if ivf_centroids is None:
            raise ValueError(
                f"Cannot append to index '{name}': all segments of a vector index must share "
                "one IVF model, and retraining would produce a divergent one. Pass the same "
                "ivf_centroids (and pq_codebook, for PQ variants) the existing segments were "
                "built with, or rebuild the whole index with replace=True and no fragment_ids."
            )
        if index_type in _PQ_INDEX_TYPES and pq_codebook is None:
            raise ValueError(
                f"Cannot append to PQ index '{name}' without the original pq_codebook: all "
                "segments must share one model. Pass the codebook the existing segments were "
                "built with, or rebuild the whole index with replace=True and no fragment_ids."
            )
        requested_fragment_ids = to_build

    if requested_fragment_ids is not None:
        fragments = [fragment for fragment in fragments if fragment.fragment_id in requested_fragment_ids]
    fragment_ids_to_use = sorted(
        requested_fragment_ids if requested_fragment_ids is not None else (f.fragment_id for f in fragments)
    )
    if not fragment_ids_to_use:
        raise ValueError(f"Dataset at {open_context.uri} contains no fragments")

    # Validate grouping before any training work so argument errors surface first.
    if fragment_group_size is None:
        fragment_group_size = 10
    elif fragment_group_size <= 0:
        raise ValueError("fragment_group_size must be positive")

    # Phase 1: train the global model once on the driver so all segments
    # share the same centroids (and codebook) and commit as one logical index.
    num_rows = lance_ds.count_rows()
    effective_sample_rate = sample_rate
    needs_ivf_training = ivf_centroids is None
    needs_pq_training = index_type in _PQ_INDEX_TYPES and pq_codebook is None
    if needs_ivf_training or needs_pq_training:
        # Clamp the sample rate to what this dataset can support, mirroring
        # Lance's own training requirements: num_partitions * sample_rate rows
        # for IVF (Lance derives num_partitions as sqrt(num_rows) when None)
        # and 256 * sample_rate rows for the 8-bit PQ codebook.
        effective_partitions = num_partitions if num_partitions is not None else max(1, round(math.sqrt(num_rows)))
        caps = [max(1, num_rows // effective_partitions)]
        if needs_pq_training:
            caps.append(max(1, num_rows // _PQ_CODEBOOK_SIZE))
        clamped = min([sample_rate, *caps])
        if clamped < sample_rate:
            logger.warning(
                "sample_rate %d exceeds what the dataset supports (%d rows); clamping to %d",
                sample_rate,
                num_rows,
                clamped,
            )
            effective_sample_rate = clamped

    builder = lance.indices.IndicesBuilder(lance_ds, column)
    ivf_model: lance.indices.IvfModel | None = None
    if needs_ivf_training:
        logger.info(
            "Phase 1: training IVF centroids (index_type=%s, metric=%s, num_partitions=%s, sample_rate=%d)",
            index_type,
            metric,
            num_partitions,
            effective_sample_rate,
        )
        ivf_model = builder.train_ivf(
            num_partitions=num_partitions,
            distance_type=metric.lower(),
            sample_rate=effective_sample_rate,
        )
        ivf_centroids = ivf_model.centroids
        num_partitions = ivf_model.num_partitions
        logger.info("IVF training completed: num_partitions=%d", num_partitions)

    if needs_pq_training:
        logger.info(
            "Phase 1: training PQ codebook (num_sub_vectors=%s, sample_rate=%d)", num_sub_vectors, effective_sample_rate
        )
        if ivf_model is None:
            # Caller-supplied centroids: wrap them so train_pq can partition
            # its samples with the same model the segments will be built with.
            ivf_model = lance.indices.IvfModel(ivf_centroids, metric.lower())
        pq_model = builder.train_pq(
            ivf_model,
            num_subvectors=num_sub_vectors,
            sample_rate=effective_sample_rate,
        )
        pq_codebook = pq_model.codebook
        num_sub_vectors = pq_model.num_subvectors
        logger.info("PQ training completed: num_sub_vectors=%d", num_sub_vectors)

    # Derive the model shape from supplied artifacts when not given: the
    # partition count is the number of centroids, and the sub-vector count
    # follows from the codebook's entry size versus the column dimension.
    if num_partitions is None and ivf_centroids is not None:
        num_partitions = len(ivf_centroids)
    if num_sub_vectors is None and pq_codebook is not None:
        dimension = getattr(field.type, "list_size", None)
        subvector_size = getattr(pq_codebook.type, "list_size", None)
        if dimension and subvector_size and dimension % subvector_size == 0:
            num_sub_vectors = dimension // subvector_size
        else:
            raise ValueError(
                f"Cannot derive num_sub_vectors from the supplied pq_codebook "
                f"(column dimension {dimension}, codebook entry size {subvector_size}); "
                "pass num_sub_vectors explicitly."
            )

    model_kwargs: dict[str, Any] = {
        "metric": metric,
        "ivf_centroids": ivf_centroids,
        "num_partitions": num_partitions,
        "num_sub_vectors": num_sub_vectors,
        "pq_codebook": pq_codebook,
    }

    # Phase 2: distribute fragment batches across Daft workers, one partition
    # per batch so distributed runners build the segments in parallel.
    if fragment_group_size > len(fragment_ids_to_use):
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
        df = from_pylist(fragment_data).into_partitions(len(fragment_data))
        df = df.select(handler(df["fragment_ids"]).alias("index_meta"))
        collected = df.collect()

    index_metas = [pickle.loads(raw) for raw in collected.to_pydict()["index_meta"]]

    # Phase 3: validate against the live manifest and commit atomically.
    lance_ds = open_context.open_latest()
    _validate_segments_against_manifest(lance_ds, index_metas, fragment_ids_to_use)

    logger.info("Collected %d vector index segments; committing as segmented index %s", len(index_metas), name)
    lance_ds.commit_existing_index_segments(name, column, index_metas)
    logger.info("Vector index %s committed successfully", name)
