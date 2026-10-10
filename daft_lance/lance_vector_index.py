from __future__ import annotations

import inspect
import logging
import pickle
from functools import lru_cache
from typing import TYPE_CHECKING, Any

import daft
from daft import execution_config_ctx, from_pylist

if TYPE_CHECKING:
    from daft_lance.namespace import DatasetOpenContext

import lance

from daft_lance.lance_scalar_index import (
    _validate_segments_against_manifest,
)
from daft_lance.utils import distribute_fragments_balanced

logger = logging.getLogger(__name__)

# Vector index types built with the distributed segment workflow. IVF and
# IVF_HNSW families over FLAT/PQ/SQ storage.
VECTOR_INDEX_TYPES = frozenset({"IVF_FLAT", "IVF_PQ", "IVF_SQ", "IVF_HNSW_FLAT", "IVF_HNSW_PQ", "IVF_HNSW_SQ"})

# PQ variants need a codebook trained alongside the IVF centroids.
_PQ_INDEX_TYPES = frozenset({"IVF_PQ", "IVF_HNSW_PQ"})

# Arguments managed by this workflow or incompatible with its shared-model
# builds; they are not valid user kwargs on the worker's segment-build call.
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
        "num_bits",
        "ivf_centroids",
        "pq_codebook",
        "storage_options",
        "index_uuid",
        "ivf_centroids_file",
    }
)


@lru_cache(maxsize=1)
def _accepted_worker_kwargs() -> frozenset[str]:
    """Keyword arguments Lance's segment-build API actually understands.

    ``create_index_uncommitted`` ends in ``**kwargs`` and silently drops
    unknown keys (verified: a misspelled or unsupported parameter builds an
    index with the wrong configuration and no warning), so the driver
    validates explicit parameters and the HNSW options parsed from ``**kwargs``.
    """
    params = inspect.signature(lance.LanceDataset.create_index_uncommitted).parameters
    return (frozenset(params) | {"m", "max_level", "ef_construction"}) - _HANDLER_MANAGED_KWARGS


def _validate_worker_kwargs(kwargs: dict[str, Any]) -> None:
    """Reject kwargs Lance's segment build would silently ignore."""
    if "fragment_ids" in kwargs:
        raise TypeError(
            "create_vector_index no longer accepts fragment_ids; it builds all fragments. "
            "Use optimize_indices to index appended data."
        )
    removed_models = sorted(set(kwargs) & {"ivf_centroids", "pq_codebook", "ivf_centroids_file"})
    if removed_models:
        raise TypeError(
            f"create_vector_index does not accept {removed_models}; models are trained by the build workflow."
        )
    if "segmented" in kwargs:
        raise TypeError(
            "The 'segmented' parameter was removed: the distributed segment-index "
            "workflow is the only code path. Remove the argument."
        )
    if "index_uuid" in kwargs:
        raise TypeError("index_uuid is managed per segment; each worker must generate a unique UUID.")
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


def _index_exists(dataset: lance.LanceDataset, name: str) -> bool:
    return any(index.name == name for index in dataset.describe_indices())


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
        # Vector builds mutably borrow the Lance handle; concurrent invocations
        # must open independent handles to the same pinned snapshot.
        index_meta = self.open_context.open_pinned().create_index_uncommitted(
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
    num_bits: int = 8,
    sample_rate: int = 256,
    fragment_group_size: int | None = None,
    max_concurrency: int | None = None,
    **kwargs: Any,
) -> None:
    """Internal implementation of distributed vector index creation.

    ``lance_ds`` is the driver's live dataset (planning, training, commits);
    ``open_context`` is the serializable handle workers reopen from and the
    single source of uri, storage options and namespace kwargs.

    The build has three phases:

    1. The driver trains the global model once — IVF centroids via
       ``IndicesBuilder.train_ivf`` and, for PQ variants, the PQ codebook via
       ``IndicesBuilder.train_pq``. The user's ``sample_rate`` is passed
       unchanged; Lance validates that enough training data is available.
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
    duplicated here. Every fragment in the opened dataset snapshot is indexed.
    Incremental coverage of appended data belongs to ``optimize_indices``.
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
    if index_type not in _PQ_INDEX_TYPES and num_bits != 8:
        raise ValueError("num_bits is configurable only for IVF_PQ and IVF_HNSW_PQ.")

    # Reject kwargs Lance's segment build would silently swallow (misspelled
    # or unsupported parameters must not build a misconfigured index).
    _validate_worker_kwargs(kwargs)

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

    index_exists = _index_exists(lance_ds, name)
    if index_exists and not replace:
        raise ValueError(f"Index with name '{name}' already exists. Set replace=True to replace it.")

    # Each worker builds against the pinned snapshot. Replacement is committed
    # once, after every fragment batch has succeeded.
    handler_replace = index_exists
    fragments = lance_ds.get_fragments()
    fragment_ids_to_use = sorted(fragment.fragment_id for fragment in fragments)
    if not fragment_ids_to_use:
        raise ValueError(f"Dataset at {open_context.uri} contains no fragments")

    # Validate grouping before any training work so argument errors surface first.
    if fragment_group_size is None:
        fragment_group_size = 10
    elif fragment_group_size <= 0:
        raise ValueError("fragment_group_size must be positive")

    # Phase 1: train the global model once on the driver so all segments
    # share the same centroids (and codebook) and commit as one logical index.
    builder = lance.indices.IndicesBuilder(lance_ds, column)
    logger.info(
        "Phase 1: training IVF centroids (index_type=%s, metric=%s, num_partitions=%s, sample_rate=%d)",
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

    pq_codebook = None
    if index_type in _PQ_INDEX_TYPES:
        logger.info("Phase 1: training PQ codebook (num_sub_vectors=%s, sample_rate=%d)", num_sub_vectors, sample_rate)
        pq_model = builder.train_pq(
            ivf_model,
            num_subvectors=num_sub_vectors,
            num_bits=num_bits,
            sample_rate=sample_rate,
        )
        pq_codebook = pq_model.codebook
        num_sub_vectors = pq_model.num_subvectors
        logger.info("PQ training completed: num_sub_vectors=%d", num_sub_vectors)

    model_kwargs: dict[str, Any] = {
        "metric": metric,
        "ivf_centroids": ivf_centroids,
        "num_partitions": num_partitions,
        "num_sub_vectors": num_sub_vectors,
        "num_bits": num_bits,
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
    if not replace and _index_exists(lance_ds, name):
        raise ValueError(f"Index '{name}' changed during the build; retry against the latest dataset version.")
    if replace:
        indexed_fragments = {
            fid
            for index in lance_ds.describe_indices()
            if index.name == name
            for segment in index.segments
            for fid in segment.fragment_ids
        }
        live_fragments = {fragment.fragment_id for fragment in lance_ds.get_fragments()}
        if (indexed_fragments & live_fragments).difference(fragment_ids_to_use):
            raise ValueError(
                f"Index '{name}' gained coverage outside the build plan; retry against the latest dataset version."
            )
    _validate_segments_against_manifest(lance_ds, index_metas, fragment_ids_to_use)

    # Keep the checked handle: a later same-name CreateIndex transaction then
    # conflicts in Lance instead of being treated as an index to replace.
    logger.info("Collected %d vector index segments; committing as segmented index %s", len(index_metas), name)
    lance_ds.commit_existing_index_segments(name, column, index_metas)
    logger.info("Vector index %s committed successfully", name)
