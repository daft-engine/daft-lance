from __future__ import annotations

import warnings
from pathlib import Path

import lance
import numpy as np
import pytest
from lance.indices import IndicesBuilder

from daft.dependencies import pa
from daft_lance import create_vector_index, optimize_indices

warnings.filterwarnings("ignore", category=DeprecationWarning, module="lance")

ALL_VECTOR_INDEX_TYPES = ["IVF_FLAT", "IVF_PQ", "IVF_SQ", "IVF_HNSW_FLAT", "IVF_HNSW_PQ", "IVF_HNSW_SQ"]


def _make_vector_dataset(
    path: Path,
    num_rows: int = 2048,
    dim: int = 8,
    rows_per_file: int = 256,
    seed: int = 42,
) -> tuple[str, np.ndarray]:
    """Create a vector dataset and return (uri, vectors) for ground-truth checks."""
    rng = np.random.default_rng(seed)
    vectors = rng.standard_normal((num_rows, dim)).astype(np.float32)
    table = pa.table(
        {
            "id": list(range(num_rows)),
            "vector": pa.FixedSizeListArray.from_arrays(vectors.reshape(-1), dim),
        }
    )
    lance.write_dataset(table, str(path), mode="create", max_rows_per_file=rows_per_file)
    return str(path), vectors


def _covered_fragments(uri: str, index_name: str) -> set[int]:
    for desc in lance.dataset(uri).describe_indices():
        if desc.name == index_name:
            covered: set[int] = set()
            for segment in desc.segments:
                covered.update(segment.fragment_ids)
            return covered
    return set()


def test_ivf_flat_multi_segment_build_and_search(tmp_path: Path) -> None:
    """IVF_FLAT builds one segment per fragment group; search returns the query point."""
    uri, vectors = _make_vector_dataset(tmp_path / "flat.lance", num_rows=240, rows_per_file=40, seed=1)

    # 6 fragments in groups of 2 -> 3 segments built by separate tasks.
    create_vector_index(
        uri, column="vector", index_type="IVF_FLAT", num_partitions=4, sample_rate=32, fragment_group_size=2
    )

    ds = lance.dataset(uri)
    described = ds.describe_indices()
    assert len(described) == 1
    desc = described[0]
    assert desc.name == "vector_idx"
    assert desc.index_type == "IVF_FLAT"
    assert desc.num_rows_indexed == 240
    assert len(desc.segments) == 3

    results = ds.to_table(nearest={"column": "vector", "q": vectors[7], "k": 5})
    assert results.num_rows == 5
    assert 7 in results["id"].to_pylist()


def test_ivf_pq_shared_model_multi_segment(tmp_path: Path) -> None:
    """IVF_PQ trains one shared centroid+codebook pair; segments commit as one index."""
    uri, vectors = _make_vector_dataset(tmp_path / "pq.lance")

    create_vector_index(
        uri,
        column="vector",
        index_type="IVF_PQ",
        num_partitions=8,
        num_sub_vectors=2,
        sample_rate=8,
        fragment_group_size=4,
    )

    ds = lance.dataset(uri)
    desc = ds.describe_indices()[0]
    assert desc.index_type == "IVF_PQ"
    assert desc.num_rows_indexed == 2048
    assert len(desc.segments) == 2

    assert _recall_at_5(ds, vectors) >= 0.5


# Lossy compression keeps less of the true top-5; exact-membership assertions
# on a lossy index flake across platforms, so each family gets a recall floor
# calibrated with headroom over measured values (FLAT 1.00, SQ 0.95, PQ 0.42+).
_MIN_RECALL = {
    "IVF_FLAT": 0.9,
    "IVF_HNSW_FLAT": 0.9,
    "IVF_SQ": 0.8,
    "IVF_HNSW_SQ": 0.8,
    "IVF_PQ": 0.2,
    "IVF_HNSW_PQ": 0.2,
}


def _recall_at_5(ds, vectors: np.ndarray) -> float:
    """Recall@5 of the index against brute-force ground truth, over fixed queries."""
    queries = list(range(0, 400, 40)) + [7]
    hits = 0
    for qi in queries:
        truth = set(np.argsort(((vectors - vectors[qi]) ** 2).sum(axis=1))[:5].tolist())
        got = set(ds.to_table(nearest={"column": "vector", "q": vectors[qi], "k": 5})["id"].to_pylist())
        assert len(got) == 5
        hits += len(truth & got)
    return hits / (5 * len(queries))


@pytest.mark.parametrize("index_type", ALL_VECTOR_INDEX_TYPES)
def test_every_index_type_builds_and_answers_queries(tmp_path: Path, index_type: str) -> None:
    """All six advertised vector index types build multi-segment and return results."""
    uri, vectors = _make_vector_dataset(tmp_path / f"t_{index_type}.lance", seed=13)

    create_vector_index(
        uri, column="vector", index_type=index_type, num_partitions=4, sample_rate=8, fragment_group_size=4
    )

    ds = lance.dataset(uri)
    desc = ds.describe_indices()[0]
    assert desc.index_type == index_type
    assert desc.num_rows_indexed == 2048
    assert len(desc.segments) == 2

    recall = _recall_at_5(ds, vectors)
    assert recall >= _MIN_RECALL[index_type], f"{index_type} recall@5 {recall:.2f} < {_MIN_RECALL[index_type]}"


def test_ivf_sq_multi_segment_queryable(tmp_path: Path) -> None:
    """IVF_SQ also builds multi-segment; recall stays comparable to a single-segment build."""
    uri, vectors = _make_vector_dataset(tmp_path / "sq.lance", seed=7)

    create_vector_index(
        uri, column="vector", index_type="IVF_SQ", num_partitions=8, sample_rate=8, fragment_group_size=4
    )

    ds = lance.dataset(uri)
    desc = ds.describe_indices()[0]
    assert desc.index_type == "IVF_SQ"
    assert len(desc.segments) == 2

    hits = 0
    queries = list(range(0, 400, 40))
    for qi in queries:
        truth = set(np.argsort(((vectors - vectors[qi]) ** 2).sum(axis=1))[:5].tolist())
        got = set(ds.to_table(nearest={"column": "vector", "q": vectors[qi], "k": 5})["id"].to_pylist())
        hits += len(truth & got)
    # SQ is a lossy compression; a healthy build keeps most of the true top-5.
    assert hits >= 0.8 * 5 * len(queries)


def test_cosine_metric_orders_by_angle_not_distance(tmp_path: Path) -> None:
    """metric='cosine' must order by cosine similarity, provably not by L2 distance."""
    rng = np.random.default_rng(5)
    dim = 8
    u = rng.standard_normal(dim).astype(np.float32)
    u /= np.linalg.norm(u)
    v = rng.standard_normal(dim).astype(np.float32)
    v -= v.dot(u) * u  # orthogonal direction to u
    v /= np.linalg.norm(v)

    vectors = np.stack(
        [u * (9.0 + (i % 10)) if i % 2 == 0 else v * (0.5 + (i % 10) * 0.01) for i in range(400)]
    ).astype(np.float32)
    table = pa.table(
        {
            "id": list(range(400)),
            "vector": pa.FixedSizeListArray.from_arrays(vectors.reshape(-1), dim),
        }
    )
    uri = str(tmp_path / "cosine.lance")
    lance.write_dataset(table, uri, mode="create", max_rows_per_file=100)

    create_vector_index(uri, column="vector", index_type="IVF_FLAT", metric="cosine", num_partitions=2, sample_rate=32)

    query = u.astype(np.float32)
    got = set(lance.dataset(uri).to_table(nearest={"column": "vector", "q": query, "k": 5})["id"].to_pylist())
    # Even ids are u-direction rows (cosine similarity 1.0); odd ids are v-direction.
    cosine_truth = {i for i in range(400) if i % 2 == 0}
    l2_truth = set(np.argsort(((vectors - query) ** 2).sum(axis=1))[:5].tolist())
    # Sanity: on this dataset the two ground truths genuinely differ.
    assert all(i % 2 == 1 for i in l2_truth)
    # The index must follow the cosine ground truth, not the L2 one.
    assert got <= cosine_truth
    assert got.isdisjoint(l2_truth)


def test_small_dataset_default_sample_rate_is_clamped(tmp_path: Path) -> None:
    """Default sample_rate=256 would demand 65k rows; it must clamp instead of failing."""
    uri, vectors = _make_vector_dataset(tmp_path / "clamp.lance", num_rows=8192, rows_per_file=512, seed=17)

    # No explicit sample_rate: training needs 16*256 (IVF) and 256*256 (PQ) rows
    # unclamped — far more than the dataset has.
    create_vector_index(uri, column="vector", index_type="IVF_PQ", num_partitions=16, fragment_group_size=2)

    desc = lance.dataset(uri).describe_indices()[0]
    assert desc.index_type == "IVF_PQ"
    assert desc.num_rows_indexed == 8192
    # The clamp makes the build succeed; recall quality under a heavily clamped
    # sample is the lossy-compression tradeoff, guarded by the PQ-specific
    # recall tests, so only assert the index answers queries here.
    results = lance.dataset(uri).to_table(nearest={"column": "vector", "q": vectors[7], "k": 5})
    assert results.num_rows == 5


def test_pretrained_centroids_are_used(tmp_path: Path) -> None:
    """Caller-supplied ivf_centroids skip driver-side training and still build."""
    uri, _ = _make_vector_dataset(tmp_path / "pre.lance", num_rows=240, rows_per_file=40, seed=3)

    centroids = (
        IndicesBuilder(lance.dataset(uri), "vector")
        .train_ivf(num_partitions=4, distance_type="l2", sample_rate=32)
        .centroids
    )

    create_vector_index(
        uri,
        column="vector",
        index_type="IVF_FLAT",
        ivf_centroids=centroids,
        fragment_group_size=2,
    )

    desc = lance.dataset(uri).describe_indices()[0]
    assert desc.index_type == "IVF_FLAT"
    assert desc.num_rows_indexed == 240


def test_pq_codebook_without_num_sub_vectors_derives_it(tmp_path: Path) -> None:
    """A supplied codebook implies num_sub_vectors; no deep worker error."""
    uri, _ = _make_vector_dataset(tmp_path / "codebook.lance", seed=19)
    ds = lance.dataset(uri)
    builder = IndicesBuilder(ds, "vector")
    ivf = builder.train_ivf(num_partitions=4, distance_type="l2", sample_rate=32)
    pq = builder.train_pq(ivf, num_subvectors=2, sample_rate=8)

    create_vector_index(
        uri,
        column="vector",
        index_type="IVF_PQ",
        ivf_centroids=ivf.centroids,
        pq_codebook=pq.codebook,
        fragment_group_size=4,
    )

    desc = lance.dataset(uri).describe_indices()[0]
    assert desc.index_type == "IVF_PQ"
    assert desc.num_rows_indexed == 2048


def test_backfill_requires_the_original_shared_model(tmp_path: Path) -> None:
    """Appending segments without the original centroids would fork the IVF model."""
    uri, _ = _make_vector_dataset(tmp_path / "guard.lance")
    build = dict(column="vector", index_type="IVF_FLAT", num_partitions=4, sample_rate=8, name="v_idx")

    create_vector_index(uri, fragment_ids=[0, 1, 2, 3], **build)

    # Retraining on the backfill would produce a divergent model — refused.
    with pytest.raises(ValueError, match="ivf_centroids"):
        create_vector_index(uri, fragment_ids=[4, 5], **build)


def test_backfill_with_shared_model_then_optimize_indices_merges(tmp_path: Path) -> None:
    """A same-model backfill stays mergeable by optimize_indices (shared-centroid invariant)."""
    uri, vectors = _make_vector_dataset(tmp_path / "backfill.lance")
    centroids = (
        IndicesBuilder(lance.dataset(uri), "vector")
        .train_ivf(num_partitions=4, distance_type="l2", sample_rate=8)
        .centroids
    )
    build = dict(column="vector", index_type="IVF_FLAT", ivf_centroids=centroids, name="v_idx")

    create_vector_index(uri, fragment_ids=[0, 1, 2, 3], **build)
    assert _covered_fragments(uri, "v_idx") == {0, 1, 2, 3}

    # Backfill appends coverage for the remaining fragments.
    create_vector_index(uri, fragment_ids=[1, 4, 5, 6, 7], **build)
    assert _covered_fragments(uri, "v_idx") == {0, 1, 2, 3, 4, 5, 6, 7}

    # Segments built from one model must remain mergeable.
    optimize_indices(uri, indices=["v_idx"], num_indices_to_merge=2)
    assert _covered_fragments(uri, "v_idx") == {0, 1, 2, 3, 4, 5, 6, 7}
    assert 7 in lance.dataset(uri).to_table(nearest={"column": "vector", "q": vectors[7], "k": 5})["id"].to_pylist()

    # A fully covered request is a no-op: version stays unchanged.
    version_before = lance.dataset(uri).version
    create_vector_index(uri, fragment_ids=[2, 3], **build)
    assert lance.dataset(uri).version == version_before


def test_replace_default_false_rejects_existing_name(tmp_path: Path) -> None:
    uri, _ = _make_vector_dataset(tmp_path / "repl.lance", num_rows=240, rows_per_file=40, seed=5)
    create_vector_index(uri, column="vector", index_type="IVF_FLAT", num_partitions=4, sample_rate=32, name="v_idx")

    with pytest.raises(ValueError, match="already exists. Set replace=True"):
        create_vector_index(uri, column="vector", index_type="IVF_FLAT", num_partitions=4, sample_rate=32, name="v_idx")


def test_replace_true_rebuilds_atomically_in_one_version(tmp_path: Path) -> None:
    uri, vectors = _make_vector_dataset(tmp_path / "rebuild.lance", num_rows=240, rows_per_file=40, seed=6)
    build = dict(
        column="vector",
        index_type="IVF_FLAT",
        num_partitions=4,
        sample_rate=32,
        name="v_idx",
        fragment_group_size=2,
    )
    create_vector_index(uri, **build)
    version_before = lance.dataset(uri).version

    create_vector_index(uri, replace=True, **build)

    ds = lance.dataset(uri)
    assert ds.version == version_before + 1, "a successful replace must commit exactly one new version"
    described = ds.describe_indices()
    assert len(described) == 1
    assert described[0].name == "v_idx"
    assert len(described[0].segments) == 3
    results = ds.to_table(nearest={"column": "vector", "q": vectors[7], "k": 5})
    assert 7 in results["id"].to_pylist()


def test_worker_failure_keeps_version_and_old_index(tmp_path: Path) -> None:
    """A failing segment build aborts before the commit; the old index stays usable."""
    uri, vectors = _make_vector_dataset(tmp_path / "fail.lance", num_rows=240, rows_per_file=40, seed=9)
    create_vector_index(
        uri,
        column="vector",
        index_type="IVF_FLAT",
        num_partitions=4,
        sample_rate=32,
        name="v_idx",
        fragment_group_size=2,
    )
    ds = lance.dataset(uri)
    version_before = ds.version

    original = lance.LanceDataset.create_index_uncommitted

    def fail_one_batch(self, *args, **kwargs):
        fragment_ids = kwargs.get("fragment_ids")
        if fragment_ids and 4 in fragment_ids:
            raise RuntimeError("injected segment build failure")
        return original(self, *args, **kwargs)

    lance.LanceDataset.create_index_uncommitted = fail_one_batch
    try:
        with pytest.raises(Exception, match="injected segment build failure"):
            create_vector_index(
                uri,
                column="vector",
                index_type="IVF_FLAT",
                num_partitions=4,
                sample_rate=32,
                name="v_idx",
                fragment_group_size=2,
                replace=True,
            )
    finally:
        lance.LanceDataset.create_index_uncommitted = original

    ds = lance.dataset(uri)
    assert ds.version == version_before
    described = ds.describe_indices()
    assert len(described) == 1
    assert described[0].name == "v_idx"
    assert len(described[0].segments) == 3
    results = ds.to_table(nearest={"column": "vector", "q": vectors[7], "k": 5})
    assert 7 in results["id"].to_pylist()


def test_unknown_kwargs_are_rejected(tmp_path: Path) -> None:
    """Lance's segment build silently ignores unknown kwargs; this API must not."""
    uri, _ = _make_vector_dataset(tmp_path / "kwargs.lance", num_rows=240, rows_per_file=40, seed=21)

    with pytest.raises(TypeError, match="totally_bogus_param"):
        create_vector_index(uri, column="vector", index_type="IVF_FLAT", totally_bogus_param=1)

    # pylance's training API spells it distance_type; here it must be 'metric'
    # or the build would silently be L2.
    with pytest.raises(TypeError, match="'distance_type'.*'metric'"):
        create_vector_index(uri, column="vector", index_type="IVF_FLAT", distance_type="cosine")

    with pytest.raises(TypeError, match="'train'"):
        create_vector_index(uri, column="vector", index_type="IVF_FLAT", train=False)

    with pytest.raises(TypeError, match="'segmented' parameter was removed"):
        create_vector_index(uri, column="vector", index_type="IVF_FLAT", segmented=True)

    # A real build kwarg still passes through.
    create_vector_index(uri, column="vector", index_type="IVF_FLAT", num_partitions=4, target_partition_size=1024)


def test_invalid_arguments_raise(tmp_path: Path) -> None:
    uri, _ = _make_vector_dataset(tmp_path / "bad.lance", num_rows=240, rows_per_file=40, seed=11)

    with pytest.raises(ValueError, match="Unsupported distributed vector index type 'BTREE'"):
        create_vector_index(uri, column="vector", index_type="BTREE")

    with pytest.raises(ValueError, match="Column name cannot be empty"):
        create_vector_index(uri, column="", index_type="IVF_FLAT")

    with pytest.raises(ValueError, match="Column 'missing' not found"):
        create_vector_index(uri, column="missing", index_type="IVF_FLAT")

    with pytest.raises(ValueError, match="sample_rate must be positive"):
        create_vector_index(uri, column="vector", index_type="IVF_FLAT", sample_rate=0)

    with pytest.raises(ValueError, match="fragment_ids must be a non-empty list"):
        create_vector_index(uri, column="vector", index_type="IVF_FLAT", fragment_ids=[])

    with pytest.raises(ValueError, match="do not exist in the dataset"):
        create_vector_index(uri, column="vector", index_type="IVF_FLAT", fragment_ids=[99])

    # Argument validation must fire before training work, even when training
    # would also fail on this small dataset with a large sample_rate.
    with pytest.raises(ValueError, match="fragment_group_size must be positive"):
        create_vector_index(uri, column="vector", index_type="IVF_FLAT", fragment_group_size=0)
