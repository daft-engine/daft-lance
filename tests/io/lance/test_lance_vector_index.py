from __future__ import annotations

import warnings
from pathlib import Path

import lance
import numpy as np
import pytest
from lance.indices import IndicesBuilder

from daft.dependencies import pa
from daft_lance import create_vector_index, lance_vector_index, optimize_indices

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
    """All six advertised vector index types build multiple segments and return results."""
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


@pytest.mark.parametrize("index_type", ["IVF_SQ", "IVF_HNSW_SQ"])
def test_sq_backfill_and_failed_merge_preserve_index(tmp_path: Path, index_type: str) -> None:
    """SQ backfill is searchable; incompatible merges leave the committed index intact."""
    rng = np.random.default_rng(7)
    vectors = rng.uniform(0, 1, (2048, 8)).astype(np.float32)
    vectors[1024:] += 10
    uri = str(tmp_path / "sq_backfill.lance")
    lance.write_dataset(
        pa.table({"id": list(range(2048)), "vector": pa.FixedSizeListArray.from_arrays(vectors.reshape(-1), 8)}),
        uri,
        max_rows_per_file=256,
    )
    centroids = (
        IndicesBuilder(lance.dataset(uri), "vector")
        .train_ivf(num_partitions=4, distance_type="l2", sample_rate=8)
        .centroids
    )
    build = dict(column="vector", index_type=index_type, ivf_centroids=centroids, name="v_idx")
    create_vector_index(uri, fragment_ids=[0, 1, 2, 3], **build)
    create_vector_index(uri, fragment_ids=[4, 5, 6, 7], **build)

    before = lance.dataset(uri)
    assert len(before.describe_indices()[0].segments) == 2
    assert _covered_fragments(uri, "v_idx") == set(range(8))
    segment_ids_before = [segment.uuid for segment in before.describe_indices()[0].segments]
    results_before = []
    for qi in [7, 1300]:
        result = before.to_table(nearest={"column": "vector", "q": vectors[qi], "k": 5, "nprobes": 4})
        assert qi in result["id"].to_pylist()
        results_before.append(result.to_pydict())

    with pytest.raises(OSError, match="do not share quantizer metadata"):
        optimize_indices(uri, indices=["v_idx"], num_indices_to_merge=2)

    after = lance.dataset(uri)
    assert after.version == before.version
    assert [segment.uuid for segment in after.describe_indices()[0].segments] == segment_ids_before
    assert _covered_fragments(uri, "v_idx") == set(range(8))
    for qi, expected in zip([7, 1300], results_before):
        assert (
            after.to_table(nearest={"column": "vector", "q": vectors[qi], "k": 5, "nprobes": 4}).to_pydict() == expected
        )


def test_sq_single_segment_stays_maintainable(tmp_path: Path) -> None:
    """An explicitly requested single-segment SQ index supports incremental maintenance."""
    uri, vectors = _make_vector_dataset(tmp_path / "sq_life.lance")

    create_vector_index(
        uri, column="vector", index_type="IVF_SQ", num_partitions=8, sample_rate=8, name="v_idx", fragment_group_size=8
    )
    assert len(lance.dataset(uri).describe_indices()[0].segments) == 1

    more = np.random.default_rng(99).standard_normal((512, 8)).astype(np.float32)
    lance.write_dataset(
        pa.table(
            {
                "id": list(range(2048, 2560)),
                "vector": pa.FixedSizeListArray.from_arrays(more.reshape(-1), 8),
            }
        ),
        uri,
        mode="append",
        max_rows_per_file=256,
    )

    optimize_indices(uri, indices=["v_idx"], num_indices_to_merge=2)
    ds = lance.dataset(uri)
    assert len(ds.describe_indices()[0].segments) == 1
    assert 7 in ds.to_table(nearest={"column": "vector", "q": vectors[7], "k": 5})["id"].to_pylist()


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


def test_pq_codebook_requires_original_num_sub_vectors(tmp_path: Path) -> None:
    """A bare codebook cannot reveal how the original vectors were split."""
    uri, _ = _make_vector_dataset(tmp_path / "codebook.lance", seed=19)
    ds = lance.dataset(uri)
    builder = IndicesBuilder(ds, "vector")
    ivf = builder.train_ivf(num_partitions=4, distance_type="l2", sample_rate=32)
    pq = builder.train_pq(ivf, num_subvectors=2, sample_rate=8)

    with pytest.raises(ValueError, match="original num_sub_vectors"):
        create_vector_index(
            uri, column="vector", index_type="IVF_PQ", ivf_centroids=ivf.centroids, pq_codebook=pq.codebook
        )

    create_vector_index(
        uri,
        column="vector",
        index_type="IVF_PQ",
        ivf_centroids=ivf.centroids,
        pq_codebook=pq.codebook,
        num_sub_vectors=2,
        fragment_group_size=4,
    )

    desc = lance.dataset(uri).describe_indices()[0]
    assert desc.index_type == "IVF_PQ"
    assert desc.num_rows_indexed == 2048
    assert all(
        segment["sub_index"]["num_sub_vectors"] == 2
        for segment in lance.dataset(uri).stats.index_stats("vector_idx")["indices"]
    )


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


def test_worker_failure_keeps_version_and_old_index(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
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

    class FailingVectorFragmentIndexHandler(lance_vector_index.VectorFragmentIndexHandler):
        def __call__(self, fragment_ids: list[int]) -> bytes:
            if 4 in fragment_ids:
                raise RuntimeError("injected segment build failure")
            return super().__call__(fragment_ids)

    monkeypatch.setattr(lance_vector_index, "VectorFragmentIndexHandler", FailingVectorFragmentIndexHandler)
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


def test_existing_segment_uuid_is_rejected_without_overwriting_index(tmp_path: Path) -> None:
    """A caller cannot overwrite published segment files during a failed rebuild."""
    uri, vectors = _make_vector_dataset(tmp_path / "uuid.lance", num_rows=240, rows_per_file=60)
    build = dict(column="vector", index_type="IVF_FLAT", num_partitions=4, sample_rate=8)
    create_vector_index(uri, fragment_group_size=4, **build)
    before = lance.dataset(uri)
    segment_uuid = str(before.describe_indices()[0].segments[0].uuid)
    query = {"column": "vector", "q": vectors[7], "k": 5, "nprobes": 4}
    expected = before.to_table(nearest=query).to_pydict()

    with pytest.raises(TypeError, match="index_uuid"):
        create_vector_index(
            uri,
            replace=True,
            fragment_group_size=1,
            max_concurrency=1,
            index_uuid=segment_uuid,
            **build,
        )

    after = lance.dataset(uri)
    assert after.version == before.version
    assert str(after.describe_indices()[0].segments[0].uuid) == segment_uuid
    assert _covered_fragments(uri, "vector_idx") == {0, 1, 2, 3}
    assert after.to_table(nearest=query).to_pydict() == expected


@pytest.mark.parametrize("initial_metric,append_metric", [("L2", "cosine"), ("cosine", None)])
def test_backfill_rejects_a_different_metric(tmp_path: Path, initial_metric: str, append_metric: str | None) -> None:
    """Segments must use comparable distances, including when backfill defaults to L2."""
    uri = str(tmp_path / "mixed_metric.lance")
    vectors = np.array([[0, 1], [100, 0]], dtype=np.float32)
    lance.write_dataset(
        pa.table({"id": [0, 1], "vector": pa.FixedSizeListArray.from_arrays(vectors.reshape(-1), 2)}),
        uri,
        max_rows_per_file=1,
    )
    centroids = pa.FixedSizeListArray.from_arrays(pa.array([1, 0], type=pa.float32()), 2)
    build = dict(column="vector", index_type="IVF_FLAT", ivf_centroids=centroids)
    create_vector_index(uri, metric=initial_metric, fragment_ids=[0], **build)
    before = lance.dataset(uri)
    query = {"column": "vector", "q": [1, 0], "k": 2, "nprobes": 1, "metric": initial_metric}
    expected = before.to_table(nearest=query).to_pydict()

    append_kwargs = {} if append_metric is None else {"metric": append_metric}
    with pytest.raises(ValueError, match="[Mm]etric"):
        create_vector_index(uri, fragment_ids=[1], **build, **append_kwargs)

    after = lance.dataset(uri)
    assert after.version == before.version
    assert _covered_fragments(uri, "vector_idx") == {0}
    assert after.to_table(nearest=query).to_pydict() == expected


@pytest.mark.parametrize("append_metric", ["l2", "L2", "euclidean", "EUCLIDEAN"])
def test_backfill_accepts_equivalent_metric_spellings(tmp_path: Path, append_metric: str) -> None:
    """Lance's Euclidean alias and case variations do not imply a changed metric."""
    uri, _ = _make_vector_dataset(tmp_path / "metric_alias.lance", num_rows=240, rows_per_file=120)
    centroids = (
        IndicesBuilder(lance.dataset(uri), "vector")
        .train_ivf(num_partitions=4, distance_type="l2", sample_rate=8)
        .centroids
    )
    build = dict(column="vector", index_type="IVF_FLAT", ivf_centroids=centroids)
    create_vector_index(uri, metric="L2", fragment_ids=[0], **build)
    create_vector_index(uri, metric=append_metric, fragment_ids=[1], **build)
    assert _covered_fragments(uri, "vector_idx") == {0, 1}


def test_hnsw_build_parameters_are_forwarded(tmp_path: Path) -> None:
    """Real HNSW kwargs are implemented by Lance even though its Python signature uses **kwargs."""
    uri, vectors = _make_vector_dataset(tmp_path / "hnsw_kwargs.lance", num_rows=240, rows_per_file=60)
    create_vector_index(
        uri,
        column="vector",
        index_type="IVF_HNSW_FLAT",
        num_partitions=4,
        sample_rate=8,
        fragment_group_size=2,
        m=4,
        max_level=3,
        ef_construction=24,
    )
    ds = lance.dataset(uri)
    assert len(ds.describe_indices()[0].segments) == 2
    for segment in ds.index_statistics("vector_idx")["segments"]:
        params = segment["sub_index"]["params"]
        assert params["m"] == 4
        assert params["max_level"] == 3
        assert params["ef_construction"] == 24
    assert 7 in ds.to_table(nearest={"column": "vector", "q": vectors[7], "k": 5, "nprobes": 4})["id"].to_pylist()


def test_centroids_file_is_rejected_before_segment_build(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Unsupported model-file input must fail before training or writing segment files."""
    uri, _ = _make_vector_dataset(tmp_path / "centroids_file.lance", num_rows=240, rows_per_file=60)
    before = lance.dataset(uri)

    def unexpected_training(*args, **kwargs):
        raise AssertionError("unsupported model-file argument reached training")

    monkeypatch.setattr(IndicesBuilder, "train_ivf", unexpected_training)
    with pytest.raises(TypeError, match="ivf_centroids_file"):
        create_vector_index(uri, column="vector", index_type="IVF_FLAT", ivf_centroids_file="centroids.npy")
    assert lance.dataset(uri).version == before.version
    assert not lance.dataset(uri).describe_indices()


@pytest.mark.parametrize("index_type", ["IVF_PQ", "IVF_HNSW_PQ"])
@pytest.mark.parametrize("num_bits", [4, 8])
def test_pq_bit_width_is_shared_by_all_segments(tmp_path: Path, index_type: str, num_bits: int) -> None:
    uri, vectors = _make_vector_dataset(tmp_path / "bits.lance", num_rows=512, rows_per_file=128)
    create_vector_index(
        uri,
        column="vector",
        index_type=index_type,
        num_partitions=2,
        num_sub_vectors=2,
        num_bits=num_bits,
        sample_rate=8,
        fragment_group_size=2,
    )
    ds = lance.dataset(uri)
    segments = ds.stats.index_stats("vector_idx")["indices"]
    assert len(segments) == 2
    assert all(segment["sub_index"]["nbits"] == num_bits for segment in segments)
    assert all(segment["sub_index"]["num_sub_vectors"] == 2 for segment in segments)
    result = ds.to_table(nearest={"column": "vector", "q": vectors[7], "k": 5, "nprobes": 2})
    assert result.num_rows == 5
    assert np.isfinite(result["_distance"].to_numpy()).all()


def test_four_bit_pq_sampling_uses_sixteen_centroids(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    uri, _ = _make_vector_dataset(tmp_path / "sampling_bits.lance", num_rows=128, rows_per_file=64)
    centroids = pa.FixedSizeListArray.from_arrays(pa.array([0.0] * 8, type=pa.float32()), 8)
    original_train = IndicesBuilder.train_pq
    calls = []

    def train_with_recorded_sample(builder, *args, **kwargs):
        calls.append((kwargs["num_bits"], kwargs["sample_rate"]))
        return original_train(builder, *args, **kwargs)

    monkeypatch.setattr(IndicesBuilder, "train_pq", train_with_recorded_sample)
    create_vector_index(
        uri,
        column="vector",
        index_type="IVF_PQ",
        ivf_centroids=centroids,
        num_sub_vectors=2,
        num_bits=4,
        sample_rate=8,
        fragment_group_size=1,
    )
    # 16 * 8 samples fit in 128 rows; no IVF training or 256-entry cap applies.
    assert calls == [(4, 8)]
    assert all(
        segment["sub_index"]["nbits"] == 4 for segment in lance.dataset(uri).stats.index_stats("vector_idx")["indices"]
    )


def test_supplied_four_bit_pq_model_backfills_with_original_configuration(tmp_path: Path) -> None:
    uri, vectors = _make_vector_dataset(tmp_path / "supplied_bits.lance", num_rows=256, rows_per_file=128)
    builder = IndicesBuilder(lance.dataset(uri), "vector")
    ivf = builder.train_ivf(num_partitions=2, sample_rate=8)
    pq = builder.train_pq(ivf, num_subvectors=2, num_bits=4, sample_rate=8)
    build = dict(
        column="vector",
        index_type="IVF_PQ",
        ivf_centroids=ivf.centroids,
        pq_codebook=pq.codebook,
        num_sub_vectors=2,
        num_bits=4,
    )
    create_vector_index(uri, fragment_ids=[0], **build)
    create_vector_index(uri, fragment_ids=[1], **build)
    ds = lance.dataset(uri)
    segments = ds.stats.index_stats("vector_idx")["indices"]
    assert len(segments) == 2
    assert all(segment["sub_index"]["nbits"] == 4 for segment in segments)
    assert all(segment["sub_index"]["num_sub_vectors"] == 2 for segment in segments)
    assert _covered_fragments(uri, "vector_idx") == {0, 1}
    assert ds.to_table(nearest={"column": "vector", "q": vectors[7], "k": 5, "nprobes": 2}).num_rows == 5


@pytest.mark.parametrize("index_type", ["IVF_FLAT", "IVF_SQ", "IVF_HNSW_FLAT", "IVF_HNSW_SQ"])
def test_non_pq_bit_width_is_not_silently_ignored(tmp_path: Path, index_type: str) -> None:
    uri, _ = _make_vector_dataset(tmp_path / "non_pq_bits.lance", num_rows=128, rows_per_file=64)
    before = lance.dataset(uri)
    with pytest.raises(ValueError, match="configurable only for IVF_PQ"):
        create_vector_index(uri, column="vector", index_type=index_type, num_bits=4)
    assert lance.dataset(uri).version == before.version
    assert not lance.dataset(uri).describe_indices()
