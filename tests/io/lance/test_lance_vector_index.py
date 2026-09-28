from __future__ import annotations

import warnings
from pathlib import Path

import lance
import numpy as np
import pytest
from lance.indices import IndicesBuilder

from daft.dependencies import pa
from daft_lance import create_vector_index

warnings.filterwarnings("ignore", category=DeprecationWarning, module="lance")


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

    results = ds.to_table(nearest={"column": "vector", "q": vectors[7], "k": 5})
    assert results.num_rows == 5
    assert 7 in results["id"].to_pylist()


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
    queries = range(0, 400, 40)
    for qi in queries:
        truth = set(np.argsort(((vectors - vectors[qi]) ** 2).sum(axis=1))[:5].tolist())
        got = set(ds.to_table(nearest={"column": "vector", "q": vectors[qi], "k": 5})["id"].to_pylist())
        hits += len(truth & got)
    # SQ is a lossy compression; a healthy build keeps most of the true top-5.
    assert hits >= 0.8 * 5 * len(list(queries))


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


def test_fragment_ids_partial_build_then_backfill(tmp_path: Path) -> None:
    uri, _ = _make_vector_dataset(tmp_path / "partial.lance", num_rows=240, rows_per_file=40, seed=8)
    build = dict(column="vector", index_type="IVF_FLAT", num_partitions=4, sample_rate=32, name="v_idx")

    create_vector_index(uri, fragment_ids=[0, 1, 2], **build)
    assert _covered_fragments(uri, "v_idx") == {0, 1, 2}

    # Backfill appends coverage for the remaining fragments.
    create_vector_index(uri, fragment_ids=[1, 3, 4, 5], **build)
    assert _covered_fragments(uri, "v_idx") == {0, 1, 2, 3, 4, 5}

    # A fully covered request is a no-op: version and coverage stay unchanged.
    version_before = lance.dataset(uri).version
    create_vector_index(uri, fragment_ids=[2, 3], **build)
    assert lance.dataset(uri).version == version_before
    assert _covered_fragments(uri, "v_idx") == {0, 1, 2, 3, 4, 5}


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
        with pytest.raises(Exception, match="injected segment build failure|Error processing"):
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


def test_invalid_arguments_raise(tmp_path: Path) -> None:
    uri, _ = _make_vector_dataset(tmp_path / "bad.lance", num_rows=240, rows_per_file=40, seed=11)

    with pytest.raises(ValueError, match="Unsupported distributed vector index type 'BTREE'"):
        create_vector_index(uri, column="vector", index_type="BTREE")

    with pytest.raises(ValueError, match="Column 'missing' not found"):
        create_vector_index(uri, column="missing", index_type="IVF_FLAT")

    with pytest.raises(ValueError, match="sample_rate must be positive"):
        create_vector_index(uri, column="vector", index_type="IVF_FLAT", sample_rate=0)

    with pytest.raises(ValueError, match="fragment_ids"):
        create_vector_index(uri, column="vector", index_type="IVF_FLAT", fragment_ids=[])
