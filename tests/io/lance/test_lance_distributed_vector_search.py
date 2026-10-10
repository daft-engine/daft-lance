from __future__ import annotations

import io
from pathlib import Path

import lance
import numpy as np
import pyarrow as pa
import pytest

from daft_lance import create_vector_index, vector_search


def _dataset(path: Path, metric: str = "l2", tied: bool = False) -> tuple[str, np.ndarray]:
    vectors = np.random.default_rng(12).normal(size=(512, 8)).astype("float32")
    if tied:
        vectors[:] = 1
    uri = str(path)
    lance.write_dataset(
        pa.table(
            {
                "id": np.arange(len(vectors)),
                "bucket": np.arange(len(vectors)) % 4,
                "vector": pa.FixedSizeListArray.from_arrays(pa.array(vectors.ravel()), 8),
            }
        ),
        uri,
        max_rows_per_file=128,
    )
    create_vector_index(
        uri,
        column="vector",
        index_type="IVF_FLAT",
        name="v_idx",
        metric=metric,
        num_partitions=1 if tied else 4,
        sample_rate=2,
        fragment_group_size=1,
    )
    return uri, vectors


@pytest.mark.parametrize("metric", ["l2", "cosine", "dot"])
@pytest.mark.parametrize("predicate", [None, "bucket = 1", "id < 3", "id < 0"])
def test_global_topk_matches_reference(tmp_path: Path, metric: str, predicate: str | None) -> None:
    uri, _ = _dataset(tmp_path / "search.lance", metric)
    q = np.random.default_rng(17).normal(size=8).astype("float32")
    nearest = {"column": "vector", "q": q, "k": 10, "nprobes": 4, "metric": metric, "refine_factor": 2}
    result = vector_search(uri, nearest=nearest, columns=["id"], filter=predicate).to_arrow()
    reference = (
        lance.dataset(uri)
        .scanner(columns=["id", "_distance"], nearest=nearest, with_row_id=True, filter=predicate, prefilter=True)
        .to_table()
        .sort_by([("_distance", "ascending"), ("_rowid", "ascending")])
    )
    assert result["id"].to_pylist() == reference["id"].to_pylist()
    assert result["_rowid"].to_pylist() == reference["_rowid"].to_pylist()
    np.testing.assert_allclose(result["_distance"].to_numpy(), reference["_distance"].to_numpy(), atol=1e-6)


def test_plan_has_multiple_tasks_and_global_limit(tmp_path: Path) -> None:
    uri, vectors = _dataset(tmp_path / "plan.lance")
    df = vector_search(uri, nearest={"column": "vector", "q": vectors[3], "k": 7})
    output = io.StringIO()
    df.explain(show_all=True, file=output)
    assert "Num Scan Tasks = 4" in output.getvalue()
    assert df.count_rows() == 7
    assert df.limit(2).count_rows() == 2
    # A downstream filter applies to the global top-k, not to individual shards.
    import daft

    assert df.where(daft.col("id") < 0).to_arrow().num_rows == 0
    grouped = vector_search(uri, nearest={"column": "vector", "q": vectors[3], "k": 7}, num_tasks=2)
    assert grouped.select("id").to_pydict() == df.select("id").to_pydict()


def test_batch_queries_have_independent_topk(tmp_path: Path) -> None:
    uri, vectors = _dataset(tmp_path / "batch.lance")
    query_vectors = vectors[[3, 301]]
    result = vector_search(uri, nearest={"column": "vector", "q": query_vectors, "k": 5}).to_arrow()
    assert result["_query_id"].to_pylist() == [0] * 5 + [1] * 5
    for query_id, q in enumerate(query_vectors):
        expected = vector_search(uri, nearest={"column": "vector", "q": q, "k": 5}).to_arrow()
        assert result.slice(query_id * 5, 5)["id"].to_pylist() == expected["id"].to_pylist()


def test_tie_boundary_is_deterministic(tmp_path: Path) -> None:
    uri, vectors = _dataset(tmp_path / "ties.lance", tied=True)
    nearest = {"column": "vector", "q": vectors[0], "k": 5, "nprobes": 1}
    for tasks in (1, 2, 4):
        result = vector_search(uri, nearest=nearest, num_tasks=tasks, columns=["id"]).to_arrow()
        assert result["id"].to_pylist() == list(range(5))


def test_snapshot_is_pinned_and_partial_coverage_rejected(tmp_path: Path) -> None:
    uri, vectors = _dataset(tmp_path / "pinned.lance")
    nearest = {"column": "vector", "q": vectors[0], "k": 5}
    df = vector_search(uri, nearest=nearest, columns=["id"])
    expected = df.to_pydict()
    pending = vector_search(uri, nearest=nearest, columns=["id"])
    lance.write_dataset(lance.dataset(uri).to_table().slice(0, 1), uri, mode="append")
    # A fresh plan sees uncovered data; the already planned DataFrame retains its snapshot.
    with pytest.raises(ValueError, match="complete index coverage"):
        vector_search(uri, nearest=nearest)
    assert (
        vector_search(uri, nearest=nearest, version=lance.dataset(uri).version - 1, columns=["id"]).to_pydict()
        == expected
    )
    assert df.to_pydict() == expected
    assert pending.to_pydict() == expected


def test_namespace_search(tmp_path: Path) -> None:
    uri, vectors = _dataset(tmp_path / "ns.lance")
    result = vector_search(
        table_id=["ns"],
        namespace_impl="dir",
        namespace_properties={"root": str(tmp_path)},
        nearest={"column": "vector", "q": vectors[0], "k": 3},
    )
    assert result.select("id").to_pydict()["id"][0] == 0


@pytest.mark.parametrize("nearest", [{"column": "vector", "q": [1] * 8, "k": 0}, {"column": "vector", "q": [], "k": 3}])
def test_invalid_search_arguments(tmp_path: Path, nearest: dict) -> None:
    uri, _ = _dataset(tmp_path / "invalid.lance")
    with pytest.raises(ValueError):
        vector_search(uri, nearest=nearest)


def test_missing_index_and_disabled_index_rejected(tmp_path: Path) -> None:
    uri, vectors = _dataset(tmp_path / "missing.lance")
    nearest = {"column": "vector", "q": vectors[0], "k": 3}
    with pytest.raises(ValueError, match="Select exactly one"):
        vector_search(uri, nearest=nearest, index_name="missing")
    with pytest.raises(ValueError, match="use_index"):
        vector_search(uri, nearest={**nearest, "use_index": False})
