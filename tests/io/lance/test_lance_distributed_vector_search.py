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


def test_tie_boundary_keeps_bounded_candidates(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from daft_lance import lance_vector_search

    uri, vectors = _dataset(tmp_path / "ties.lance", tied=True)
    nearest = {"column": "vector", "q": vectors[0], "k": 5, "nprobes": 1}
    ds = lance.dataset(uri)
    segments = [str(segment.uuid) for segment in ds.describe_indices()[0].segments]
    scanner = ds.scanner
    calls: list[int] = []

    def counted_scanner(**kwargs):
        calls.append(kwargs["nearest"]["k"])
        return scanner(**kwargs)

    monkeypatch.setattr(lance_vector_search, "_worker_dataset", lambda _: ds)
    monkeypatch.setattr(ds, "scanner", counted_scanner)
    batches = list(lance_vector_search._search_segments(b"", segments, nearest, ["id", "_distance"], None, 0))
    assert calls == [5]
    assert sum(len(batch) for batch in batches) == 5
    for tasks in (1, 2, 4):
        result = vector_search(uri, nearest=nearest, num_tasks=tasks, columns=["id"]).to_arrow()
        assert result.num_rows == 5
        assert len(set(result["id"].to_pylist())) == 5
        assert result["_distance"].to_pylist() == [0.0] * 5
        assert result["_rowid"].to_pylist() == sorted(result["_rowid"].to_pylist())


@pytest.mark.parametrize("batch", [False, True])
def test_query_buffer_is_copied_when_planning(tmp_path: Path, batch: bool) -> None:
    uri, vectors = _dataset(tmp_path / "query-copy.lance")
    queries = vectors[[3, 301]].copy() if batch else vectors[3].copy()
    original = queries.copy()
    pending = vector_search(uri, nearest={"column": "vector", "q": queries, "k": 5}, columns=["id"])
    queries[:] = vectors[400]
    expected = vector_search(uri, nearest={"column": "vector", "q": original, "k": 5}, columns=["id"])
    assert pending.to_pydict() == expected.to_pydict()


def test_quoted_vector_column_matches_lance(tmp_path: Path) -> None:
    uri, vectors = _dataset(tmp_path / "quoted.lance")
    nearest = {"column": "`vector`", "q": vectors[3], "k": 5, "nprobes": 4}
    result = vector_search(uri, nearest=nearest, columns=["id"]).to_arrow()
    reference = lance.dataset(uri).scanner(nearest=nearest, columns=["id"]).to_table()
    assert result["id"].to_pylist() == reference["id"].to_pylist()


def test_uneven_segments_are_balanced_by_rows(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from types import SimpleNamespace

    from daft_lance import lance_vector_search
    from daft_lance.namespace import DatasetOpenContext

    sizes = [256, 32, 256, 32]
    vectors = np.random.default_rng(19).normal(size=(sum(sizes), 8)).astype("float32")
    uri = str(tmp_path / "skew.lance")
    start = 0
    for size in sizes:
        lance.write_dataset(
            pa.table(
                {
                    "id": np.arange(start, start + size),
                    "vector": pa.FixedSizeListArray.from_arrays(pa.array(vectors[start : start + size].ravel()), 8),
                }
            ),
            uri,
            mode="create" if start == 0 else "append",
            max_rows_per_file=size,
        )
        start += size
    create_vector_index(
        uri, column="vector", index_type="IVF_FLAT", num_partitions=4, sample_rate=2, fragment_group_size=1
    )
    ds = lance.dataset(uri)
    rows = {fragment.fragment_id: fragment.metadata.num_rows for fragment in ds.get_fragments()}
    index = ds.describe_indices()[0]
    # Manifest order is not a workload ordering guarantee. Arrange valid
    # segments by fragment ID so round-robin groups both large segments together.
    segments = sorted(index.segments, key=lambda segment: min(segment.fragment_ids))
    description = SimpleNamespace(index_type=index.index_type, fields=index.fields, name=index.name, segments=segments)
    monkeypatch.setattr(ds, "describe_indices", lambda: [description])
    weights = {
        str(segment.uuid): sum(rows[fid] for fid in segment.fragment_ids)
        for segment in ds.describe_indices()[0].segments
    }
    operator = lance_vector_search.VectorSearchScanOperator
    groups: list[list[str]] = []

    def capture_groups(open_context, assigned, *args):
        groups.extend(assigned)
        return operator(open_context, assigned, *args)

    monkeypatch.setattr(lance_vector_search, "VectorSearchScanOperator", capture_groups)
    nearest = {"column": "vector", "q": vectors[3], "k": 5, "nprobes": 4}
    result = lance_vector_search.vector_search_internal(
        ds,
        DatasetOpenContext.from_dataset(ds, uri),
        nearest=nearest,
        index_name=None,
        columns=["id"],
        predicate=None,
        num_tasks=2,
    ).to_arrow()
    assert [sum(weights[segment] for segment in group) for group in groups] == [288, 288]
    reference = ds.scanner(nearest=nearest, columns=["id"]).to_table()
    assert result["id"].to_pylist() == reference["id"].to_pylist()


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
