from __future__ import annotations

from pathlib import Path

import lance
import numpy as np
import pytest
from lance.commit import CommitConflictError

from daft.dependencies import pa
from daft_lance import create_vector_index, lance_vector_index, optimize_indices
from daft_lance.namespace import DatasetOpenContext


def _dataset(tmp_path: Path):
    vectors = np.random.default_rng(23).standard_normal((240, 8)).astype(np.float32)
    uri = str(tmp_path / "concurrent.lance")
    lance.write_dataset(
        pa.table({"id": range(240), "vector": pa.FixedSizeListArray.from_arrays(vectors.ravel(), 8)}),
        uri,
        max_rows_per_file=40,
    )
    return uri, vectors


def _segments(dataset):
    return {segment.uuid for segment in dataset.describe_indices()[0].segments}


def test_same_name_created_before_live_check_is_preserved(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    uri, vectors = _dataset(tmp_path)
    original_open = DatasetOpenContext.open_latest
    competing = {}

    def open_after_competing_commit(self):
        dataset = lance.dataset(uri)
        dataset.create_index("vector", "IVF_FLAT", name="vector_idx", num_partitions=4)
        competing["version"] = dataset.version
        competing["segments"] = _segments(dataset)
        return original_open(self)

    monkeypatch.setattr(DatasetOpenContext, "open_latest", open_after_competing_commit)
    with pytest.raises(ValueError, match="changed during the build"):
        create_vector_index(
            uri, column="vector", index_type="IVF_FLAT", num_partitions=4, sample_rate=8, fragment_group_size=2
        )

    dataset = lance.dataset(uri)
    assert dataset.version == competing["version"]
    assert _segments(dataset) == competing["segments"]
    assert 7 in dataset.to_table(nearest={"column": "vector", "q": vectors[7], "k": 5})["id"].to_pylist()


def test_same_name_created_after_live_check_causes_commit_conflict(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    uri, vectors = _dataset(tmp_path)
    original_validate = lance_vector_index._validate_segments_against_manifest
    competing = {}

    def validate_after_competing_commit(dataset, index_metas, expected_fragment_ids):
        other = lance.dataset(uri)
        other.create_index("vector", "IVF_FLAT", name="vector_idx", num_partitions=4)
        competing["version"] = other.version
        competing["segments"] = _segments(other)
        return original_validate(dataset, index_metas, expected_fragment_ids)

    monkeypatch.setattr(lance_vector_index, "_validate_segments_against_manifest", validate_after_competing_commit)
    with pytest.raises(CommitConflictError, match="CreateIndex"):
        create_vector_index(
            uri, column="vector", index_type="IVF_FLAT", num_partitions=4, sample_rate=8, fragment_group_size=2
        )

    dataset = lance.dataset(uri)
    assert dataset.version == competing["version"]
    assert _segments(dataset) == competing["segments"]
    assert 7 in dataset.to_table(nearest={"column": "vector", "q": vectors[7], "k": 5})["id"].to_pylist()


def test_replace_true_replaces_concurrently_rebuilt_index(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    uri, vectors = _dataset(tmp_path)
    create_vector_index(uri, column="vector", index_type="IVF_FLAT", num_partitions=4, sample_rate=8)
    original_open = DatasetOpenContext.open_latest
    competing = {}

    def open_after_rebuild(self):
        dataset = lance.dataset(uri)
        dataset.create_index(
            "vector",
            "IVF_FLAT",
            name="vector_idx",
            metric="cosine",
            replace=True,
            num_partitions=4,
        )
        competing["version"] = dataset.version
        competing["segments"] = _segments(dataset)
        return original_open(self)

    monkeypatch.setattr(DatasetOpenContext, "open_latest", open_after_rebuild)
    create_vector_index(
        uri,
        column="vector",
        index_type="IVF_FLAT",
        num_partitions=4,
        sample_rate=8,
        replace=True,
        fragment_group_size=2,
    )

    dataset = lance.dataset(uri)
    assert dataset.version == competing["version"] + 1
    assert _segments(dataset).isdisjoint(competing["segments"])
    assert len(_segments(dataset)) == 3
    assert all(segment["metric_type"] == "l2" for segment in dataset.index_statistics("vector_idx")["segments"])
    assert 7 in dataset.to_table(nearest={"column": "vector", "q": vectors[7], "k": 5})["id"].to_pylist()


def test_unrelated_append_after_live_check_allows_index_commit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    uri, vectors = _dataset(tmp_path)
    original_validate = lance_vector_index._validate_segments_against_manifest

    def validate_after_append(dataset, index_metas, expected_fragment_ids):
        lance.write_dataset(
            pa.table({"id": [240], "vector": pa.FixedSizeListArray.from_arrays(vectors[7], 8)}), uri, mode="append"
        )
        return original_validate(dataset, index_metas, expected_fragment_ids)

    monkeypatch.setattr(lance_vector_index, "_validate_segments_against_manifest", validate_after_append)
    create_vector_index(
        uri, column="vector", index_type="IVF_FLAT", num_partitions=4, sample_rate=8, fragment_group_size=2
    )

    dataset = lance.dataset(uri)
    assert dataset.version == 3
    assert dataset.count_rows() == 241
    assert dataset.describe_indices()[0].num_rows_indexed == 240
    assert len(_segments(dataset)) == 3
    assert 7 in dataset.to_table(nearest={"column": "vector", "q": vectors[7], "k": 5})["id"].to_pylist()


def test_replace_rejects_concurrently_indexed_appended_fragments(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A replacement must not leave an old-metric segment outside its pinned build plan."""
    uri, vectors = _dataset(tmp_path)
    create_vector_index(uri, column="vector", index_type="IVF_FLAT", num_partitions=4, sample_rate=8)
    original_open = DatasetOpenContext.open_latest
    more = np.random.default_rng(31).standard_normal((40, 8)).astype(np.float32) + 10
    competing = {}
    queries = [vectors[7], more[7]]

    def open_after_incremental_maintenance(self):
        lance.write_dataset(
            pa.table({"id": range(240, 280), "vector": pa.FixedSizeListArray.from_arrays(more.ravel(), 8)}),
            uri,
            mode="append",
        )
        monkeypatch.setattr(DatasetOpenContext, "open_latest", original_open)
        optimize_indices(uri, indices=["vector_idx"], num_indices_to_merge=0)
        dataset = lance.dataset(uri)
        competing["version"] = dataset.version
        competing["segments"] = _segments(dataset)
        competing["results"] = [
            dataset.to_table(nearest={"column": "vector", "q": query, "k": 5, "nprobes": 4}).to_pydict()
            for query in queries
        ]
        return original_open(self)

    monkeypatch.setattr(DatasetOpenContext, "open_latest", open_after_incremental_maintenance)
    with pytest.raises(ValueError, match="coverage outside the build plan"):
        create_vector_index(
            uri,
            column="vector",
            index_type="IVF_FLAT",
            num_partitions=4,
            sample_rate=8,
            metric="cosine",
            replace=True,
            fragment_group_size=2,
        )

    dataset = lance.dataset(uri)
    assert dataset.version == competing["version"]
    assert _segments(dataset) == competing["segments"]
    assert dataset.describe_indices()[0].num_rows_indexed == 280
    assert all(segment["metric_type"] == "l2" for segment in dataset.index_statistics("vector_idx")["segments"])
    for query, expected in zip(queries, competing["results"]):
        assert dataset.to_table(nearest={"column": "vector", "q": query, "k": 5, "nprobes": 4}).to_pydict() == expected
    assert 7 in competing["results"][0]["id"]
    assert 247 in competing["results"][1]["id"]
