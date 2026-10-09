from __future__ import annotations

from pathlib import Path

import lance
import numpy as np
import pytest
from lance.commit import CommitConflictError

from daft.dependencies import pa
from daft_lance import create_vector_index, lance_vector_index
from daft_lance.namespace import DatasetOpenContext


def _dataset(tmp_path: Path):
    vectors = np.random.default_rng(23).standard_normal((240, 8)).astype(np.float32)
    uri = str(tmp_path / "concurrent.lance")
    lance.write_dataset(
        pa.table({"id": range(240), "vector": pa.FixedSizeListArray.from_arrays(vectors.ravel(), 8)}),
        uri,
        max_rows_per_file=40,
    )
    centroids = (
        lance.indices.IndicesBuilder(lance.dataset(uri), "vector").train_ivf(num_partitions=4, sample_rate=8).centroids
    )
    return uri, vectors, centroids


def _segments(dataset):
    return {segment.uuid for segment in dataset.describe_indices()[0].segments}


def test_same_name_created_before_live_check_is_preserved(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    uri, vectors, centroids = _dataset(tmp_path)
    original_open = DatasetOpenContext.open_latest
    competing = {}

    def open_after_competing_commit(self):
        dataset = lance.dataset(uri)
        dataset.create_index("vector", "IVF_FLAT", name="vector_idx", ivf_centroids=centroids, num_partitions=4)
        competing["version"] = dataset.version
        competing["segments"] = _segments(dataset)
        return original_open(self)

    monkeypatch.setattr(DatasetOpenContext, "open_latest", open_after_competing_commit)
    with pytest.raises(ValueError, match="changed during the build"):
        create_vector_index(uri, column="vector", index_type="IVF_FLAT", ivf_centroids=centroids, fragment_group_size=2)

    dataset = lance.dataset(uri)
    assert dataset.version == competing["version"]
    assert _segments(dataset) == competing["segments"]
    assert 7 in dataset.to_table(nearest={"column": "vector", "q": vectors[7], "k": 5})["id"].to_pylist()


def test_same_name_created_after_live_check_causes_commit_conflict(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    uri, vectors, centroids = _dataset(tmp_path)
    original_validate = lance_vector_index._validate_segments_against_manifest
    competing = {}

    def validate_after_competing_commit(dataset, index_metas, expected_fragment_ids):
        other = lance.dataset(uri)
        other.create_index("vector", "IVF_FLAT", name="vector_idx", ivf_centroids=centroids, num_partitions=4)
        competing["version"] = other.version
        competing["segments"] = _segments(other)
        return original_validate(dataset, index_metas, expected_fragment_ids)

    monkeypatch.setattr(lance_vector_index, "_validate_segments_against_manifest", validate_after_competing_commit)
    with pytest.raises(CommitConflictError, match="CreateIndex"):
        create_vector_index(uri, column="vector", index_type="IVF_FLAT", ivf_centroids=centroids, fragment_group_size=2)

    dataset = lance.dataset(uri)
    assert dataset.version == competing["version"]
    assert _segments(dataset) == competing["segments"]
    assert 7 in dataset.to_table(nearest={"column": "vector", "q": vectors[7], "k": 5})["id"].to_pylist()


def test_backfill_rejects_concurrent_rebuild_with_changed_metric(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    uri, vectors, centroids = _dataset(tmp_path)
    create_vector_index(uri, column="vector", index_type="IVF_FLAT", ivf_centroids=centroids, fragment_ids=[0, 1, 2])
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
            ivf_centroids=centroids,
            num_partitions=4,
        )
        competing["version"] = dataset.version
        competing["segments"] = _segments(dataset)
        return original_open(self)

    monkeypatch.setattr(DatasetOpenContext, "open_latest", open_after_rebuild)
    with pytest.raises(ValueError, match="changed during the build"):
        create_vector_index(
            uri, column="vector", index_type="IVF_FLAT", ivf_centroids=centroids, fragment_ids=[3, 4, 5]
        )

    dataset = lance.dataset(uri)
    assert dataset.version == competing["version"]
    assert _segments(dataset) == competing["segments"]
    assert dataset.index_statistics("vector_idx")["segments"][0]["metric_type"] == "cosine"
    assert 7 in dataset.to_table(nearest={"column": "vector", "q": vectors[7], "k": 5})["id"].to_pylist()


def test_unrelated_append_after_live_check_allows_index_commit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    uri, vectors, centroids = _dataset(tmp_path)
    original_validate = lance_vector_index._validate_segments_against_manifest

    def validate_after_append(dataset, index_metas, expected_fragment_ids):
        lance.write_dataset(
            pa.table({"id": [240], "vector": pa.FixedSizeListArray.from_arrays(vectors[7], 8)}), uri, mode="append"
        )
        return original_validate(dataset, index_metas, expected_fragment_ids)

    monkeypatch.setattr(lance_vector_index, "_validate_segments_against_manifest", validate_after_append)
    create_vector_index(uri, column="vector", index_type="IVF_FLAT", ivf_centroids=centroids, fragment_group_size=2)

    dataset = lance.dataset(uri)
    assert dataset.version == 3
    assert dataset.count_rows() == 241
    assert dataset.describe_indices()[0].num_rows_indexed == 240
    assert len(_segments(dataset)) == 3
    assert 7 in dataset.to_table(nearest={"column": "vector", "q": vectors[7], "k": 5})["id"].to_pylist()
