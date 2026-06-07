from __future__ import annotations

import tempfile
from pathlib import Path

import lance
import numpy as np
import pyarrow as pa
import pytest

import daft
from daft_lance import create_vector_index


@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


def _make_vector_dataset(path: str | Path, *, num_rows: int = 256, dim: int = 16) -> str:
    """Create a Lance dataset with a fixed-size list vector column."""
    rng = np.random.default_rng(42)
    vectors = rng.standard_normal((num_rows, dim)).astype(np.float32)
    vector_type = pa.list_(pa.float32(), dim)
    vector_array = pa.FixedSizeListArray.from_arrays(vectors.flatten(), list_size=dim)
    ids = pa.array(range(num_rows), type=pa.int64())
    table = pa.table({"id": ids, "vector": vector_array.cast(vector_type)})
    uri = str(Path(path) / "vector_dataset.lance")
    lance.write_dataset(table, uri)
    return uri


class TestCreateVectorIndex:
    """Tests for the create_vector_index API."""

    def test_create_ivf_pq_index(self, temp_dir):
        """Create an IVF_PQ index and verify it appears in describe_indices."""
        uri = _make_vector_dataset(temp_dir)

        create_vector_index(uri, column="vector", index_type="IVF_PQ", num_partitions=2, num_sub_vectors=2)

        ds = lance.dataset(uri)
        indices = ds.describe_indices()
        assert len(indices) == 1
        assert indices[0].name is not None

    def test_create_ivf_pq_index_default_type(self, temp_dir):
        """Default index_type should be IVF_PQ."""
        uri = _make_vector_dataset(temp_dir)

        create_vector_index(uri, column="vector", num_partitions=2, num_sub_vectors=2)

        ds = lance.dataset(uri)
        indices = ds.describe_indices()
        assert len(indices) == 1

    def test_create_index_cosine_metric(self, temp_dir):
        """Create an index with cosine metric."""
        uri = _make_vector_dataset(temp_dir)

        create_vector_index(
            uri,
            column="vector",
            index_type="IVF_PQ",
            metric="cosine",
            num_partitions=2,
            num_sub_vectors=2,
        )

        ds = lance.dataset(uri)
        indices = ds.describe_indices()
        assert len(indices) == 1

    def test_create_index_custom_name(self, temp_dir):
        """Create an index with a custom name and verify it."""
        uri = _make_vector_dataset(temp_dir)
        index_name = "my_custom_vector_idx"

        create_vector_index(
            uri,
            column="vector",
            name=index_name,
            num_partitions=2,
            num_sub_vectors=2,
        )

        ds = lance.dataset(uri)
        indices = ds.describe_indices()
        assert len(indices) == 1
        assert indices[0].name == index_name

    def test_replace_existing_index(self, temp_dir):
        """replace=True should overwrite an existing index without error."""
        uri = _make_vector_dataset(temp_dir)

        create_vector_index(uri, column="vector", num_partitions=2, num_sub_vectors=2)
        # Should not raise because replace=True is the default
        create_vector_index(uri, column="vector", num_partitions=2, num_sub_vectors=2, replace=True)

        ds = lance.dataset(uri)
        indices = ds.describe_indices()
        # Should still have exactly one index (replaced, not duplicated)
        vector_indices = [idx for idx in indices if "vector" in idx.field_names]
        assert len(vector_indices) == 1

    def test_replace_false_raises_on_existing(self, temp_dir):
        """replace=False should raise when an index already exists on the column."""
        uri = _make_vector_dataset(temp_dir)

        create_vector_index(uri, column="vector", num_partitions=2, num_sub_vectors=2)

        with pytest.raises(Exception):
            create_vector_index(uri, column="vector", replace=False, num_partitions=2, num_sub_vectors=2)

    def test_invalid_column_not_found(self, temp_dir):
        """Raise ValueError when the column does not exist in the dataset."""
        uri = _make_vector_dataset(temp_dir)

        with pytest.raises(ValueError, match="not found in dataset"):
            create_vector_index(uri, column="nonexistent_column")

    def test_invalid_column_not_vector(self, temp_dir):
        """Raise ValueError when the column is not a vector (fixed-size list) type."""
        uri = _make_vector_dataset(temp_dir)

        with pytest.raises(ValueError, match="not a vector type"):
            create_vector_index(uri, column="id")

    def test_invalid_index_type(self, temp_dir):
        """Raise ValueError for unsupported index type."""
        uri = _make_vector_dataset(temp_dir)

        with pytest.raises(ValueError, match="Unsupported vector index type"):
            create_vector_index(uri, column="vector", index_type="INVERTED")

    def test_vector_search_after_index_creation(self, temp_dir):
        """Verify that vector search works after creating an index."""
        uri = _make_vector_dataset(temp_dir, num_rows=256, dim=8)

        create_vector_index(
            uri,
            column="vector",
            index_type="IVF_PQ",
            num_partitions=2,
            num_sub_vectors=2,
        )

        # Read with vector search
        query = pa.array([1.0] * 8, type=pa.float32())
        nearest = {"column": "vector", "q": query, "k": 5}
        df = daft.read_lance(uri, default_scan_options={"nearest": nearest})
        result = df.select("id").to_pydict()

        assert len(result["id"]) == 5

    def test_case_insensitive_index_type(self, temp_dir):
        """Index type matching should be case-insensitive."""
        uri = _make_vector_dataset(temp_dir)

        # lowercase should work
        create_vector_index(uri, column="vector", index_type="ivf_pq", num_partitions=2, num_sub_vectors=2)

        ds = lance.dataset(uri)
        indices = ds.describe_indices()
        assert len(indices) == 1

    def test_kwargs_forwarded(self, temp_dir):
        """Extra kwargs should be forwarded to lance without error."""
        uri = _make_vector_dataset(temp_dir)

        # filter_nan is a valid lance create_index kwarg
        create_vector_index(
            uri,
            column="vector",
            num_partitions=2,
            num_sub_vectors=2,
            filter_nan=True,
        )

        ds = lance.dataset(uri)
        indices = ds.describe_indices()
        assert len(indices) == 1
