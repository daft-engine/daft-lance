from __future__ import annotations

import tempfile
from pathlib import Path

import lance
import pytest

import daft
from daft.dependencies import pd
from daft_lance import create_scalar_index


@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture
def multi_fragment_lance_dataset(temp_dir):
    """Create a Lance dataset with multiple fragments for testing."""
    path = Path(temp_dir) / "multi_fragment_text.lance"
    # Create dataset with multiple fragments (2 rows per fragment)
    text_data = {
        "id": [1, 2, 3, 4, 5, 6, 7, 8],
        "text": [
            "The quick brown fox jumps over the lazy dog",
            "Python is a powerful programming language",
            "Machine learning algorithms are fascinating",
            "Data science requires statistical knowledge",
            "Natural language processing uses text analysis",
            "Distributed computing scales horizontally",
            "Daft framework enables parallel processing",
            "Lance format provides efficient storage",
        ],
        "category": [
            "animals",
            "tech",
            "ml",
            "data",
            "nlp",
            "distributed",
            "daft",
            "storage",
        ],
    }
    text_dataset = daft.from_pydict(text_data)
    text_dataset.write_lance(uri=path, max_rows_per_file=2)
    return str(path)


def generate_multi_fragment_dataset(tmp_path, num_fragments=4, rows_per_fragment=250):
    """Generate a test dataset with multiple fragments."""
    all_data = []
    for frag_idx in range(num_fragments):
        for row_idx in range(rows_per_fragment):
            row_id = frag_idx * rows_per_fragment + row_idx
            all_data.append(
                {
                    "id": row_id,
                    "text": f"This is test document {row_id} with some sample text content for fragment {frag_idx}",
                    "fragment_id": frag_idx,
                }
            )

    df = pd.DataFrame(all_data)
    dataset = daft.from_pandas(df)

    path = Path(tmp_path) / "large_multi_fragment.lance"
    dataset.write_lance(uri=path, max_rows_per_file=rows_per_fragment)
    return str(path)


class TestDistributedIndexing:
    """Test cases for distributed indexing functionality."""

    def test_build_distributed_index_search_functionality(self, multi_fragment_lance_dataset):
        """Test that the built index actually works for searching."""
        dataset_uri = multi_fragment_lance_dataset

        # Build distributed index
        create_scalar_index(
            uri=dataset_uri,
            column="text",
            index_type="INVERTED",
        )
        updated_dataset = lance.dataset(dataset_uri)

        # Verify the index was created
        indices = updated_dataset.list_indices()
        assert len(indices) > 0, "No indices found after building"

        # Find our index
        text_index = None
        for idx in indices:
            if "text" in idx["name"]:
                text_index = idx
                break

        assert text_index is not None, "Text index not found"
        assert text_index["type"] == "Inverted", f"Expected Inverted index, got {text_index['type']}"

        # Test full-text search functionality
        search_term = "Python"
        results = updated_dataset.scanner(
            full_text_query=search_term,
            columns=["id", "text"],
        ).to_table()
        # Should find at least one result containing "Python"
        assert results.num_rows > 0, f"No results found for search term '{search_term}'"

        # Verify results contain the search term
        text_results = results.column("text").to_pylist()
        assert any(search_term in text for text in text_results), "Search results don't contain the search term"

    def test_build_distributed_index_with_name(self, multi_fragment_lance_dataset):
        """Test building distributed index with custom name."""
        dataset_uri = multi_fragment_lance_dataset
        custom_name = "custom_text_index"

        # Build distributed index with custom name
        create_scalar_index(
            uri=dataset_uri,
            column="text",
            index_type="INVERTED",
            name=custom_name,
        )
        updated_dataset = lance.dataset(dataset_uri)

        # Verify the index was created with correct name
        indices = updated_dataset.list_indices()
        index_names = [idx["name"] for idx in indices]
        assert custom_name in index_names, f"Custom index name '{custom_name}' not found in {index_names}"

    def test_build_distributed_index_large_dataset(self, temp_dir):
        """Test distributed indexing on a larger dataset with multiple fragments."""
        # Generate larger dataset
        dataset_uri = generate_multi_fragment_dataset(temp_dir, num_fragments=4, rows_per_fragment=50)

        # Build distributed index
        create_scalar_index(
            uri=dataset_uri,
            column="text",
            index_type="INVERTED",
            max_concurrency=4,
        )
        updated_dataset = lance.dataset(dataset_uri)

        # Verify the index was created
        indices = updated_dataset.list_indices()
        assert len(indices) > 0, "No indices found after building"

        # Test search functionality
        search_term = "test"
        results = updated_dataset.scanner(
            full_text_query=search_term,
            columns=["id", "text"],
        ).to_table()

        assert results.num_rows > 0, f"No results found for search term '{search_term}'"

    def test_build_distributed_index_invalid_column(self, multi_fragment_lance_dataset):
        """Test error handling for invalid column."""
        dataset_uri = multi_fragment_lance_dataset

        with pytest.raises(ValueError, match="Column 'nonexistent' not found"):
            create_scalar_index(
                uri=dataset_uri,
                column="nonexistent",
                index_type="INVERTED",
            )

    @pytest.mark.xfail(reason="Lance distributed index API compatibility")
    def test_build_distributed_index_invalid_index_type(self, multi_fragment_lance_dataset):
        """Test error handling for invalid index type."""
        dataset_uri = multi_fragment_lance_dataset

        with pytest.raises(
            NotImplementedError,
            match=r'Only "BTREE", "BITMAP", "NGRAM", "ZONEMAP", "LABEL_LIST", or "INVERTED" or "BLOOMFILTER" are supported for scalar columns.  Received INVALID',
        ):
            create_scalar_index(
                uri=dataset_uri,
                column="text",
                index_type="INVALID",
            )

    def test_build_distributed_index_empty_column(self, multi_fragment_lance_dataset):
        """Test error handling for empty column name."""
        dataset_uri = multi_fragment_lance_dataset

        with pytest.raises(ValueError, match="Column name cannot be empty"):
            create_scalar_index(
                uri=dataset_uri,
                column="",
                index_type="INVERTED",
            )

    def test_build_distributed_index_non_string_column(self, temp_dir):
        """Test error handling for non-string column."""
        # Create dataset with non-string column
        data = pd.DataFrame(
            {
                "id": [1, 2, 3, 4],
                "numeric_col": [10, 20, 30, 40],
                "text": ["text1", "text2", "text3", "text4"],
            }
        )
        dataset = daft.from_pandas(data)
        path = Path(temp_dir) / "non_string_test.lance"
        dataset.write_lance(uri=path, max_rows_per_file=2)

        with pytest.raises(TypeError, match="Column numeric_col must be string type"):
            create_scalar_index(
                uri=path,
                column="numeric_col",
                index_type="INVERTED",
            )

    def test_build_distributed_index_with_storage_options(self, multi_fragment_lance_dataset):
        """Test building distributed index with storage options."""
        dataset_uri = multi_fragment_lance_dataset

        # Build distributed index with storage options
        create_scalar_index(
            uri=dataset_uri,
            column="text",
            index_type="INVERTED",
            storage_options={},  # Empty storage options should work
        )

        updated_dataset = lance.dataset(dataset_uri)
        indices = updated_dataset.list_indices()
        assert len(indices) > 0, "No indices found after building"

    def test_build_distributed_index_with_kwargs(self, multi_fragment_lance_dataset):
        """Test building distributed index with additional kwargs."""
        dataset_uri = multi_fragment_lance_dataset

        # Build distributed index with additional kwargs
        create_scalar_index(
            uri=dataset_uri,
            column="text",
            index_type="INVERTED",
            remove_stop_words=False,  # Additional kwarg for create_scalar_index
        )

        updated_dataset = lance.dataset(dataset_uri)
        indices = updated_dataset.list_indices()
        assert len(indices) > 0, "No indices found after building"

    def test_build_distributed_index_replace_false_existing_index(self, multi_fragment_lance_dataset):
        """Test that replace=False raises error when trying to create index with existing name."""
        dataset_uri = multi_fragment_lance_dataset
        index_name = "test_replace_false_index"

        # First, create an index
        create_scalar_index(
            uri=dataset_uri,
            column="text",
            index_type="INVERTED",
            name=index_name,
        )

        updated_dataset = lance.dataset(dataset_uri)
        indices = updated_dataset.list_indices()
        assert len(indices) > 0, "Initial index creation failed"

        # Now try to create another index with the same name but replace=False
        # The error might be raised as RuntimeError during distributed processing
        with pytest.raises((ValueError, RuntimeError)) as exc_info:
            create_scalar_index(
                uri=dataset_uri,
                column="text",
                index_type="INVERTED",
                name=index_name,
                replace=False,
            )

        # Verify the error message contains information about existing index
        error_msg = str(exc_info.value)
        assert "already exists" in error_msg and index_name in error_msg

    def test_build_distributed_index_replace_true_overwrite_existing(self, multi_fragment_lance_dataset):
        """Test that replace=True successfully overwrites existing index."""
        dataset_uri = multi_fragment_lance_dataset
        index_name = "test_replace_true_index"

        # First, create an index
        create_scalar_index(
            uri=dataset_uri,
            column="text",
            index_type="INVERTED",
            name=index_name,
        )

        updated_dataset = lance.dataset(dataset_uri)
        initial_indices = updated_dataset.list_indices()
        assert len(initial_indices) > 0, "Initial index creation failed"

        # Find our initial index
        initial_index = None
        for idx in initial_indices:
            if idx["name"] == index_name:
                initial_index = idx
                break
        assert initial_index is not None, "Initial index not found"

        # Now create another index with the same name but replace=True
        create_scalar_index(
            uri=dataset_uri,
            column="text",
            index_type="INVERTED",
            name=index_name,
            replace=True,
        )

        updated_dataset = lance.dataset(dataset_uri)
        final_indices = updated_dataset.list_indices()
        final_index = None
        for idx in final_indices:
            if idx["name"] == index_name:
                final_index = idx
                break

        assert final_index is not None, "Index should still exist after replacement"
        assert final_index["type"] == "Inverted", "Index type should remain Inverted"

        # Test that the replaced index still works for searching
        search_term = "Python"
        results = updated_dataset.scanner(
            full_text_query=search_term,
            columns=["id", "text"],
        ).to_table()

        assert results.num_rows > 0, f"No results found for search term '{search_term}' after index replacement"

    def test_build_distributed_index_auto_adjust_workers(self, temp_dir):
        """Test that concurrency is automatically adjusted if it exceeds fragment count."""
        # Create dataset with only 2 fragments
        data = {
            "id": [1, 2, 3, 4],
            "text": ["text1", "text2", "text3", "text4"],
        }
        dataset = daft.from_pydict(data)
        path = Path(temp_dir) / "small_dataset.lance"
        dataset.write_lance(uri=path, max_rows_per_file=2)

        # Request more workers than fragments
        create_scalar_index(
            uri=path,
            column="text",
            index_type="INVERTED",
            max_concurrency=10,
        )

        # Should still work and create the index
        updated_dataset = lance.dataset(path)
        indices = updated_dataset.list_indices()
        assert len(indices) > 0, "No indices found after building"

    def test_build_distributed_index_fragment_group_size(self, multi_fragment_lance_dataset):
        """Test building distributed index with fragment_group_size parameter."""
        dataset_uri = multi_fragment_lance_dataset

        # Build distributed index with custom fragment_group_size
        create_scalar_index(
            uri=dataset_uri,
            column="text",
            index_type="INVERTED",
            fragment_group_size=2,
            max_concurrency=2,
        )

        updated_dataset = lance.dataset(dataset_uri)
        indices = updated_dataset.list_indices()
        assert len(indices) > 0, "No indices found after building"

    def test_build_distributed_index_partition_num(self, multi_fragment_lance_dataset):
        """Test building distributed index with num_partitions parameter."""
        dataset_uri = multi_fragment_lance_dataset

        # Build distributed index with custom num_partitions
        create_scalar_index(
            uri=dataset_uri,
            column="text",
            index_type="INVERTED",
            num_partitions=2,
            max_concurrency=2,
        )

        updated_dataset = lance.dataset(dataset_uri)
        indices = updated_dataset.list_indices()
        assert len(indices) > 0, "No indices found after building"

    def test_build_distributed_index_fts_type(self, multi_fragment_lance_dataset):
        """Test building distributed FTS (Full-Text Search) index."""
        dataset_uri = multi_fragment_lance_dataset

        # Skip this test if FTS is not supported in the current LanceDB version
        # This test will be enabled when LanceDB version supports FTS index type
        pytest.skip("FTS index type may not be supported in the current LanceDB version")

        # Build distributed FTS index
        create_scalar_index(
            uri=dataset_uri,
            column="text",
            index_type="FTS",
            max_concurrency=2,
        )

        updated_dataset = lance.dataset(dataset_uri)
        indices = updated_dataset.list_indices()
        assert len(indices) > 0, "No indices found after building"

        # Test search functionality
        search_term = "Python"
        results = updated_dataset.scanner(
            full_text_query=search_term,
            columns=["id", "text"],
        ).to_table()

        assert results.num_rows > 0, f"No results found for search term '{search_term}'"

    def test_build_distributed_index_btree_type(self, temp_dir):
        """Test building distributed BTREE index."""
        # Create dataset with numeric column
        data = {
            "id": [1, 2, 3, 4, 5, 6, 7, 8],
            "price": [10.5, 20.75, 30.0, 40.25, 50.5, 60.75, 70.0, 80.25],
            "name": ["item1", "item2", "item3", "item4", "item5", "item6", "item7", "item8"],
        }
        dataset = daft.from_pydict(data)
        path = Path(temp_dir) / "btree_test.lance"
        dataset.write_lance(uri=path, max_rows_per_file=2)

        # Build distributed BTREE index on numeric column
        create_scalar_index(
            uri=path,
            column="price",
            index_type="BTREE",
            name="price_btree_index",
            max_concurrency=2,
        )

        updated_dataset = lance.dataset(path)
        indices = updated_dataset.list_indices()
        assert len(indices) > 0, "No indices found after building"
        index_names = [idx["name"] for idx in indices]
        assert "price_btree_index" in index_names, f"BTREE index not found in {index_names}"

        # Test that we can query using the index
        results = updated_dataset.scanner(
            filter="price > 30.0",
            columns=["id", "price", "name"],
        ).to_table()

        assert results.num_rows > 0, "No results found for BTREE index query"

    def test_build_distributed_index_with_all_params(self, temp_dir):
        """Test building distributed index with all new parameters together."""
        # Create dataset with multiple fragments
        data = {
            "id": [i for i in range(16)],
            "text": [f"This is test document {i}" for i in range(16)],
            "category": [f"cat_{i % 4}" for i in range(16)],
        }
        dataset = daft.from_pydict(data)
        path = Path(temp_dir) / "all_params_test.lance"
        dataset.write_lance(uri=path, max_rows_per_file=2)

        # Build distributed index with all new parameters
        create_scalar_index(
            uri=path,
            column="text",
            index_type="INVERTED",
            name="comprehensive_index",
            replace=True,
            fragment_group_size=3,
            num_partitions=4,
            max_concurrency=2,
        )

        updated_dataset = lance.dataset(path)
        indices = updated_dataset.list_indices()
        assert len(indices) > 0, "No indices found after building"

        # Verify the index works
        search_term = "test"
        results = updated_dataset.scanner(
            full_text_query=search_term,
            columns=["id", "text"],
        ).to_table()

        assert results.num_rows > 0, f"No results found for search term '{search_term}'"

    def test_build_distributed_index_no_fragments(self, temp_dir):
        """Test distributed indexing when dataset has no fragments (empty dataset)."""
        # Create empty dataset with explicit schema
        import pyarrow as pa

        # Create empty table with string type for 'text' column
        schema = pa.schema([("id", pa.int64()), ("text", pa.string())])
        empty_table = pa.Table.from_arrays(
            [pa.array([], type=pa.int64()), pa.array([], type=pa.string())], schema=schema
        )
        dataset = daft.from_arrow(empty_table)

        path = Path(temp_dir) / "empty_dataset.lance"
        dataset.write_lance(uri=path)

        # Try to build index on empty dataset
        create_scalar_index(
            uri=path,
            column="text",
            index_type="INVERTED",
        )

        # Verify no index was created (since no data)
        updated_dataset = lance.dataset(path)
        indices = updated_dataset.list_indices()
        assert len(indices) == 0, f"Expected no indices for empty dataset, got {len(indices)}"

    def test_build_distributed_index_zonemap_type(self, temp_dir):
        """Test building ZONEMAP index on numeric column (falls back to single-threaded)."""
        data = {
            "id": [1, 2, 3, 4, 5, 6, 7, 8],
            "price": [10.5, 20.75, 30.0, 40.25, 50.5, 60.75, 70.0, 80.25],
            "name": ["item1", "item2", "item3", "item4", "item5", "item6", "item7", "item8"],
        }
        dataset = daft.from_pydict(data)
        path = Path(temp_dir) / "zonemap_test.lance"
        dataset.write_lance(uri=path, max_rows_per_file=2)

        # ZONEMAP is not supported by merge_index_metadata, so it falls back
        # to single-threaded creation via Lance's create_scalar_index.
        create_scalar_index(
            uri=path,
            column="price",
            index_type="ZONEMAP",
            name="price_zonemap_index",
        )

        updated_dataset = lance.dataset(path)
        indices = updated_dataset.list_indices()
        assert len(indices) > 0, "No indices found after building"
        index_names = [idx["name"] for idx in indices]
        assert "price_zonemap_index" in index_names, f"ZONEMAP index not found in {index_names}"

        # Test that we can query using the index
        results = updated_dataset.scanner(
            filter="price > 30.0",
            columns=["id", "price", "name"],
        ).to_table()
        assert results.num_rows > 0, "No results found for ZONEMAP index query"

    def test_build_distributed_index_zonemap_integer_column(self, temp_dir):
        """Test building ZONEMAP index on integer column (falls back to single-threaded)."""
        data = {
            "id": [1, 2, 3, 4, 5, 6, 7, 8],
            "score": [100, 200, 300, 400, 500, 600, 700, 800],
        }
        dataset = daft.from_pydict(data)
        path = Path(temp_dir) / "zonemap_int_test.lance"
        dataset.write_lance(uri=path, max_rows_per_file=2)

        create_scalar_index(
            uri=path,
            column="score",
            index_type="ZONEMAP",
            name="score_zonemap_index",
        )

        updated_dataset = lance.dataset(path)
        indices = updated_dataset.list_indices()
        index_names = [idx["name"] for idx in indices]
        assert "score_zonemap_index" in index_names, f"ZONEMAP index not found in {index_names}"

    def test_build_distributed_index_zonemap_invalid_string_column(self, temp_dir):
        """Test that ZONEMAP index rejects string columns via Lance."""
        data = {
            "id": [1, 2, 3, 4],
            "text": ["a", "b", "c", "d"],
        }
        dataset = daft.from_pydict(data)
        path = Path(temp_dir) / "zonemap_string_test.lance"
        dataset.write_lance(uri=path, max_rows_per_file=2)

        # ZONEMAP falls back to single-threaded Lance, which will reject
        # unsupported column types at the Lance level.
        with pytest.raises(Exception):
            create_scalar_index(
                uri=path,
                column="text",
                index_type="ZONEMAP",
            )


class TestSegmentedBTreeIndex:
    """Test cases for segmented BTree index functionality."""

    def test_segmented_btree_basic(self, temp_dir):
        """Test basic segmented BTree index creation and query."""
        data = {
            "id": [1, 2, 3, 4, 5, 6, 7, 8],
            "price": [10.5, 20.75, 30.0, 40.25, 50.5, 60.75, 70.0, 80.25],
            "name": ["item1", "item2", "item3", "item4", "item5", "item6", "item7", "item8"],
        }
        dataset = daft.from_pydict(data)
        path = Path(temp_dir) / "segmented_btree_basic.lance"
        dataset.write_lance(uri=path, max_rows_per_file=2)

        create_scalar_index(
            uri=path,
            column="price",
            index_type="BTREE",
            name="price_seg_idx",
            segmented=True,
            max_concurrency=2,
        )

        updated_dataset = lance.dataset(path)

        # describe_indices must work (the whole point of the segmented flow)
        described = updated_dataset.describe_indices()
        assert len(described) == 1
        assert described[0].name == "price_seg_idx"
        assert "BTree" in described[0].type_url

        # Query must work
        results = updated_dataset.scanner(
            filter="price > 50.0",
            columns=["id", "price"],
        ).to_table()
        assert results.num_rows == 4
        prices = sorted(results.column("price").to_pylist())
        assert prices == [50.5, 60.75, 70.0, 80.25]

    def test_segmented_btree_multiple_segments(self, temp_dir):
        """Test segmented BTree with small fragment_group_size to force multiple segments."""
        data = {
            "id": list(range(16)),
            "score": [i * 10.0 for i in range(16)],
        }
        dataset = daft.from_pydict(data)
        path = Path(temp_dir) / "segmented_btree_multi.lance"
        dataset.write_lance(uri=path, max_rows_per_file=2)

        create_scalar_index(
            uri=path,
            column="score",
            index_type="BTREE",
            name="score_seg_idx",
            segmented=True,
            fragment_group_size=2,
            max_concurrency=2,
        )

        updated_dataset = lance.dataset(path)
        described = updated_dataset.describe_indices()
        assert len(described) == 1
        assert len(described[0].segments) >= 2, "Expected multiple segments with fragment_group_size=2"

        # Query must work across segment boundaries
        results = updated_dataset.scanner(
            filter="score >= 100.0",
            columns=["id", "score"],
        ).to_table()
        assert results.num_rows == 6  # ids 10..15

    def test_segmented_btree_describe_indices_works(self, temp_dir):
        """Test that describe_indices returns valid details for segmented index.

        This is the core regression that the segmented flow resolves: the legacy
        partitioned-and-merged flow produces indices with empty index_details,
        causing describe_indices() to fail.
        """
        data = {
            "id": [1, 2, 3, 4],
            "value": [100, 200, 300, 400],
        }
        dataset = daft.from_pydict(data)
        path = Path(temp_dir) / "segmented_describe.lance"
        dataset.write_lance(uri=path, max_rows_per_file=2)

        create_scalar_index(
            uri=path,
            column="value",
            index_type="BTREE",
            name="value_idx",
            segmented=True,
        )

        updated_dataset = lance.dataset(path)

        # This must not raise — it would with the legacy flow.
        described = updated_dataset.describe_indices()
        assert len(described) == 1
        desc = described[0]
        assert desc.name == "value_idx"
        assert desc.type_url == "/lance.table.BTreeIndexDetails"
        assert desc.num_rows_indexed == 4

    def test_segmented_btree_replace_existing(self, temp_dir):
        """Test replacing an existing segmented BTree index."""
        data = {
            "id": [1, 2, 3, 4, 5, 6, 7, 8],
            "price": [10.5, 20.75, 30.0, 40.25, 50.5, 60.75, 70.0, 80.25],
        }
        dataset = daft.from_pydict(data)
        path = Path(temp_dir) / "segmented_btree_replace.lance"
        dataset.write_lance(uri=path, max_rows_per_file=2)

        # Create initial index
        create_scalar_index(
            uri=path,
            column="price",
            index_type="BTREE",
            name="price_idx",
            segmented=True,
        )

        ds1 = lance.dataset(path)
        assert len(ds1.describe_indices()) == 1

        # Replace it
        create_scalar_index(
            uri=path,
            column="price",
            index_type="BTREE",
            name="price_idx",
            segmented=True,
            replace=True,
        )

        ds2 = lance.dataset(path)
        described = ds2.describe_indices()
        assert len(described) == 1
        assert described[0].name == "price_idx"

        # Query still works after replacement
        results = ds2.scanner(filter="price > 50.0", columns=["id", "price"]).to_table()
        assert results.num_rows == 4

    def test_segmented_btree_string_column(self, temp_dir):
        """Test segmented BTree index on a string column."""
        import pyarrow as pa

        # Explicitly use pa.string() (not large_string) to satisfy the BTREE type check.
        table = pa.table(
            {
                "id": pa.array([1, 2, 3, 4], type=pa.int64()),
                "category": pa.array(["alpha", "beta", "gamma", "delta"], type=pa.string()),
            }
        )
        path = Path(temp_dir) / "segmented_btree_string.lance"
        lance.write_dataset(table, str(path), max_rows_per_file=2)

        create_scalar_index(
            uri=path,
            column="category",
            index_type="BTREE",
            name="cat_idx",
            segmented=True,
        )

        updated_dataset = lance.dataset(path)
        described = updated_dataset.describe_indices()
        assert len(described) == 1
        assert described[0].name == "cat_idx"
        assert "BTree" in described[0].type_url

    def test_segmented_btree_integer_column(self, temp_dir):
        """Test segmented BTree index on an integer column."""
        data = {
            "id": [1, 2, 3, 4, 5, 6, 7, 8],
            "count": [100, 200, 300, 400, 500, 600, 700, 800],
        }
        dataset = daft.from_pydict(data)
        path = Path(temp_dir) / "segmented_btree_int.lance"
        dataset.write_lance(uri=path, max_rows_per_file=2)

        create_scalar_index(
            uri=path,
            column="count",
            index_type="BTREE",
            name="count_idx",
            segmented=True,
        )

        updated_dataset = lance.dataset(path)
        described = updated_dataset.describe_indices()
        assert len(described) == 1
        assert described[0].name == "count_idx"

        results = updated_dataset.scanner(filter="count > 500", columns=["id", "count"]).to_table()
        assert results.num_rows == 3  # 600, 700, 800

    def test_segmented_false_uses_legacy_flow(self, temp_dir):
        """Test that segmented=False still uses the legacy partitioned-and-merged flow."""
        data = {
            "id": [1, 2, 3, 4, 5, 6, 7, 8],
            "price": [10.5, 20.75, 30.0, 40.25, 50.5, 60.75, 70.0, 80.25],
        }
        dataset = daft.from_pydict(data)
        path = Path(temp_dir) / "segmented_false.lance"
        dataset.write_lance(uri=path, max_rows_per_file=2)

        # segmented=False (default) should use the legacy flow
        create_scalar_index(
            uri=path,
            column="price",
            index_type="BTREE",
            name="price_legacy_idx",
            segmented=False,
        )

        updated_dataset = lance.dataset(path)
        indices = updated_dataset.list_indices()
        index_names = [idx["name"] for idx in indices]
        assert "price_legacy_idx" in index_names

    def test_segmented_inverted_falls_back_to_legacy(self, multi_fragment_lance_dataset):
        """Test that segmented=True with INVERTED still uses the legacy flow.

        The segmented workflow currently only supports BTREE.
        """
        dataset_uri = multi_fragment_lance_dataset

        # segmented=True but INVERTED => legacy flow is used
        create_scalar_index(
            uri=dataset_uri,
            column="text",
            index_type="INVERTED",
            name="text_inv_idx",
            segmented=True,
        )

        updated_dataset = lance.dataset(dataset_uri)
        indices = updated_dataset.list_indices()
        index_names = [idx["name"] for idx in indices]
        assert "text_inv_idx" in index_names
