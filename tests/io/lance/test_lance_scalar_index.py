from __future__ import annotations

import pickle
import tempfile
from inspect import signature
from pathlib import Path
from typing import Any, cast

import lance
import pytest

import daft
from daft.dependencies import pa, pd
from daft_lance import create_scalar_index, lance_scalar_index
from daft_lance.lance_scalar_index import (
    SegmentedFragmentIndexHandler,
    _existing_index_coverage,
    create_scalar_index_internal,
)
from daft_lance.namespace import DatasetOpenContext


class FakeOpenContext:
    """Stands in for DatasetOpenContext so handlers can be driven against a fake dataset.

    Also counts opens, which is what proves a handler reopens once per instance
    rather than once per call.
    """

    def __init__(self, dataset, uri="memory://fake"):
        self.dataset = dataset
        self.uri = uri
        self.opens = 0

    def open_pinned(self):
        self.opens += 1
        return self.dataset

    def open_latest(self):
        return self.open_pinned()


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

    def test_replace_defaults_to_true(self) -> None:
        """Replacement is opt-out, matching pylance's default."""
        assert signature(create_scalar_index).parameters["replace"].default is True
        assert signature(create_scalar_index_internal).parameters["replace"].default is True

    def test_segmented_kwarg_is_rejected_loudly(self) -> None:
        """The removed segmented parameter must fail at the API boundary."""
        with pytest.raises(TypeError, match="'segmented' parameter was removed"):
            create_scalar_index_internal(
                lance_ds=cast(Any, None),
                open_context=cast(Any, None),
                column="a",
                index_type="INVERTED",
                segmented=True,
            )

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

        # Verify the index was created.
        indices = updated_dataset.describe_indices()
        index_names = [idx.name for idx in indices]
        assert "text_idx" in index_names, f"Text index not found in {index_names}"

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
        indices = updated_dataset.describe_indices()
        index_names = [idx.name for idx in indices]
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
        indices = updated_dataset.describe_indices()
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
        indices = updated_dataset.describe_indices()
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
        indices = updated_dataset.describe_indices()
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
        indices = updated_dataset.describe_indices()
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

    def test_build_distributed_index_replace_true_overwrites_existing(self, multi_fragment_lance_dataset):
        """Test that default non-segmented replace=True overwrites existing indexes."""
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
        initial_indices = updated_dataset.describe_indices()
        assert len(initial_indices) > 0, "Initial index creation failed"

        # Find our initial index
        initial_index = next((idx for idx in initial_indices if idx.name == index_name), None)
        assert initial_index is not None, "Initial index not found"

        create_scalar_index(
            uri=dataset_uri,
            column="text",
            index_type="INVERTED",
            name=index_name,
            replace=True,
        )

        updated_dataset = lance.dataset(dataset_uri)
        final_indices = updated_dataset.describe_indices()
        final_index = next((idx for idx in final_indices if idx.name == index_name), None)

        assert final_index is not None, "Index should still exist after replacement"

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
        indices = updated_dataset.describe_indices()
        assert len(indices) > 0, "No indices found after building"

    def test_build_distributed_index_fragment_group_size(self, multi_fragment_lance_dataset):
        """Test distributed INVERTED indexes built from multiple fragment groups."""
        dataset_uri = multi_fragment_lance_dataset
        index_name = "text_fragment_group_idx"

        # Build distributed index with custom fragment_group_size
        create_scalar_index(
            uri=dataset_uri,
            column="text",
            index_type="INVERTED",
            name=index_name,
            fragment_group_size=2,
            max_concurrency=2,
        )

        updated_dataset = lance.dataset(dataset_uri)
        indices = updated_dataset.describe_indices()
        index_names = [idx.name for idx in indices]
        assert index_name in index_names, f"Index {index_name!r} not found in {index_names}"

        results = updated_dataset.scanner(
            full_text_query="Python",
            columns=["id", "text"],
        ).to_table()
        assert results.num_rows > 0

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
        indices = updated_dataset.describe_indices()
        assert len(indices) > 0, "No indices found after building"

    def test_build_distributed_index_fts_type(self, multi_fragment_lance_dataset):
        """Test building distributed FTS (Full-Text Search) index."""
        dataset_uri = multi_fragment_lance_dataset
        index_name = "text_fts_idx"

        # Build distributed FTS index
        create_scalar_index(
            uri=dataset_uri,
            column="text",
            index_type="FTS",
            name=index_name,
            max_concurrency=2,
        )

        updated_dataset = lance.dataset(dataset_uri)
        indices = updated_dataset.describe_indices()
        index_names = [idx.name for idx in indices]
        assert index_name in index_names, f"FTS index not found in {index_names}"

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
        indices = updated_dataset.describe_indices()
        assert len(indices) > 0, "No indices found after building"
        index_names = [idx.name for idx in indices]
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
        indices = updated_dataset.describe_indices()
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

        # Building an index on an empty dataset fails loudly (no silent no-op).
        with pytest.raises(ValueError, match="contains no fragments"):
            create_scalar_index(
                uri=path,
                column="text",
                index_type="INVERTED",
            )

    def test_build_distributed_index_zonemap_type(self, temp_dir):
        """Test building ZONEMAP index distributed on a numeric column."""
        data = {
            "id": [1, 2, 3, 4, 5, 6, 7, 8],
            "price": [10.5, 20.75, 30.0, 40.25, 50.5, 60.75, 70.0, 80.25],
            "name": ["item1", "item2", "item3", "item4", "item5", "item6", "item7", "item8"],
        }
        dataset = daft.from_pydict(data)
        path = Path(temp_dir) / "zonemap_test.lance"
        dataset.write_lance(uri=path, max_rows_per_file=2)

        create_scalar_index(
            uri=path,
            column="price",
            index_type="ZONEMAP",
            name="price_zonemap_index",
        )

        updated_dataset = lance.dataset(path)
        indices = updated_dataset.describe_indices()
        assert len(indices) > 0, "No indices found after building"
        index_names = [idx.name for idx in indices]
        assert "price_zonemap_index" in index_names, f"ZONEMAP index not found in {index_names}"

        # Test that we can query using the index
        results = updated_dataset.scanner(
            filter="price > 30.0",
            columns=["id", "price", "name"],
        ).to_table()
        assert results.num_rows > 0, "No results found for ZONEMAP index query"

    def test_build_distributed_index_zonemap_integer_column(self, temp_dir):
        """Test building ZONEMAP index distributed on an integer column."""
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
        indices = updated_dataset.describe_indices()
        index_names = [idx.name for idx in indices]
        assert "score_zonemap_index" in index_names, f"ZONEMAP index not found in {index_names}"

    def test_build_distributed_index_zonemap_string_column(self, temp_dir):
        """Test that ZONEMAP index builds on string columns (supported since Lance 11)."""
        data = {
            "id": [1, 2, 3, 4],
            "text": ["a", "b", "c", "d"],
        }
        dataset = daft.from_pydict(data)
        path = Path(temp_dir) / "zonemap_string_test.lance"
        dataset.write_lance(uri=path, max_rows_per_file=2)

        # ZONEMAP falls back to single-threaded Lance index creation.
        create_scalar_index(
            uri=path,
            column="text",
            index_type="ZONEMAP",
            name="text_zonemap_index",
        )

        index_names = [idx.name for idx in lance.dataset(path).describe_indices()]
        assert "text_zonemap_index" in index_names, f"ZONEMAP index not found in {index_names}"


class TestSegmentedBTreeIndex:
    """Test cases for segmented BTree index functionality."""

    def test_segmented_handler_uses_public_uncommitted_index_api(self):
        """Test that segmented workers use Lance's public uncommitted index API."""

        class FakeLanceDataset:
            def __init__(self):
                self.calls = []

            @property
            def _ds(self):
                raise AssertionError("segmented index creation must not use private _ds.create_index")

            def create_index_uncommitted(self, **kwargs):
                self.calls.append(kwargs)
                return {"segment": "metadata"}

        fake_ds = FakeLanceDataset()
        handler = SegmentedFragmentIndexHandler(
            open_context=FakeOpenContext(fake_ds),
            column="price",
            index_type="BTREE",
            name="price_idx",
            custom="value",
        )

        raw_segment = handler([1, 2])

        assert pickle.loads(raw_segment) == {"segment": "metadata"}
        assert fake_ds.calls == [
            {
                "column": "price",
                "index_type": "BTREE",
                "name": "price_idx",
                "replace": False,
                "train": True,
                "fragment_ids": [1, 2],
                "custom": "value",
            }
        ]

    def test_segments_are_committed_without_merge(self):
        """Worker-built segments commit as-is, with no merge step.

        Multi-segment indexes are fully functional (verified: split segments
        load and prune at query time with identical scores); compaction is
        left to optimize_indices.
        """
        # The merge helper is gone from the module entirely.
        assert not hasattr(lance_scalar_index, "_prepare_index_segments_for_commit")
        assert not hasattr(lance_scalar_index, "MERGED_SEGMENTED_INDEX_TYPES")

    def test_existing_index_coverage_when_describe_indices_fails(self, caplog):
        """A describe_indices() failure reads as "no existing index"."""

        class FakeLanceDataset:
            def describe_indices(self):
                raise RuntimeError("missing index_details")

        with caplog.at_level("WARNING", logger="daft_lance.lance_scalar_index"):
            assert _existing_index_coverage(FakeLanceDataset(), "idx") is None
        assert "describe_indices() failed" in caplog.text

    def test_segmented_bitmap_handler_builds_without_shard_id(self):
        """BITMAP segment creation needs no shard id; segments commit as-is."""

        class FakeLanceDataset:
            def __init__(self) -> None:
                self.calls = []

            def create_index_uncommitted(self, **kwargs):
                self.calls.append(kwargs)
                return {"segment": "bitmap-metadata"}

        fake_ds = FakeLanceDataset()
        handler = SegmentedFragmentIndexHandler(
            open_context=FakeOpenContext(fake_ds),
            column="flag",
            index_type="BITMAP",
            name="flag_idx",
        )

        raw_segment = handler([1, 2])

        assert pickle.loads(raw_segment) == {"segment": "bitmap-metadata"}
        assert fake_ds.calls == [
            {
                "column": "flag",
                "index_type": "BITMAP",
                "name": "flag_idx",
                "replace": False,
                "train": True,
                "fragment_ids": [1, 2],
            }
        ]

    def test_segmented_bitmap_respects_fragment_group_size(self, monkeypatch):
        """Test that segmented BITMAP can group multiple fragments per segment."""

        class FakeFragment:
            def __init__(self, fragment_id: int) -> None:
                self.fragment_id = fragment_id

            def count_rows(self) -> int:
                return 1

        class FakeLanceDataset:
            schema = pa.schema([("flag", pa.int64())])

            def describe_indices(self):
                return []

            def get_fragments(self):
                return [FakeFragment(i) for i in range(4)]

        calls = []

        def fake_create_segmented_index(**kwargs):
            calls.append(kwargs)

        monkeypatch.setattr(lance_scalar_index, "_create_segmented_index", fake_create_segmented_index)

        create_scalar_index_internal(
            lance_ds=FakeLanceDataset(),
            open_context=DatasetOpenContext(uri="memory://bitmap", version=1),
            column="flag",
            index_type="BITMAP",
            name="flag_bitmap_idx",
            fragment_group_size=2,
        )

        assert [len(group["fragment_ids"]) for group in calls[0]["fragment_data"]] == [2, 2]

    def test_replace_true_uses_atomic_overlap_replacement(self, monkeypatch):
        """replace=True replaces atomically, without dropping the old index.

        The segment commit retires the overlapped segments in the same
        transaction.
        """

        class FakeSegment:
            def __init__(self, fragment_ids: set[int]) -> None:
                self.fragment_ids = fragment_ids

        class ExistingIndex:
            name = "flag_bitmap_idx"
            index_type = "Bitmap"
            field_names = ["flag"]
            segments = [FakeSegment({0, 1})]

        class FakeFragment:
            def __init__(self, fragment_id: int) -> None:
                self.fragment_id = fragment_id

            def count_rows(self) -> int:
                return 1

        class FakeLanceDataset:
            schema = pa.schema([("flag", pa.int64())])

            def __init__(self) -> None:
                self.dropped: list[str] = []

            def describe_indices(self) -> list[Any]:
                return [ExistingIndex()]

            def drop_index(self, name: str) -> None:
                self.dropped.append(name)

            def get_fragments(self) -> list[Any]:
                return [FakeFragment(0), FakeFragment(1)]

        fake_ds = FakeLanceDataset()
        calls = []
        monkeypatch.setattr(lance_scalar_index, "_create_segmented_index", lambda **kwargs: calls.append(kwargs))

        create_scalar_index_internal(
            lance_ds=cast(Any, fake_ds),
            open_context=DatasetOpenContext(uri="memory://bitmap", version=1),
            column="flag",
            index_type="BITMAP",
            name="flag_bitmap_idx",
            replace=True,
        )

        # No drop: overlap replacement retires the old segments atomically.
        assert fake_ds.dropped == []
        # Workers still build with replace=True: the pinned snapshot has the name.
        assert calls[0]["handler_replace"] is True

    def test_replace_true_rebuilds_single_index_with_same_name(self, temp_dir):
        """Replacing an index must leave exactly one index behind, same name."""
        path = Path(temp_dir) / "bitmap_default_replace.lance"
        table = pa.table(
            {
                "flag": pa.array([1, 2, 1, 3, 2, 1, 3, 2], type=pa.int64()),
            }
        )
        lance.write_dataset(table, str(path), max_rows_per_file=2)

        create_scalar_index(uri=path, column="flag", index_type="BITMAP", name="flag_idx")
        assert len(lance.dataset(str(path)).describe_indices()) == 1

        create_scalar_index(uri=path, column="flag", index_type="BITMAP", name="flag_idx")

        described = lance.dataset(str(path)).describe_indices()
        assert len(described) == 1
        assert described[0].name == "flag_idx"
        assert described[0].num_rows_indexed == 8
        results = lance.dataset(str(path)).scanner(filter="flag = 1").to_table()
        assert results.num_rows == 3

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
        )

        updated_dataset = lance.dataset(path)

        # This must not raise — it would with the legacy flow.
        described = updated_dataset.describe_indices()
        assert len(described) == 1
        desc = described[0]
        assert desc.name == "value_idx"
        assert desc.type_url == "/lance.table.BTreeIndexDetails"
        assert desc.num_rows_indexed == 4

    def test_btree_replace_semantics(self, temp_dir):
        """replace=False rejects an existing name; replace=True rebuilds it."""
        data = {
            "id": [1, 2, 3, 4, 5, 6, 7, 8],
            "price": [10.5, 20.75, 30.0, 40.25, 50.5, 60.75, 70.0, 80.25],
        }
        dataset = daft.from_pydict(data)
        path = Path(temp_dir) / "segmented_btree_replace.lance"
        dataset.write_lance(uri=path, max_rows_per_file=2)

        create_scalar_index(
            uri=path,
            column="price",
            index_type="BTREE",
            name="price_idx",
        )
        assert len(lance.dataset(path).describe_indices()) == 1

        # replace=False refuses to touch the existing index.
        with pytest.raises(ValueError, match="already exists. Set replace=True"):
            create_scalar_index(
                uri=path,
                column="price",
                index_type="BTREE",
                name="price_idx",
                replace=False,
            )

        # Default (replace=True) drops and rebuilds; still exactly one index.
        create_scalar_index(
            uri=path,
            column="price",
            index_type="BTREE",
            name="price_idx",
        )
        ds2 = lance.dataset(path)
        described = ds2.describe_indices()
        assert len(described) == 1
        assert described[0].name == "price_idx"

        results = ds2.scanner(filter="price > 50.0", columns=["id", "price"]).to_table()
        assert results.num_rows == 4

    def test_replace_rebuild_advances_version_exactly_once(self, temp_dir):
        """Atomic overlap replacement lands as a single new dataset version."""
        data = {
            "id": [1, 2, 3, 4, 5, 6, 7, 8],
            "price": [10.5, 20.75, 30.0, 40.25, 50.5, 60.75, 70.0, 80.25],
        }
        dataset = daft.from_pydict(data)
        path = Path(temp_dir) / "atomic_replace.lance"
        dataset.write_lance(uri=path, max_rows_per_file=2)

        create_scalar_index(uri=path, column="price", index_type="BTREE", name="atomic_idx")
        version_before = lance.dataset(path).version

        create_scalar_index(uri=path, column="price", index_type="BTREE", name="atomic_idx")

        latest = lance.dataset(path)
        # One transaction: no intermediate drop version, no append duplication.
        assert latest.version == version_before + 1
        described = latest.describe_indices()
        assert len(described) == 1
        assert described[0].name == "atomic_idx"
        assert len(described[0].segments) == 1
        assert latest.scanner(filter="price > 50.0", columns=["id", "price"]).to_table().num_rows == 4

    def test_replace_with_stale_coverage_rebuilds_atomically(self, temp_dir) -> None:
        """Stale coverage from a fully deleted fragment retires atomically.

        A fully deleted fragment inside a mixed segment is the only stale
        coverage normal operations produce; the rebuild retires it without a
        drop, in exactly one new version.
        """
        data = {
            "id": list(range(80)),
            "name": [f"name-{i % 8}" for i in range(80)],
        }
        path = Path(temp_dir) / "stale_coverage.lance"
        lance.write_dataset(daft.from_pydict(data).to_arrow(), str(path), max_rows_per_file=20)

        create_scalar_index(uri=path, column="name", index_type="INVERTED", name="stale_idx")
        # Delete every row of fragment 0: the committed segment keeps its
        # (now partially dead) coverage {0,1,2,3}.
        lance.dataset(str(path)).delete("id < 20")

        version_before = lance.dataset(str(path)).version
        create_scalar_index(uri=path, column="name", index_type="INVERTED", name="stale_idx")

        latest = lance.dataset(str(path))
        assert latest.version == version_before + 1
        described = latest.describe_indices()[0]
        assert len(described.segments) == 1
        assert sorted(described.segments[0].fragment_ids) == [1, 2, 3]
        # ids 3, 11, 19 (name-3) were among the 20 deleted rows: 10 - 3 remain.
        assert latest.scanner(filter="name = 'name-3'").to_table().num_rows == 7

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
        )

        updated_dataset = lance.dataset(path)
        described = updated_dataset.describe_indices()
        assert len(described) == 1
        assert described[0].name == "count_idx"

        results = updated_dataset.scanner(filter="count > 500", columns=["id", "count"]).to_table()
        assert results.num_rows == 3  # 600, 700, 800

    def test_unsupported_type_raises_instead_of_single_node_fallback(self) -> None:
        """Types without a distributed path fail loudly; no silent fallback."""
        with pytest.raises(ValueError, match="Unsupported distributed index type 'RTREE'"):
            create_scalar_index_internal(
                lance_ds=cast(Any, None),
                open_context=cast(Any, None),
                column="geom",
                index_type="RTREE",
            )

    def test_segmented_inverted_creates_index(self, multi_fragment_lance_dataset):
        """INVERTED creates an index through the distributed segment workflow."""
        dataset_uri = multi_fragment_lance_dataset

        create_scalar_index(
            uri=dataset_uri,
            column="text",
            index_type="INVERTED",
            name="text_inv_idx",
        )

        updated_dataset = lance.dataset(dataset_uri)
        described = updated_dataset.describe_indices()
        text_index = next((idx for idx in described if idx.name == "text_inv_idx"), None)
        assert text_index is not None
        assert "Inverted" in text_index.type_url
