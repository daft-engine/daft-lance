from __future__ import annotations

import lance
import pyarrow as pa
import pytest

import daft
from daft_lance import create_scalar_index


def build_text_dataset(tmp_path_factory) -> str:
    tmp_dir = tmp_path_factory.mktemp("lance_fts")

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
    table = pa.table(text_data)
    lance.write_dataset(table, tmp_dir)
    return str(tmp_dir)


def build_multi_fragment_text_dataset(tmp_path_factory) -> str:
    tmp_dir = tmp_path_factory.mktemp("lance_fts_multi")

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
    }
    table = pa.table(text_data)
    lance.write_dataset(table, tmp_dir, max_rows_per_file=2)
    return str(tmp_dir)


def test_full_text_query_string_search(tmp_path_factory) -> None:
    """Test FTS with a plain string query."""
    dataset_path = build_text_dataset(tmp_path_factory)

    # Build INVERTED index on the text column
    create_scalar_index(uri=dataset_path, column="text", index_type="INVERTED")

    # Search via daft read_lance with full_text_query
    search_term = "Python"
    df = daft.read_lance(
        dataset_path,
        default_scan_options={"full_text_query": search_term},
    )
    result = df.select("id", "text").to_pydict()

    # Should find row id=2: "Python is a powerful programming language"
    assert len(result["id"]) >= 1
    assert 2 in result["id"]


def test_full_text_query_no_results(tmp_path_factory) -> None:
    """Test FTS with a term that has no matches."""
    dataset_path = build_text_dataset(tmp_path_factory)

    create_scalar_index(uri=dataset_path, column="text", index_type="INVERTED")

    search_term = "zzzzznonexistent"
    df = daft.read_lance(
        dataset_path,
        default_scan_options={"full_text_query": search_term},
    )
    result = df.select("id").to_pydict()

    assert result["id"] == []


def test_full_text_query_multi_fragment(tmp_path_factory) -> None:
    """Test FTS across multiple fragments."""
    dataset_path = build_multi_fragment_text_dataset(tmp_path_factory)

    create_scalar_index(uri=dataset_path, column="text", index_type="INVERTED")

    # Search for "language" - row 5: "Natural language processing uses text analysis"
    search_term = "language"
    df = daft.read_lance(
        dataset_path,
        default_scan_options={"full_text_query": search_term},
    )
    result = df.select("id", "text").to_pydict()

    assert len(result["id"]) >= 1
    assert 5 in result["id"]


def test_full_text_query_with_filter(tmp_path_factory) -> None:
    """Test FTS combined with a standard filter."""
    dataset_path = build_text_dataset(tmp_path_factory)

    create_scalar_index(uri=dataset_path, column="text", index_type="INVERTED")

    # Search for "learning" - rows 3 and 4 both mention "learning"
    search_term = "learning"
    df = daft.read_lance(
        dataset_path,
        default_scan_options={"full_text_query": search_term},
    )
    # Filter to only category=ml
    result = df.select("id", "category").to_pydict()

    assert len(result["id"]) >= 1
    assert 3 in result["id"]  # "Machine learning algorithms are fascinating", category=ml


def test_full_text_query_column_projection(tmp_path_factory) -> None:
    """Test FTS with column projection."""
    dataset_path = build_text_dataset(tmp_path_factory)

    create_scalar_index(uri=dataset_path, column="text", index_type="INVERTED")

    search_term = "storage"
    df = daft.read_lance(
        dataset_path,
        default_scan_options={"full_text_query": search_term},
    )
    result = df.select("id").to_pydict()

    assert result["id"] == [8]  # "Lance format provides efficient storage"


def test_full_text_query_without_index(tmp_path_factory) -> None:
    """Test FTS without an index should still work via brute-force scan."""
    dataset_path = build_text_dataset(tmp_path_factory)

    # Do NOT build an index - Lance should fall back to brute-force search
    search_term = "Python"
    df = daft.read_lance(
        dataset_path,
        default_scan_options={"full_text_query": search_term},
    )
    result = df.select("id").to_pydict()

    # Should still find the result even without an index
    assert len(result["id"]) >= 1
    assert 2 in result["id"]
