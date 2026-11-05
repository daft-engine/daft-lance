from __future__ import annotations

from pathlib import Path
from typing import cast

import lance
import pandas as pd
import pyarrow as pa
import pytest

from daft_lance import compact_files, create_scalar_index, create_vector_index


@pytest.mark.parametrize("index_type", ["BTREE", "IVF_FLAT"])
def test_distributed_index_preserves_compaction(tmp_path: Path, index_type: str) -> None:
    """Index segments leave adjacent small fragments mergeable."""
    uri = str(tmp_path / "indexed_compaction.lance")
    table = pa.table(
        {
            "id": range(800),
            "value": range(800),
            "vector": pa.array([[float(i), float(i % 7)] for i in range(800)], type=pa.list_(pa.float32(), 2)),
        }
    )
    dataset = lance.write_dataset(table, uri, max_rows_per_file=100, max_rows_per_group=100)
    assert len(dataset.get_fragments()) == 8

    if index_type == "BTREE":
        # Equivalent builds on different columns must use the same boundaries.
        for column in ["id", "value"]:
            create_scalar_index(
                uri,
                column=column,
                index_type="BTREE",
                name=f"{column}_idx",
                fragment_group_size=4,
                max_concurrency=2,
            )
    else:
        create_vector_index(
            uri,
            column="vector",
            index_type="IVF_FLAT",
            name="vector_idx",
            fragment_group_size=4,
            max_concurrency=2,
            num_partitions=2,
        )

    dataset = lance.dataset(uri)
    for index in dataset.describe_indices():
        assert sorted(sorted(segment.fragment_ids) for segment in index.segments) == [[0, 1, 2, 3], [4, 5, 6, 7]]

    def check_query(dataset: lance.LanceDataset) -> None:
        if index_type == "BTREE":
            scanner = dataset.scanner(filter="id >= 120 AND value < 130", columns=["id"])
            assert "ScalarIndexQuery" in scanner.explain_plan()
            expected_ids = list(range(120, 130))
        else:
            scanner = dataset.scanner(nearest={"column": "vector", "q": [123.0, 4.0], "k": 1}, columns=["id"])
            assert "ANNSubIndex" in scanner.explain_plan()
            expected_ids = [123]
        assert sorted(cast(list[int], scanner.to_table()["id"].to_pylist())) == expected_ids

    check_query(dataset)
    metrics = compact_files(
        uri,
        compaction_options={"target_rows_per_fragment": 1000, "num_threads": 1},
        partition_num=2,
        concurrency=2,
    )
    assert metrics is not None
    assert metrics.fragments_removed == 8
    assert metrics.fragments_added == 2

    dataset = lance.dataset(uri)
    assert [fragment.count_rows() for fragment in dataset.get_fragments()] == [400, 400]
    assert dataset.to_table().sort_by("id") == table
    check_query(dataset)


def create_dataset_with_fragments(path: Path, fragment_data: list[pd.DataFrame]):
    """Create a Lance dataset with multiple fragments by appending batches."""
    assert len(fragment_data) >= 1, "fragment_data must contain at least one DataFrame"
    first_df = fragment_data[0]
    lance.write_dataset(pa.Table.from_pandas(first_df), path, max_rows_per_file=len(first_df))

    for df in fragment_data[1:]:
        lance.write_dataset(pa.Table.from_pandas(df), path, mode="append", max_rows_per_file=len(df))

    return lance.dataset(str(path))


def test_basic_compaction(tmp_path: Path):
    """Basic flow: compact two fragments into one; row count unchanged and fragment count reduced."""
    dataset_path = tmp_path / "test_dataset_for_compaction"
    fragment1 = pd.DataFrame({"id": range(1, 11), "value": [f"row_{i}" for i in range(1, 11)]})
    fragment2 = pd.DataFrame({"id": range(11, 21), "value": [f"row_{i}" for i in range(11, 21)]})

    dataset = create_dataset_with_fragments(dataset_path, [fragment1, fragment2])
    assert len(dataset.get_fragments()) == 2, "Expected 2 fragments initially"
    assert dataset.count_rows() == 20, "Expected 20 rows initially"
    original_schema = dataset.schema

    metrics = compact_files(
        uri=str(dataset_path),
        compaction_options={
            "target_rows_per_fragment": 100,
            "num_threads": 1,
        },
    )
    assert metrics is not None, "Compaction should produce metrics"
    assert getattr(metrics, "fragments_removed", None) == 2, "Should remove 2 fragments"
    assert getattr(metrics, "fragments_added", None) == 1, "Should add 1 fragment"

    dataset = lance.dataset(str(dataset_path))
    fragments = dataset.get_fragments()
    assert len(fragments) == 1, "Expected 1 fragment after compaction"
    assert fragments[0].count_rows() == 20, "Single fragment should contain 20 rows after compaction"
    assert dataset.count_rows() == 20, "Total row count should remain 20"
    assert dataset.schema == original_schema, "Compaction should not alter dataset schema"


def test_deletion_compaction(tmp_path: Path):
    """Deletion materialization: compaction merges fragments and preserves post-deletion row count."""
    dataset_path = tmp_path / "test_dataset_for_deletion_compaction"
    fragment1 = pd.DataFrame({"id": range(1, 11), "value": [f"row_{i}" for i in range(1, 11)]})
    fragment2 = pd.DataFrame({"id": range(11, 21), "value": [f"row_{i}" for i in range(11, 21)]})

    dataset = create_dataset_with_fragments(dataset_path, [fragment1, fragment2])
    assert len(dataset.get_fragments()) == 2
    assert dataset.count_rows() == 20

    dataset.delete("id <= 9")
    dataset = lance.dataset(str(dataset_path))
    assert len(dataset.get_fragments()) == 2, "Delete marks should not immediately change fragment count"
    assert dataset.count_rows() == 11, "Expected 11 rows after deletion"

    metrics = compact_files(
        uri=str(dataset_path),
        compaction_options={
            "materialize_deletions": True,
            "materialize_deletions_threshold": 0.5,
            "target_rows_per_fragment": 100,
            "num_threads": 1,
        },
    )
    assert metrics is not None
    assert getattr(metrics, "fragments_removed", None) == 2
    assert getattr(metrics, "fragments_added", None) == 1

    dataset = lance.dataset(str(dataset_path))
    fragments = dataset.get_fragments()
    assert len(fragments) == 1
    assert fragments[0].count_rows() == 11
    assert dataset.count_rows() == 11


def test_idempotent_repeated_compaction(tmp_path: Path):
    """Idempotency: repeat compaction on already compacted dataset should return None and have no side effects."""
    dataset_path = tmp_path / "test_idempotent_compaction"
    df1 = pd.DataFrame({"id": range(0, 5), "v": [f"a{i}" for i in range(5)]})
    df2 = pd.DataFrame({"id": range(5, 10), "v": [f"b{i}" for i in range(5)]})

    dataset = create_dataset_with_fragments(dataset_path, [df1, df2])
    metrics1 = compact_files(uri=str(dataset_path), compaction_options={"target_rows_per_fragment": 100})
    assert metrics1 is not None

    metrics2 = compact_files(uri=str(dataset_path), compaction_options={"target_rows_per_fragment": 100})
    assert metrics2 is None, "Second compaction should return None"

    dataset = lance.dataset(str(dataset_path))
    assert len(dataset.get_fragments()) == 1
    assert dataset.count_rows() == 10


def test_invalid_compaction_options_key(tmp_path: Path):
    """Unknown compaction option key should raise ValueError."""
    dataset_path = tmp_path / "test_invalid_options"
    df1 = pd.DataFrame({"id": [0, 1], "v": ["x", "y"]})
    df2 = pd.DataFrame({"id": [2, 3], "v": ["z", "w"]})
    create_dataset_with_fragments(dataset_path, [df1, df2])

    with pytest.raises(ValueError):
        compact_files(uri=str(dataset_path), compaction_options={"nonexistent_option": True})


def test_compaction_with_partition_num(tmp_path: Path):
    """Compaction using partition_num should succeed or gracefully skip when unsupported."""
    dataset_path = tmp_path / "test_compaction_partition_num"
    data = pa.table({"a": range(800), "b": range(800)})
    dataset = lance.write_dataset(data, dataset_path, max_rows_per_file=200)
    pre_fragments = dataset.get_fragments()
    assert len(pre_fragments) == 4

    pre_rows = dataset.count_rows()
    metrics = compact_files(
        uri=str(dataset_path),
        compaction_options={
            "target_rows_per_fragment": 400,
            "num_threads": 1,
        },
        partition_num=2,
        concurrency=2,
    )
    assert metrics is not None, "Compaction should produce metrics"
    dataset = lance.dataset(str(dataset_path))
    post_fragments = len(dataset.get_fragments())
    post_rows = dataset.count_rows()
    assert post_fragments == 2, "Fragment count should be reduced after compaction"
    assert post_rows == pre_rows, "Row count should remain unchanged after compaction"
