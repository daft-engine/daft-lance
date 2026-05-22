"""Unit-level tests for LanceDataSink internals.

Phase 1 covers the _load_existing_dataset typed-exception path and the Lance
message-format pin.
"""

from __future__ import annotations

import uuid

import lance
import pyarrow as pa
import pytest

from daft_lance.lance_data_sink import LanceDataSink


def test_load_existing_dataset_missing_raises_for_append(tmp_path):
    schema = pa.schema([("a", pa.int64())])
    with pytest.raises(ValueError, match="Cannot append to non-existent Lance dataset"):
        LanceDataSink(uri=str(tmp_path / f"missing-{uuid.uuid4()}"), schema=schema, mode="append")


def test_load_existing_dataset_missing_returns_none_for_create(tmp_path):
    schema = pa.schema([("a", pa.int64())])
    sink = LanceDataSink(uri=str(tmp_path / f"missing-{uuid.uuid4()}"), schema=schema, mode="create")
    # Construction succeeds; the dataset will be created on the first write.
    assert sink is not None


def test_mode_merge_rejected_at_construction(tmp_path):
    schema = pa.schema([("a", pa.int64())])
    with pytest.raises(ValueError, match='mode="merge" is no longer supported'):
        LanceDataSink(uri=str(tmp_path / "tbl"), schema=schema, mode="merge")  # type: ignore[arg-type]


def test_lance_message_format_unchanged(tmp_path):
    """Lance error format should remain unchanged.

    If this test fails after a Lance upgrade, _load_existing_dataset's
    substring check must be updated to match the new Lance error format.
    """
    missing = str(tmp_path / f"nonexistent-{uuid.uuid4()}")
    with pytest.raises(ValueError) as exc_info:
        lance.dataset(missing)
    assert "was not found" in str(exc_info.value), (
        f"Lance error format changed; update LanceDataSink._load_existing_dataset. Got: {exc_info.value!r}"
    )


def test_accumulator_rows_threshold():
    from daft_lance.lance_data_sink import _LanceFragmentBuffer

    acc = _LanceFragmentBuffer(max_rows=10, max_bytes=2**63 - 1)
    t1 = pa.table({"a": pa.array(range(4), type=pa.int64())})
    t2 = pa.table({"a": pa.array(range(4), type=pa.int64())})
    t3 = pa.table({"a": pa.array(range(4), type=pa.int64())})

    assert acc.add(t1) is False
    assert acc.add(t2) is False
    assert acc.add(t3) is True  # 12 >= 10
    drained = acc.drain()
    assert drained.num_rows == 12
    assert acc.has_rows() is False


def test_accumulator_bytes_threshold():
    from daft_lance.lance_data_sink import _LanceFragmentBuffer

    payload = b"x" * 1024
    t = pa.table({"b": pa.array([payload] * 8, type=pa.large_binary())})
    # 8 KiB per add; cap at ~16 KiB
    acc = _LanceFragmentBuffer(max_rows=10**9, max_bytes=16 * 1024)
    assert acc.add(t) is False
    assert acc.add(t) is True
    drained = acc.drain()
    assert drained.num_rows == 16


def test_accumulator_drain_resets_state():
    from daft_lance.lance_data_sink import _LanceFragmentBuffer

    acc = _LanceFragmentBuffer(max_rows=4, max_bytes=2**63 - 1)
    t = pa.table({"a": pa.array(range(4), type=pa.int64())})
    assert acc.add(t) is True
    acc.drain()
    assert acc.has_rows() is False
    # Second cycle works
    assert acc.add(t) is True
    drained = acc.drain()
    assert drained.num_rows == 4


def test_prepare_arrow_table_castable_drift(tmp_path):
    # Existing dataset with int64; input with int32 — castable.
    schema = pa.schema([("a", pa.int64())])
    sink = LanceDataSink(uri=str(tmp_path / "tbl"), schema=schema, mode="create")
    int32_table = pa.table({"a": pa.array([1, 2, 3], type=pa.int32())})
    out = sink._prepare_arrow_table(int32_table)
    assert out.schema.field("a").type == pa.int64()
    assert out.column("a").to_pylist() == [1, 2, 3]


def test_default_path_coalesces_multi_partition_input(tmp_path):
    """Buffer coalesces multiple small partitions (issue #6 regression test).

    Issue #6 regression: drive ``LanceDataSink.write()`` directly with many
    small micropartitions and assert the buffer coalesces them into far fewer
    write_fragments calls than the input partition count. Locks in the customer
    fix so a future change to _LanceFragmentBuffer can't silently reintroduce
    one-fragment-per-micropartition behavior.
    """
    from types import SimpleNamespace
    from unittest.mock import patch

    from daft.recordbatch import MicroPartition
    from daft.schema import Schema
    from daft_lance.lance_data_sink import LanceDataSink

    schema = Schema.from_pyarrow_schema(pa.schema([pa.field("a", pa.int64())]))

    calls: list[pa.Table] = []

    class FakeLance:
        def __init__(self) -> None:
            def write_fragments(table, *_, **__):
                calls.append(table)
                return [SimpleNamespace(path="/tmp/fake.lance", rows=table.num_rows)]

            self.fragment = SimpleNamespace(write_fragments=write_fragments)

        def dataset(self, *args, **kwargs):
            raise ValueError("Dataset was not found")  # forces create-path

        def __getattr__(self, name):
            return SimpleNamespace()

    fake = FakeLance()
    n_partitions = 20
    rows_per_partition = 100_000
    total_rows = n_partitions * rows_per_partition
    mps = [MicroPartition.from_pydict({"a": list(range(rows_per_partition))}) for _ in range(n_partitions)]

    with patch("daft_lance.lance_data_sink.lance", fake):
        sink = LanceDataSink(uri=str(tmp_path / "tbl"), schema=schema, mode="create")
        write_results = list(sink.write(iter(mps)))

    # Default ``max_rows_per_file`` is 1_048_576. 20 partitions x 100K = 2M rows
    # coalesce into at most ceil(2M / 1M) + 1 = 3 emitted fragments.
    assert len(write_results) <= 3, len(write_results)
    assert sum(t.num_rows for t in calls) == total_rows
    assert sum(r.rows_written for r in write_results) == total_rows


def test_prepare_arrow_table_missing_column_rejected(tmp_path):
    """Phase 4: append with missing columns surfaces a clear cast error."""
    # Create a dataset with two columns
    schema = pa.schema([("a", pa.int64()), ("b", pa.string())])
    initial = pa.table({"a": pa.array([1], type=pa.int64()), "b": pa.array(["x"], type=pa.string())})
    uri = str(tmp_path / "tbl")
    lance.write_dataset(initial, uri)
    # Try to append with only one column
    sink = LanceDataSink(uri=uri, schema=schema, mode="append")
    missing_col = pa.table({"a": pa.array([2], type=pa.int64())})
    with pytest.raises(Exception):  # any error is fine; the redesign currently relies on the cast to fail
        sink._prepare_arrow_table(missing_col)
