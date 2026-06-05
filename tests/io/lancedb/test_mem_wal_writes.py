from __future__ import annotations

import os

import lance
import pyarrow as pa
import pytest

import daft
from daft_lance.lance_data_sink import LanceDataSink


@pytest.fixture(scope="function")
def lance_dataset_path(tmp_path_factory):
    tmp_dir = tmp_path_factory.mktemp("lance_mem_wal")
    yield str(tmp_dir)


data_simple = {
    "id": [1, 2, 3],
    "value": [10.0, 20.0, 30.0],
}


class TestMemWalSinkConstruction:
    def test_defaults(self, lance_dataset_path):
        schema = pa.schema([("id", pa.int64()), ("value", pa.float64())])
        sink = LanceDataSink(uri=lance_dataset_path, schema=schema, mode="create")
        assert sink._use_mem_wal is False
        assert sink._compact_after_write is True

    def test_use_mem_wal_flag(self, lance_dataset_path):
        schema = pa.schema([("id", pa.int64()), ("value", pa.float64())])
        sink = LanceDataSink(
            uri=lance_dataset_path, schema=schema, mode="create", use_mem_wal=True
        )
        assert sink._use_mem_wal is True
        assert sink._compact_after_write is True

    def test_compact_after_write_flag(self, lance_dataset_path):
        schema = pa.schema([("id", pa.int64()), ("value", pa.float64())])
        sink = LanceDataSink(
            uri=lance_dataset_path,
            schema=schema,
            mode="create",
            use_mem_wal=True,
            compact_after_write=False,
        )
        assert sink._use_mem_wal is True
        assert sink._compact_after_write is False


class TestMemWalCreatePath:
    def test_create_writes_to_wal_directory(self, lance_dataset_path):
        df = daft.from_pydict(data_simple)
        df.write_lance(lance_dataset_path, mode="create", use_mem_wal=True)

        mem_wal_dir = os.path.join(lance_dataset_path, "_mem_wal")
        assert os.path.isdir(mem_wal_dir)

    def test_create_dataset_exists_after_write(self, lance_dataset_path):
        df = daft.from_pydict(data_simple)
        df.write_lance(lance_dataset_path, mode="create", use_mem_wal=True)

        ds = lance.dataset(lance_dataset_path)
        assert ds is not None
        assert ds.schema.names == ["id", "value"]

    def test_create_mem_wal_initialized(self, lance_dataset_path):
        df = daft.from_pydict(data_simple)
        df.write_lance(lance_dataset_path, mode="create", use_mem_wal=True)

        ds = lance.dataset(lance_dataset_path)
        details = ds.mem_wal_index_details()
        assert details is not None

    def test_create_returns_stats(self, lance_dataset_path):
        df = daft.from_pydict(data_simple)
        result = df.write_lance(
            lance_dataset_path,
            mode="create",
            use_mem_wal=True,
            compact_after_write=False,
        )
        stats = result.to_pydict()
        assert "num_fragments" in stats
        assert "version" in stats
        assert stats["version"][0] >= 1


class TestMemWalAppendPath:
    def test_append_to_existing_dataset(self, lance_dataset_path):
        df1 = daft.from_pydict({"id": [1, 2], "value": [10.0, 20.0]})
        df1.write_lance(lance_dataset_path, mode="create")

        df2 = daft.from_pydict({"id": [3, 4], "value": [30.0, 40.0]})
        df2.write_lance(
            lance_dataset_path,
            mode="append",
            use_mem_wal=True,
            compact_after_write=False,
        )

        ds = lance.dataset(lance_dataset_path)
        assert ds.mem_wal_index_details() is not None

    def test_append_initializes_mem_wal_on_existing(self, lance_dataset_path):
        df1 = daft.from_pydict(data_simple)
        df1.write_lance(lance_dataset_path, mode="create")

        ds_before = lance.dataset(lance_dataset_path)
        assert ds_before.mem_wal_index_details() is None

        df2 = daft.from_pydict(data_simple)
        df2.write_lance(
            lance_dataset_path,
            mode="append",
            use_mem_wal=True,
            compact_after_write=False,
        )

        ds_after = lance.dataset(lance_dataset_path)
        assert ds_after.mem_wal_index_details() is not None


class TestMemWalDeterministicSharding:
    def test_each_micropartition_gets_unique_shard(self, lance_dataset_path):
        df = daft.from_pydict(
            {"id": list(range(100)), "value": [float(x) for x in range(100)]}
        ).repartition(4)
        df.write_lance(
            lance_dataset_path,
            mode="create",
            use_mem_wal=True,
            compact_after_write=False,
        )

        mem_wal_dir = os.path.join(lance_dataset_path, "_mem_wal")
        assert os.path.isdir(mem_wal_dir)
        shard_dirs = [
            d
            for d in os.listdir(mem_wal_dir)
            if os.path.isdir(os.path.join(mem_wal_dir, d))
        ]
        assert len(shard_dirs) >= 1

    def test_parallel_shards_no_contention(self, lance_dataset_path):
        df = daft.from_pydict(
            {"id": list(range(1000)), "value": [float(x) for x in range(1000)]}
        ).repartition(8)
        result = df.write_lance(
            lance_dataset_path,
            mode="create",
            use_mem_wal=True,
            compact_after_write=False,
        )
        stats = result.to_pydict()
        assert stats["version"][0] >= 1


class TestMemWalCompactAfterWrite:
    def test_compact_after_write_true(self, lance_dataset_path):
        df = daft.from_pydict(data_simple)
        result = df.write_lance(
            lance_dataset_path, mode="create", use_mem_wal=True, compact_after_write=True
        )
        stats = result.to_pydict()
        assert "num_fragments" in stats
        assert "version" in stats

    def test_compact_after_write_false(self, lance_dataset_path):
        df = daft.from_pydict(data_simple)
        result = df.write_lance(
            lance_dataset_path,
            mode="create",
            use_mem_wal=True,
            compact_after_write=False,
        )
        stats = result.to_pydict()
        assert "num_fragments" in stats

    def test_cow_data_visible_after_standard_write(self, lance_dataset_path):
        df = daft.from_pydict(data_simple)
        df.write_lance(lance_dataset_path, mode="create")

        ds = lance.dataset(lance_dataset_path)
        assert ds.count_rows() == 3
        loaded = ds.to_table().to_pydict()
        assert sorted(loaded["id"]) == [1, 2, 3]


class TestMemWalMultipleWrites:
    def test_multiple_sequential_writes(self, lance_dataset_path):
        df1 = daft.from_pydict({"id": [1, 2], "value": [10.0, 20.0]})
        df1.write_lance(
            lance_dataset_path,
            mode="create",
            use_mem_wal=True,
            compact_after_write=False,
        )

        df2 = daft.from_pydict({"id": [3, 4], "value": [30.0, 40.0]})
        df2.write_lance(
            lance_dataset_path,
            mode="append",
            use_mem_wal=True,
            compact_after_write=False,
        )

        ds = lance.dataset(lance_dataset_path)
        details = ds.mem_wal_index_details()
        assert details is not None


class TestMemWalEnsureDataset:
    def test_ensure_creates_dataset_if_missing(self, lance_dataset_path):
        target = os.path.join(lance_dataset_path, "new_ds")
        schema = pa.schema([("a", pa.int64())])
        sink = LanceDataSink(
            uri=target, schema=schema, mode="create", use_mem_wal=True
        )
        ds = sink._ensure_mem_wal_dataset()
        assert ds is not None
        assert ds.mem_wal_index_details() is not None

    def test_ensure_reuses_existing_dataset(self, lance_dataset_path):
        schema = pa.schema([("a", pa.int64())])
        lance.write_dataset(pa.table({"a": [1]}, schema=schema), lance_dataset_path)

        sink = LanceDataSink(
            uri=lance_dataset_path, schema=schema, mode="append", use_mem_wal=True
        )
        ds = sink._ensure_mem_wal_dataset()
        assert ds is not None
        assert ds.mem_wal_index_details() is not None

    def test_ensure_idempotent_initialization(self, lance_dataset_path):
        schema = pa.schema([("a", pa.int64())])
        lance.write_dataset(pa.table({"a": [1]}, schema=schema), lance_dataset_path)

        ds = lance.dataset(lance_dataset_path)
        ds.initialize_mem_wal(unsharded=True)

        sink = LanceDataSink(
            uri=lance_dataset_path, schema=schema, mode="append", use_mem_wal=True
        )
        ds2 = sink._ensure_mem_wal_dataset()
        assert ds2.mem_wal_index_details() is not None


class TestMemWalWriteResult:
    def test_write_result_has_empty_fragments(self, lance_dataset_path):
        from daft.recordbatch import MicroPartition

        schema = pa.schema([("a", pa.int64())])
        sink = LanceDataSink(
            uri=lance_dataset_path, schema=schema, mode="create", use_mem_wal=True
        )
        mp = MicroPartition.from_pydict({"a": [1, 2, 3]})
        results = list(sink.write(iter([mp])))
        assert len(results) == 1
        assert results[0].result == []
        assert results[0].rows_written == 3
        assert results[0].bytes_written >= 0

    def test_finalize_mem_wal_returns_stats(self, lance_dataset_path):
        from daft.io.sink import WriteResult
        from daft.recordbatch import MicroPartition

        schema = pa.schema([("a", pa.int64())])
        sink = LanceDataSink(
            uri=lance_dataset_path,
            schema=schema,
            mode="create",
            use_mem_wal=True,
            compact_after_write=False,
        )
        mp = MicroPartition.from_pydict({"a": [1, 2, 3]})
        results = list(sink.write(iter([mp])))
        stats_mp = sink.finalize(results)
        stats = stats_mp.to_pydict()
        assert "num_fragments" in stats
        assert "num_deleted_rows" in stats
        assert "num_small_files" in stats
        assert "version" in stats


class TestMemWalSchemaPreservation:
    def test_schema_types_preserved(self, lance_dataset_path):
        schema = pa.schema(
            [
                pa.field("int_col", pa.int64()),
                pa.field("float_col", pa.float64()),
                pa.field("str_col", pa.large_string()),
            ]
        )
        df = daft.from_pydict(
            {"int_col": [1, 2], "float_col": [1.5, 2.5], "str_col": ["a", "b"]}
        )
        df.write_lance(
            lance_dataset_path,
            schema=schema,
            mode="create",
            use_mem_wal=True,
            compact_after_write=False,
        )

        ds = lance.dataset(lance_dataset_path)
        ds_schema = ds.schema
        assert ds_schema.field("int_col").type == pa.int64()
        assert ds_schema.field("float_col").type == pa.float64()
        assert ds_schema.field("str_col").type == pa.large_string()


class TestMemWalCowFallback:
    def test_cow_path_unaffected(self, lance_dataset_path):
        df = daft.from_pydict(data_simple)
        df.write_lance(lance_dataset_path, mode="create", use_mem_wal=False)

        ds = lance.dataset(lance_dataset_path)
        assert ds.count_rows() == 3
        loaded = ds.to_table().to_pydict()
        assert sorted(loaded["id"]) == [1, 2, 3]

        mem_wal_dir = os.path.join(lance_dataset_path, "_mem_wal")
        assert not os.path.exists(mem_wal_dir)

    def test_default_is_cow(self, lance_dataset_path):
        df = daft.from_pydict(data_simple)
        df.write_lance(lance_dataset_path, mode="create")

        ds = lance.dataset(lance_dataset_path)
        assert ds.count_rows() == 3
        assert ds.mem_wal_index_details() is None
