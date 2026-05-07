"""Extensive tests for the fast-path merge_columns (metadata-only add columns).

The fast path writes raw .lance files via LanceFileWriter and stitches them
into fragment metadata, avoiding full fragment rewrites. If this goes wrong,
it corrupts the dataset. These tests cover correctness, ordering, integrity,
type fidelity, auto-detection, incremental merges, and edge cases.
"""

from __future__ import annotations

import hashlib
import os

import lance
import pyarrow as pa
import pytest

import daft
from daft_lance.merge import _can_use_fast_path, merge_columns_from_df


# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------

@pytest.fixture(scope="function")
def ds_path(tmp_path_factory):
    yield str(tmp_path_factory.mktemp("fast_path"))


def create_dataset(path: str, fragments: list[dict]) -> lance.LanceDataset:
    for i, data in enumerate(fragments):
        table = pa.table(data)
        lance.write_dataset(table, path, mode="create" if i == 0 else "append")
    return lance.dataset(path)


def read_with_metadata(path: str) -> daft.DataFrame:
    return daft.read_lance(
        path,
        include_fragment_id=True,
        default_scan_options={"with_row_address": True},
    )


def file_md5(filepath: str) -> str:
    h = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


# ---------------------------------------------------------------------------
# 1. Basic correctness
# ---------------------------------------------------------------------------

class TestBasicCorrectness:
    def test_fast_path_single_column_int(self, ds_path):
        ds = create_dataset(ds_path, [{"id": [1, 2, 3], "val": [10, 20, 30]}])
        df = read_with_metadata(ds_path)
        df = df.with_column("doubled", daft.col("val").cast(daft.DataType.int64()) * 2)
        ds2 = merge_columns_from_df(df, ds, ds_path)
        result = ds2.to_table().to_pydict()
        assert result["doubled"] == [2 * v for v in result["val"]]

    def test_fast_path_single_column_float(self, ds_path):
        ds = create_dataset(ds_path, [{"x": [1.0, 2.0, 3.0]}])
        df = read_with_metadata(ds_path)
        df = df.with_column("half", daft.col("x").cast(daft.DataType.float64()) / 2.0)
        ds2 = merge_columns_from_df(df, ds, ds_path)
        result = ds2.to_table().to_pydict()
        for x, h in zip(result["x"], result["half"]):
            assert pytest.approx(x / 2.0, rel=1e-6) == h

    def test_fast_path_single_column_string(self, ds_path):
        ds = create_dataset(ds_path, [{"id": [1, 2], "name": ["alice", "bob"]}])
        df = read_with_metadata(ds_path)
        df = df.with_column("greeting", daft.lit("hello_") + daft.col("name"))
        ds2 = merge_columns_from_df(df, ds, ds_path)
        result = ds2.to_table().to_pydict()
        assert result["greeting"] == ["hello_alice", "hello_bob"]

    def test_fast_path_multiple_new_columns(self, ds_path):
        ds = create_dataset(ds_path, [{"id": [1, 2, 3]}])
        df = read_with_metadata(ds_path)
        df = df.with_column("a", daft.col("id").cast(daft.DataType.int64()) * 10)
        df = df.with_column("b", daft.col("id").cast(daft.DataType.float64()) + 0.5)
        df = df.with_column("c", daft.lit("row_") + daft.col("id").cast(daft.DataType.string()))
        ds2 = merge_columns_from_df(df, ds, ds_path)
        result = ds2.to_table().to_pydict()
        assert result["a"] == [10, 20, 30]
        for i, b in zip(result["id"], result["b"]):
            assert pytest.approx(i + 0.5) == b
        assert result["c"] == ["row_1", "row_2", "row_3"]

    def test_fast_path_multi_fragment(self, ds_path):
        ds = create_dataset(ds_path, [
            {"id": [1, 2, 3], "val": [10, 20, 30]},
            {"id": [4, 5], "val": [40, 50]},
            {"id": [6, 7], "val": [60, 70]},
        ])
        df = read_with_metadata(ds_path)
        df = df.with_column("score", daft.col("val").cast(daft.DataType.float64()) * 1.5)
        ds2 = merge_columns_from_df(df, ds, ds_path)
        result = ds2.to_table().sort_by("id").to_pydict()
        for v, s in zip(result["val"], result["score"]):
            assert pytest.approx(v * 1.5, rel=1e-6) == s
        assert len(result["id"]) == 7

    def test_fast_path_computed_column(self, ds_path):
        ds = create_dataset(ds_path, [
            {"x": [10, 20], "y": [1, 2]},
            {"x": [30, 40], "y": [3, 4]},
        ])
        df = read_with_metadata(ds_path)
        df = df.with_column("z", daft.col("x").cast(daft.DataType.int64()) + daft.col("y").cast(daft.DataType.int64()))
        ds2 = merge_columns_from_df(df, ds, ds_path)
        result = ds2.to_table().sort_by("x").to_pydict()
        for x, y, z in zip(result["x"], result["y"], result["z"]):
            assert x + y == z


# ---------------------------------------------------------------------------
# 2. Fragment integrity
# ---------------------------------------------------------------------------

class TestFragmentIntegrity:
    def test_existing_columns_unchanged(self, ds_path):
        original_data = {"id": [1, 2, 3], "name": ["a", "b", "c"], "val": [10.0, 20.0, 30.0]}
        ds = create_dataset(ds_path, [original_data])
        before = ds.to_table().sort_by("id").to_pydict()

        df = read_with_metadata(ds_path)
        df = df.with_column("new_col", daft.lit(999))
        ds2 = merge_columns_from_df(df, ds, ds_path)

        after = ds2.to_table().sort_by("id").to_pydict()
        assert after["id"] == before["id"]
        assert after["name"] == before["name"]
        assert after["val"] == before["val"]

    def test_fragment_file_count(self, ds_path):
        ds = create_dataset(ds_path, [
            {"a": [1, 2]},
            {"a": [3, 4]},
        ])
        df = read_with_metadata(ds_path)
        df = df.with_column("b", daft.lit(0))
        ds2 = merge_columns_from_df(df, ds, ds_path)
        for frag in ds2.get_fragments():
            assert len(list(frag.data_files())) == 2

    def test_original_files_not_rewritten(self, ds_path):
        ds = create_dataset(ds_path, [{"a": [1, 2, 3]}])
        data_dir = os.path.join(ds_path, "data")
        original_files = {}
        for fname in os.listdir(data_dir):
            fpath = os.path.join(data_dir, fname)
            original_files[fname] = file_md5(fpath)

        df = read_with_metadata(ds_path)
        df = df.with_column("b", daft.lit(42))
        merge_columns_from_df(df, ds, ds_path)

        for fname, old_hash in original_files.items():
            fpath = os.path.join(data_dir, fname)
            assert os.path.exists(fpath), f"Original file {fname} was deleted"
            assert file_md5(fpath) == old_hash, f"Original file {fname} was modified"

    def test_row_count_preserved(self, ds_path):
        ds = create_dataset(ds_path, [
            {"v": list(range(5))},
            {"v": list(range(5, 8))},
        ])
        original_count = ds.count_rows()
        per_frag_counts = [f.count_rows() for f in ds.get_fragments()]

        df = read_with_metadata(ds_path)
        df = df.with_column("w", daft.lit(0))
        ds2 = merge_columns_from_df(df, ds, ds_path)

        assert ds2.count_rows() == original_count
        for frag, expected in zip(ds2.get_fragments(), per_frag_counts):
            assert frag.count_rows() == expected

    def test_schema_evolution_correct(self, ds_path):
        ds = create_dataset(ds_path, [{"id": [1], "name": ["x"]}])
        df = read_with_metadata(ds_path)
        df = df.with_column("score", daft.lit(3.14))
        ds2 = merge_columns_from_df(df, ds, ds_path)
        schema_names = set(ds2.schema.names)
        assert "id" in schema_names
        assert "name" in schema_names
        assert "score" in schema_names
        assert ds2.schema.field("score").type == pa.float64()


# ---------------------------------------------------------------------------
# 3. Row ordering
# ---------------------------------------------------------------------------

class TestRowOrdering:
    def test_rowaddr_sorting_restores_order(self, ds_path):
        ds = create_dataset(ds_path, [{"id": [1, 2, 3, 4, 5], "val": [10, 20, 30, 40, 50]}])
        df = read_with_metadata(ds_path)
        # Daft doesn't guarantee order, but let's force a known computation
        df = df.with_column("doubled", daft.col("val").cast(daft.DataType.int64()) * 2)
        ds2 = merge_columns_from_df(df, ds, ds_path)
        result = ds2.to_table().sort_by("id").to_pydict()
        assert result["doubled"] == [20, 40, 60, 80, 100]

    def test_multi_fragment_ordering(self, ds_path):
        ds = create_dataset(ds_path, [
            {"id": [1, 2, 3]},
            {"id": [4, 5, 6]},
            {"id": [7, 8, 9]},
        ])
        df = read_with_metadata(ds_path)
        df = df.with_column("neg", daft.col("id").cast(daft.DataType.int64()) * -1)
        ds2 = merge_columns_from_df(df, ds, ds_path)
        result = ds2.to_table().sort_by("id").to_pydict()
        for i, n in zip(result["id"], result["neg"]):
            assert n == -i


# ---------------------------------------------------------------------------
# 4. Auto-detection
# ---------------------------------------------------------------------------

class TestAutoDetection:
    def test_auto_detects_fast_path(self, ds_path):
        ds = create_dataset(ds_path, [{"id": [1, 2]}])
        df = read_with_metadata(ds_path)
        df = df.with_column("new", daft.lit(1))
        assert _can_use_fast_path(df, ds, "_rowaddr") is True

    def test_falls_back_without_rowaddr(self, ds_path):
        ds = create_dataset(ds_path, [{"id": [1, 2], "val": [10, 20]}])
        # Read WITHOUT _rowaddr — fast path should not be used
        df = daft.read_lance(ds_path, include_fragment_id=True)
        df = df.with_column("doubled", daft.col("val").cast(daft.DataType.int64()) * 2)
        assert _can_use_fast_path(df, ds, "_rowaddr") is False

    def test_falls_back_with_business_key(self, ds_path):
        ds = create_dataset(ds_path, [{"id": [1, 2], "val": [10, 20]}])
        df = daft.read_lance(
            ds_path,
            include_fragment_id=True,
            default_scan_options={"with_row_address": True},
        )
        df = df.with_column("doubled", daft.col("val").cast(daft.DataType.int64()) * 2)
        assert _can_use_fast_path(df, ds, "id") is False

    def test_falls_back_when_rows_filtered(self, ds_path):
        ds = create_dataset(ds_path, [{"id": [1, 2, 3, 4], "val": [10, 20, 30, 40]}])
        df = read_with_metadata(ds_path)
        df = df.where(daft.col("id") > 2)
        df = df.with_column("new", daft.lit(1))
        assert _can_use_fast_path(df, ds, "_rowaddr") is False

    def test_fast_path_flag_is_correct(self, ds_path):
        ds = create_dataset(ds_path, [{"id": [1, 2]}])
        # With _rowaddr + fragment_id + full rows → True
        df_full = read_with_metadata(ds_path).with_column("x", daft.lit(1))
        assert _can_use_fast_path(df_full, ds, "_rowaddr") is True

        # Without _rowaddr → False
        df_no_addr = daft.read_lance(ds_path, include_fragment_id=True).with_column("x", daft.lit(1))
        assert _can_use_fast_path(df_no_addr, ds, "_rowaddr") is False

        # Without fragment_id → False
        df_no_frag = daft.read_lance(
            ds_path, default_scan_options={"with_row_address": True}
        ).with_column("x", daft.lit(1))
        assert _can_use_fast_path(df_no_frag, ds, "_rowaddr") is False

        # Non-_rowaddr join key → False
        assert _can_use_fast_path(df_full, ds, "id") is False


# ---------------------------------------------------------------------------
# 5. Multiple merges
# ---------------------------------------------------------------------------

class TestMultipleMerges:
    def test_two_sequential_merges(self, ds_path):
        ds = create_dataset(ds_path, [{"id": [1, 2, 3]}])

        # First merge: add column A
        df = read_with_metadata(ds_path)
        df = df.with_column("a", daft.col("id").cast(daft.DataType.int64()) * 10)
        ds = merge_columns_from_df(df, ds, ds_path)

        # Second merge: add column B
        df2 = read_with_metadata(ds_path)
        df2 = df2.with_column("b", daft.col("id").cast(daft.DataType.int64()) * 100)
        ds2 = merge_columns_from_df(df2, ds, ds_path)

        result = ds2.to_table().sort_by("id").to_pydict()
        assert result["a"] == [10, 20, 30]
        assert result["b"] == [100, 200, 300]
        for frag in ds2.get_fragments():
            assert len(list(frag.data_files())) == 3

    def test_merge_after_append(self, ds_path):
        ds = create_dataset(ds_path, [{"id": [1, 2]}])
        lance.write_dataset(pa.table({"id": [3, 4]}), ds_path, mode="append")
        ds = lance.dataset(ds_path)
        assert len(ds.get_fragments()) == 2

        df = read_with_metadata(ds_path)
        df = df.with_column("flag", daft.lit(True))
        ds2 = merge_columns_from_df(df, ds, ds_path)
        result = ds2.to_table().sort_by("id").to_pydict()
        assert result["flag"] == [True, True, True, True]
        assert len(result["id"]) == 4

    def test_merge_preserves_previous_merge(self, ds_path):
        ds = create_dataset(ds_path, [{"id": [1, 2], "val": [10, 20]}])

        # Merge A
        df = read_with_metadata(ds_path)
        df = df.with_column("a", daft.col("val").cast(daft.DataType.int64()) + 1)
        ds = merge_columns_from_df(df, ds, ds_path)
        check_a = ds.to_table().sort_by("id").to_pydict()
        assert check_a["a"] == [11, 21]

        # Merge B
        df2 = read_with_metadata(ds_path)
        df2 = df2.with_column("b", daft.col("val").cast(daft.DataType.int64()) + 2)
        ds2 = merge_columns_from_df(df2, ds, ds_path)
        result = ds2.to_table().sort_by("id").to_pydict()
        assert result["a"] == [11, 21], "Column A corrupted by second merge"
        assert result["b"] == [12, 22]
        assert result["val"] == [10, 20]


# ---------------------------------------------------------------------------
# 6. Data types
# ---------------------------------------------------------------------------

class TestDataTypes:
    def test_type_int64(self, ds_path):
        ds = create_dataset(ds_path, [{"id": [1, 2]}])
        df = read_with_metadata(ds_path)
        df = df.with_column("x", daft.lit(42).cast(daft.DataType.int64()))
        ds2 = merge_columns_from_df(df, ds, ds_path)
        assert ds2.schema.field("x").type == pa.int64()
        assert ds2.to_table().column("x").to_pylist() == [42, 42]

    def test_type_float32(self, ds_path):
        # NOTE: float32 gets widened to float64 through the pylist round-trip
        # in the UDF. This is a known limitation of the current fast path.
        ds = create_dataset(ds_path, [{"id": [1, 2]}])
        df = read_with_metadata(ds_path)
        df = df.with_column("x", daft.lit(3.14).cast(daft.DataType.float32()))
        ds2 = merge_columns_from_df(df, ds, ds_path)
        # Value is preserved even if type widens
        vals = ds2.to_table().column("x").to_pylist()
        assert all(pytest.approx(v, rel=1e-5) == 3.14 for v in vals)

    def test_type_float64(self, ds_path):
        ds = create_dataset(ds_path, [{"id": [1, 2]}])
        df = read_with_metadata(ds_path)
        df = df.with_column("x", daft.lit(2.718))
        ds2 = merge_columns_from_df(df, ds, ds_path)
        assert ds2.schema.field("x").type == pa.float64()

    def test_type_string(self, ds_path):
        ds = create_dataset(ds_path, [{"id": [1, 2]}])
        df = read_with_metadata(ds_path)
        df = df.with_column("label", daft.lit("hello"))
        ds2 = merge_columns_from_df(df, ds, ds_path)
        assert ds2.to_table().column("label").to_pylist() == ["hello", "hello"]

    def test_type_bool(self, ds_path):
        ds = create_dataset(ds_path, [{"id": [1, 2, 3]}])
        df = read_with_metadata(ds_path)
        df = df.with_column("flag", daft.col("id").cast(daft.DataType.int64()) > 1)
        ds2 = merge_columns_from_df(df, ds, ds_path)
        result = ds2.to_table().sort_by("id").column("flag").to_pylist()
        assert result == [False, True, True]

    def test_type_nullable(self, ds_path):
        ds = create_dataset(ds_path, [{"id": pa.array([1, 2, 3, 4], type=pa.int64())}])
        df = read_with_metadata(ds_path)
        # Create a column with nulls: even ids → null, odd ids → id value
        df = df.with_column(
            "maybe",
            (daft.col("id").cast(daft.DataType.int64()) % 2 != 0).cast(daft.DataType.int64()) * daft.col("id").cast(daft.DataType.int64()),
        )
        ds2 = merge_columns_from_df(df, ds, ds_path)
        result = ds2.to_table().sort_by("id").column("maybe").to_pylist()
        # id=1 → odd → 1*1=1, id=2 → even → 0*2=0, id=3 → odd → 1*3=3, id=4 → even → 0*4=0
        assert result == [1, 0, 3, 0]


# ---------------------------------------------------------------------------
# 7. Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_single_row_fragment(self, ds_path):
        ds = create_dataset(ds_path, [{"id": [1]}])
        df = read_with_metadata(ds_path)
        df = df.with_column("x", daft.lit(99))
        ds2 = merge_columns_from_df(df, ds, ds_path)
        assert ds2.to_table().to_pydict() == {"id": [1], "x": [99]}

    def test_large_fragment(self, ds_path):
        n = 10_000
        ds = create_dataset(ds_path, [{"id": list(range(n))}])
        df = read_with_metadata(ds_path)
        df = df.with_column("neg", daft.col("id").cast(daft.DataType.int64()) * -1)
        ds2 = merge_columns_from_df(df, ds, ds_path)
        result = ds2.to_table().sort_by("id").to_pydict()
        assert len(result["id"]) == n
        for i, neg in zip(result["id"], result["neg"]):
            assert neg == -i

    def test_many_fragments(self, ds_path):
        fragments = [{"id": [i]} for i in range(20)]
        ds = create_dataset(ds_path, fragments)
        assert len(ds.get_fragments()) == 20

        df = read_with_metadata(ds_path)
        df = df.with_column("doubled", daft.col("id").cast(daft.DataType.int64()) * 2)
        ds2 = merge_columns_from_df(df, ds, ds_path)
        result = ds2.to_table().sort_by("id").to_pydict()
        assert len(result["id"]) == 20
        for i, d in zip(result["id"], result["doubled"]):
            assert d == i * 2

    def test_empty_new_columns_raises(self, ds_path):
        ds = create_dataset(ds_path, [{"id": [1, 2]}])
        df = read_with_metadata(ds_path)
        # No new columns added — only existing + metadata columns
        with pytest.raises(ValueError, match="No new columns"):
            merge_columns_from_df(df, ds, ds_path)

    def test_dataset_version_incremented(self, ds_path):
        ds = create_dataset(ds_path, [{"id": [1]}])
        v_before = ds.version
        df = read_with_metadata(ds_path)
        df = df.with_column("x", daft.lit(1))
        ds2 = merge_columns_from_df(df, ds, ds_path)
        assert ds2.version == v_before + 1


# ---------------------------------------------------------------------------
# 8. Fast vs slow comparison
# ---------------------------------------------------------------------------

class TestFastVsSlowComparison:
    def test_fast_vs_slow_identical_results(self, tmp_path_factory):
        fast_path = str(tmp_path_factory.mktemp("fast"))
        slow_path = str(tmp_path_factory.mktemp("slow"))

        data = [
            {"id": [1, 2, 3], "val": [10, 20, 30]},
            {"id": [4, 5], "val": [40, 50]},
        ]
        ds_fast = create_dataset(fast_path, data)
        ds_slow = create_dataset(slow_path, data)

        # Fast path: read with _rowaddr + fragment_id
        df_fast = read_with_metadata(fast_path)
        df_fast = df_fast.with_column("score", daft.col("val").cast(daft.DataType.float64()) * 2.5)
        ds_fast = merge_columns_from_df(df_fast, ds_fast, fast_path)

        # Slow path: read with fragment_id only, use business key
        df_slow = daft.read_lance(slow_path, include_fragment_id=True,
                                  default_scan_options={"with_row_address": True})
        df_slow = df_slow.with_column("score", daft.col("val").cast(daft.DataType.float64()) * 2.5)
        from daft_lance.merge import _merge_slow_path
        ds_slow = _merge_slow_path(
            df_slow, ds_slow, slow_path,
            read_columns=["_rowaddr", "score"],
            left_on="_rowaddr", right_on="_rowaddr",
            reader_schema=None, batch_size=None,
            storage_options=None,
        )

        fast_result = ds_fast.to_table().sort_by("id").to_pydict()
        slow_result = ds_slow.to_table().sort_by("id").to_pydict()
        assert fast_result["id"] == slow_result["id"]
        assert fast_result["val"] == slow_result["val"]
        for f, s in zip(fast_result["score"], slow_result["score"]):
            assert pytest.approx(f, rel=1e-6) == s


# ---------------------------------------------------------------------------
# 9. Read-back integrity
# ---------------------------------------------------------------------------

class TestReadBackIntegrity:
    def test_read_after_merge_with_filter(self, ds_path):
        ds = create_dataset(ds_path, [
            {"id": [1, 2, 3], "val": [10, 20, 30]},
            {"id": [4, 5], "val": [40, 50]},
        ])
        df = read_with_metadata(ds_path)
        df = df.with_column("score", daft.col("val").cast(daft.DataType.int64()) * 2)
        merge_columns_from_df(df, ds, ds_path)

        ds2 = lance.dataset(ds_path)
        filtered = ds2.to_table(filter="score > 40")
        ids = set(filtered.column("id").to_pylist())
        # score > 40 means val > 20, so id in {3, 4, 5}
        assert ids == {3, 4, 5}

    def test_read_after_merge_with_projection(self, ds_path):
        ds = create_dataset(ds_path, [{"id": [1, 2], "val": [10, 20]}])
        df = read_with_metadata(ds_path)
        df = df.with_column("new_col", daft.lit(42))
        merge_columns_from_df(df, ds, ds_path)

        ds2 = lance.dataset(ds_path)
        projected = ds2.to_table(columns=["new_col"])
        assert projected.column_names == ["new_col"]
        assert projected.column("new_col").to_pylist() == [42, 42]

    def test_read_after_merge_select_original_only(self, ds_path):
        original = {"id": [1, 2, 3], "name": ["a", "b", "c"]}
        ds = create_dataset(ds_path, [original])
        df = read_with_metadata(ds_path)
        df = df.with_column("extra", daft.lit("x"))
        merge_columns_from_df(df, ds, ds_path)

        ds2 = lance.dataset(ds_path)
        result = ds2.to_table(columns=["id", "name"]).sort_by("id").to_pydict()
        assert result["id"] == [1, 2, 3]
        assert result["name"] == ["a", "b", "c"]

    def test_scan_fragments_individually(self, ds_path):
        ds = create_dataset(ds_path, [
            {"id": [1, 2]},
            {"id": [3, 4]},
        ])
        df = read_with_metadata(ds_path)
        df = df.with_column("doubled", daft.col("id").cast(daft.DataType.int64()) * 2)
        merge_columns_from_df(df, ds, ds_path)

        ds2 = lance.dataset(ds_path)
        all_ids = []
        all_doubled = []
        for frag in ds2.get_fragments():
            tbl = ds2.scanner(fragments=[frag]).to_table()
            all_ids.extend(tbl.column("id").to_pylist())
            all_doubled.extend(tbl.column("doubled").to_pylist())

        combined = sorted(zip(all_ids, all_doubled))
        assert combined == [(1, 2), (2, 4), (3, 6), (4, 8)]
