from __future__ import annotations

import os

import lance
import pyarrow as pa
import pytest

import daft

from .conftest import sort_pydict

_DATA_ROOT = {"x": [1, 2, 3], "y": ["a", "b", "c"]}
_DATA_BUCKET2 = {"x": [4, 5, 6], "y": ["d", "e", "f"]}
_DATA_BUCKET3 = {"x": [7, 8, 9], "y": ["g", "h", "i"]}


@pytest.fixture()
def multi_base_dataset(tmp_path):
    primary = tmp_path / "primary"
    primary.mkdir()
    bucket2 = tmp_path / "bucket2"
    bucket2.mkdir()

    base2 = lance.DatasetBasePath(id=1, name="bucket-2", path=str(bucket2), is_dataset_root=False)
    lance.write_dataset(pa.Table.from_pydict(_DATA_ROOT), str(primary), initial_bases=[base2])
    lance.write_dataset(pa.Table.from_pydict(_DATA_BUCKET2), str(primary), mode="append", target_bases=["bucket-2"])

    yield str(primary), str(bucket2)


@pytest.fixture()
def three_base_dataset(tmp_path):
    primary = tmp_path / "primary"
    primary.mkdir()
    bucket2 = tmp_path / "bucket2"
    bucket2.mkdir()
    bucket3 = tmp_path / "bucket3"
    bucket3.mkdir()

    base2 = lance.DatasetBasePath(id=1, name="bucket-2", path=str(bucket2), is_dataset_root=False)
    base3 = lance.DatasetBasePath(id=2, name="bucket-3", path=str(bucket3), is_dataset_root=False)

    lance.write_dataset(pa.Table.from_pydict(_DATA_ROOT), str(primary), initial_bases=[base2, base3])
    lance.write_dataset(pa.Table.from_pydict(_DATA_BUCKET2), str(primary), mode="append", target_bases=["bucket-2"])
    lance.write_dataset(pa.Table.from_pydict(_DATA_BUCKET3), str(primary), mode="append", target_bases=["bucket-3"])

    yield str(primary), str(bucket2), str(bucket3)


class TestBaseStoreParamsLocal:
    """Black-box tests for base_store_params with local filesystem.

    These tests verify that passing base_store_params to read_lance does not
    break normal reads and that the parameter is correctly forwarded to the
    underlying lance.dataset() call.  For local paths the storage options are
    empty dicts because the local object store needs no credentials.
    """

    def test_read_with_empty_base_store_params(self, multi_base_dataset):
        primary, bucket2 = multi_base_dataset

        base_store_params = {
            bucket2: {},
        }
        df = daft.read_lance(primary, base_store_params=base_store_params)
        result = sort_pydict(df.to_pydict())
        expected = sort_pydict({"x": _DATA_ROOT["x"] + _DATA_BUCKET2["x"], "y": _DATA_ROOT["y"] + _DATA_BUCKET2["y"]})
        assert result == expected

    def test_read_with_none_base_store_params(self, multi_base_dataset):
        primary, _ = multi_base_dataset

        df = daft.read_lance(primary, base_store_params=None)
        result = sort_pydict(df.to_pydict())
        expected = sort_pydict({"x": _DATA_ROOT["x"] + _DATA_BUCKET2["x"], "y": _DATA_ROOT["y"] + _DATA_BUCKET2["y"]})
        assert result == expected

    def test_read_without_base_store_params_same_as_none(self, multi_base_dataset):
        primary, _ = multi_base_dataset

        df_without = daft.read_lance(primary)
        df_with_none = daft.read_lance(primary, base_store_params=None)
        assert sort_pydict(df_without.to_pydict()) == sort_pydict(df_with_none.to_pydict())

    def test_read_three_bases_with_base_store_params(self, three_base_dataset):
        primary, bucket2, bucket3 = three_base_dataset

        base_store_params = {
            bucket2: {},
            bucket3: {},
        }
        df = daft.read_lance(primary, base_store_params=base_store_params)
        result = sort_pydict(df.to_pydict())
        all_x = _DATA_ROOT["x"] + _DATA_BUCKET2["x"] + _DATA_BUCKET3["x"]
        all_y = _DATA_ROOT["y"] + _DATA_BUCKET2["y"] + _DATA_BUCKET3["y"]
        assert result == sort_pydict({"x": all_x, "y": all_y})

    def test_filter_with_base_store_params(self, multi_base_dataset):
        primary, bucket2 = multi_base_dataset

        base_store_params = {bucket2: {}}
        df = daft.read_lance(primary, base_store_params=base_store_params).filter(daft.col("x") > 3)
        result = df.to_pydict()
        assert sorted(result["x"]) == [4, 5, 6]
        assert sorted(result["y"]) == ["d", "e", "f"]

    def test_column_selection_with_base_store_params(self, multi_base_dataset):
        primary, bucket2 = multi_base_dataset

        base_store_params = {bucket2: {}}
        df = daft.read_lance(primary, base_store_params=base_store_params).select("x")
        result = df.to_pydict()
        assert sorted(result["x"]) == [1, 2, 3, 4, 5, 6]

    def test_version_with_base_store_params(self, multi_base_dataset):
        primary, bucket2 = multi_base_dataset

        base_store_params = {bucket2: {}}
        df = daft.read_lance(primary, base_store_params=base_store_params, version=1)
        result = sort_pydict(df.to_pydict())
        assert result == sort_pydict(_DATA_ROOT)

    def test_fragment_group_size_with_base_store_params(self, multi_base_dataset):
        primary, bucket2 = multi_base_dataset

        base_store_params = {bucket2: {}}
        df = daft.read_lance(primary, base_store_params=base_store_params, fragment_group_size=2)
        result = sort_pydict(df.to_pydict())
        expected = sort_pydict({"x": _DATA_ROOT["x"] + _DATA_BUCKET2["x"], "y": _DATA_ROOT["y"] + _DATA_BUCKET2["y"]})
        assert result == expected

    def test_base_store_params_matches_lance_dataset(self, multi_base_dataset):
        """Verify read_lance with base_store_params matches lance.dataset().

        This is a black-box consistency check: if pylance can read the dataset
        with base_store_params, daft.read_lance should too.
        """
        primary, bucket2 = multi_base_dataset

        base_store_params = {bucket2: {}}

        ds = lance.dataset(primary, base_store_params=base_store_params)
        lance_result = sort_pydict(ds.to_table().to_pydict())

        df = daft.read_lance(primary, base_store_params=base_store_params)
        daft_result = sort_pydict(df.to_pydict())

        assert daft_result == lance_result

    def test_base_store_params_preserved_in_open_kwargs(self, multi_base_dataset):
        """Verify base_store_params is included in _lance_open_kwargs.

        This checks the internal contract by verifying the LanceDataset object
        stored inside the scan operator carries the parameter.
        """
        primary, bucket2 = multi_base_dataset

        base_store_params = {bucket2: {}}

        from daft_lance.utils import construct_lance_dataset

        ds = construct_lance_dataset(
            primary,
            base_store_params=base_store_params,
        )
        open_kwargs = getattr(ds, "_lance_open_kwargs", None)
        assert open_kwargs is not None
        assert "base_store_params" in open_kwargs
        assert open_kwargs["base_store_params"] == base_store_params


class TestMultiBaseReadBasic:
    def test_read_all_rows(self, multi_base_dataset):
        primary, _ = multi_base_dataset
        df = daft.read_lance(primary)
        result = sort_pydict(df.to_pydict())
        expected = sort_pydict({"x": _DATA_ROOT["x"] + _DATA_BUCKET2["x"], "y": _DATA_ROOT["y"] + _DATA_BUCKET2["y"]})
        assert result == expected

    def test_column_selection(self, multi_base_dataset):
        primary, _ = multi_base_dataset
        df = daft.read_lance(primary).select("x")
        result = df.to_pydict()
        assert sorted(result["x"]) == [1, 2, 3, 4, 5, 6]

    def test_filter(self, multi_base_dataset):
        primary, _ = multi_base_dataset
        df = daft.read_lance(primary).filter(daft.col("x") > 3)
        result = df.to_pydict()
        assert sorted(result["x"]) == [4, 5, 6]
        assert sorted(result["y"]) == ["d", "e", "f"]

    def test_filter_with_column_selection(self, multi_base_dataset):
        primary, _ = multi_base_dataset
        df = daft.read_lance(primary).filter(daft.col("x") > 2).select("y")
        result = df.to_pydict()
        assert sorted(result["y"]) == ["c", "d", "e", "f"]

    def test_limit(self, multi_base_dataset):
        primary, _ = multi_base_dataset
        df = daft.read_lance(primary).limit(4)
        result = df.to_pydict()
        assert len(result["x"]) == 4

    def test_fragment_group_size(self, multi_base_dataset):
        primary, _ = multi_base_dataset
        df = daft.read_lance(primary, fragment_group_size=2)
        result = sort_pydict(df.to_pydict())
        expected = sort_pydict({"x": _DATA_ROOT["x"] + _DATA_BUCKET2["x"], "y": _DATA_ROOT["y"] + _DATA_BUCKET2["y"]})
        assert result == expected


class TestMultiBaseVersioning:
    def test_read_version_before_append(self, multi_base_dataset):
        primary, _ = multi_base_dataset
        df = daft.read_lance(primary, version=1)
        result = sort_pydict(df.to_pydict())
        assert result == sort_pydict(_DATA_ROOT)

    def test_read_latest_version(self, multi_base_dataset):
        primary, _ = multi_base_dataset
        df = daft.read_lance(primary)
        result = sort_pydict(df.to_pydict())
        expected = sort_pydict({"x": _DATA_ROOT["x"] + _DATA_BUCKET2["x"], "y": _DATA_ROOT["y"] + _DATA_BUCKET2["y"]})
        assert result == expected


class TestMultiBaseThreeBases:
    def test_read_all_rows(self, three_base_dataset):
        primary, _, _ = three_base_dataset
        df = daft.read_lance(primary)
        result = sort_pydict(df.to_pydict())
        all_x = _DATA_ROOT["x"] + _DATA_BUCKET2["x"] + _DATA_BUCKET3["x"]
        all_y = _DATA_ROOT["y"] + _DATA_BUCKET2["y"] + _DATA_BUCKET3["y"]
        assert result == sort_pydict({"x": all_x, "y": all_y})

    def test_filter_across_bases(self, three_base_dataset):
        primary, _, _ = three_base_dataset
        df = daft.read_lance(primary).filter(daft.col("x") >= 4)
        result = sort_pydict(df.to_pydict())
        assert result == sort_pydict({"x": [4, 5, 6, 7, 8, 9], "y": ["d", "e", "f", "g", "h", "i"]})

    def test_limit_across_bases(self, three_base_dataset):
        primary, _, _ = three_base_dataset
        df = daft.read_lance(primary).limit(5)
        result = df.to_pydict()
        assert len(result["x"]) == 5


class TestMultiBaseAddBases:
    def test_add_base_and_append(self, tmp_path):
        primary = tmp_path / "primary"
        primary.mkdir()
        bucket2 = tmp_path / "bucket2"
        bucket2.mkdir()
        bucket3 = tmp_path / "bucket3"
        bucket3.mkdir()

        base2 = lance.DatasetBasePath(id=1, name="bucket-2", path=str(bucket2), is_dataset_root=False)
        ds = lance.write_dataset(pa.Table.from_pydict(_DATA_ROOT), str(primary), initial_bases=[base2])

        base3 = lance.DatasetBasePath(id=2, name="bucket-3", path=str(bucket3), is_dataset_root=False)
        ds = ds.add_bases([base3])

        lance.write_dataset(pa.Table.from_pydict(_DATA_BUCKET3), str(primary), mode="append", target_bases=["bucket-3"])

        df = daft.read_lance(str(primary))
        result = sort_pydict(df.to_pydict())
        assert result == sort_pydict(
            {"x": _DATA_ROOT["x"] + _DATA_BUCKET3["x"], "y": _DATA_ROOT["y"] + _DATA_BUCKET3["y"]}
        )


class TestMultiBaseFragmentMetadata:
    def test_fragments_have_correct_base_ids(self, multi_base_dataset):
        primary, _ = multi_base_dataset
        ds = lance.dataset(primary)
        fragments = ds.get_fragments()
        assert len(fragments) == 2

        frag0_files = fragments[0].metadata.to_json()["files"]
        frag1_files = fragments[1].metadata.to_json()["files"]

        assert frag0_files[0]["base_id"] is None
        assert frag1_files[0]["base_id"] == 1

    def test_data_files_in_separate_directories(self, multi_base_dataset):
        primary, bucket2 = multi_base_dataset
        data_dir = os.path.join(primary, "data")
        assert os.path.isdir(data_dir)
        lance_files_in_root = [f for f in os.listdir(data_dir) if f.endswith(".lance")]
        assert len(lance_files_in_root) >= 1

        lance_files_in_bucket2 = [f for f in os.listdir(bucket2) if f.endswith(".lance")]
        assert len(lance_files_in_bucket2) >= 1


class TestMultiBaseIsDatasetRoot:
    def test_non_dataset_root_base_stores_files_directly(self, tmp_path):
        primary = tmp_path / "primary"
        primary.mkdir()
        bucket2 = tmp_path / "bucket2"
        bucket2.mkdir()

        base2 = lance.DatasetBasePath(id=1, name="bucket-2", path=str(bucket2), is_dataset_root=False)
        lance.write_dataset(pa.Table.from_pydict(_DATA_ROOT), str(primary), initial_bases=[base2])
        lance.write_dataset(pa.Table.from_pydict(_DATA_BUCKET2), str(primary), mode="append", target_bases=["bucket-2"])

        assert not os.path.isdir(os.path.join(bucket2, "data"))
        lance_files = [f for f in os.listdir(bucket2) if f.endswith(".lance")]
        assert len(lance_files) >= 1

    def test_dataset_root_base_uses_data_subdirectory(self, tmp_path):
        primary = tmp_path / "primary"
        primary.mkdir()

        lance.write_dataset(pa.Table.from_pydict(_DATA_ROOT), str(primary))

        assert os.path.isdir(os.path.join(primary, "data"))
