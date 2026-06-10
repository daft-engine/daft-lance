from __future__ import annotations

import pathlib
import tempfile

import lance
import pandas as pd
import pytest

import daft_lance


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as tmp:
        yield pathlib.Path(tmp)


@pytest.fixture
def lance_dataset(temp_dir):
    path = str(temp_dir / "test.lance")
    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "age": [25, 30, 35, 40, 45],
        }
    )
    lance.write_dataset(df, path)
    return path


@pytest.fixture
def lance_dataset_with_nulls(temp_dir):
    path = str(temp_dir / "nulls.lance")
    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", None, "Charlie", "David", None],
            "age": [25, 30, None, 40, 45],
        }
    )
    lance.write_dataset(df, path)
    return path


class TestUpdateLance:
    def test_update_matching_rows(self, lance_dataset):
        result = daft_lance.update_lance(
            lance_dataset, updates={"age": "age + 1"}, where="age > 35"
        )
        assert result["num_rows_updated"] == 2

        ds = lance.dataset(lance_dataset)
        table = ds.to_table().to_pandas()
        assert table.loc[table["id"] == 3, "age"].values[0] == 35
        assert table.loc[table["id"] == 4, "age"].values[0] == 41
        assert table.loc[table["id"] == 5, "age"].values[0] == 46

    def test_update_all_rows(self, lance_dataset):
        result = daft_lance.update_lance(lance_dataset, updates={"age": "age + 10"})
        assert result["num_rows_updated"] == 5

        ds = lance.dataset(lance_dataset)
        table = ds.to_table().to_pandas()
        assert table["age"].to_list() == [35, 40, 45, 50, 55]

    def test_update_string_column(self, lance_dataset):
        result = daft_lance.update_lance(
            lance_dataset, updates={"name": "'Updated'"}, where="id = 1"
        )
        assert result["num_rows_updated"] == 1

        ds = lance.dataset(lance_dataset)
        table = ds.to_table().to_pandas()
        assert table.loc[table["id"] == 1, "name"].values[0] == "Updated"
        assert table.loc[table["id"] == 2, "name"].values[0] == "Bob"

    def test_update_no_matching_rows(self, lance_dataset):
        result = daft_lance.update_lance(
            lance_dataset, updates={"age": "age + 1"}, where="age > 100"
        )
        assert result["num_rows_updated"] == 0

    def test_update_multiple_columns(self, lance_dataset):
        result = daft_lance.update_lance(
            lance_dataset,
            updates={"age": "age + 5", "name": "'Modified'"},
            where="id IN (1, 2)",
        )
        assert result["num_rows_updated"] == 2

        ds = lance.dataset(lance_dataset)
        table = ds.to_table().to_pandas()
        row1 = table[table["id"] == 1]
        assert row1["age"].values[0] == 30
        assert row1["name"].values[0] == "Modified"
        row3 = table[table["id"] == 3]
        assert row3["name"].values[0] == "Charlie"


class TestDeleteFromLance:
    def test_delete_matching_rows(self, lance_dataset):
        daft_lance.delete_from_lance(lance_dataset, where="age > 35")

        ds = lance.dataset(lance_dataset)
        table = ds.to_table().to_pandas()
        assert len(table) == 3
        assert set(table["id"].to_list()) == {1, 2, 3}

    def test_delete_single_row(self, lance_dataset):
        daft_lance.delete_from_lance(lance_dataset, where="id = 3")

        ds = lance.dataset(lance_dataset)
        table = ds.to_table().to_pandas()
        assert len(table) == 4
        assert 3 not in table["id"].values

    def test_delete_no_matching_rows(self, lance_dataset):
        daft_lance.delete_from_lance(lance_dataset, where="age > 100")

        ds = lance.dataset(lance_dataset)
        table = ds.to_table().to_pandas()
        assert len(table) == 5

    def test_delete_with_null_check(self, lance_dataset_with_nulls):
        daft_lance.delete_from_lance(lance_dataset_with_nulls, where="name IS NULL")

        ds = lance.dataset(lance_dataset_with_nulls)
        table = ds.to_table().to_pandas()
        assert len(table) == 3
        for name in table["name"]:
            assert name is not None

    def test_delete_with_is_not_null(self, lance_dataset_with_nulls):
        daft_lance.delete_from_lance(
            lance_dataset_with_nulls, where="name IS NOT NULL"
        )

        ds = lance.dataset(lance_dataset_with_nulls)
        table = ds.to_table().to_pandas()
        assert len(table) == 2
