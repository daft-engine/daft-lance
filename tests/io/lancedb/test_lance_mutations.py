from __future__ import annotations

import pytest

import daft
import daft_lance


@pytest.fixture()
def lance_dataset_path(tmp_path_factory):
    tmp_dir = tmp_path_factory.mktemp("lance_mutations")
    return str(tmp_dir / "test_dataset")


@pytest.fixture()
def populated_dataset(lance_dataset_path):
    """Create a dataset with 5 rows for mutation tests."""
    df = daft.from_pydict(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["alice", "bob", "carol", "dave", "eve"],
            "score": [10.0, 20.0, 30.0, 40.0, 50.0],
        }
    )
    df.write_lance(lance_dataset_path, mode="create")
    return lance_dataset_path


class TestDelete:
    def test_delete_with_string_predicate(self, populated_dataset):
        daft_lance.delete(populated_dataset, "id > 3")
        result = daft.read_lance(populated_dataset).sort("id").to_pydict()
        assert result["id"] == [1, 2, 3]

    def test_delete_with_arrow_expression(self, populated_dataset):
        import pyarrow.compute as pc

        daft_lance.delete(populated_dataset, pc.field("name") == "bob")
        result = daft.read_lance(populated_dataset).sort("id").to_pydict()
        assert result["id"] == [1, 3, 4, 5]
        assert "bob" not in result["name"]

    def test_delete_no_match(self, populated_dataset):
        daft_lance.delete(populated_dataset, "id > 100")
        result = daft.read_lance(populated_dataset).to_pydict()
        assert len(result["id"]) == 5

    def test_delete_all_rows(self, populated_dataset):
        daft_lance.delete(populated_dataset, "id >= 1")
        result = daft.read_lance(populated_dataset).to_pydict()
        assert len(result["id"]) == 0


class TestUpdate:
    def test_update_all_rows(self, populated_dataset):
        daft_lance.update(populated_dataset, {"score": "score * 2"})
        result = daft.read_lance(populated_dataset).sort("id").to_pydict()
        assert result["score"] == [20.0, 40.0, 60.0, 80.0, 100.0]

    def test_update_with_where(self, populated_dataset):
        daft_lance.update(
            populated_dataset,
            {"score": "score + 100"},
            where="id <= 2",
        )
        result = daft.read_lance(populated_dataset).sort("id").to_pydict()
        assert result["score"] == [110.0, 120.0, 30.0, 40.0, 50.0]

    def test_update_string_column(self, populated_dataset):
        daft_lance.update(
            populated_dataset,
            {"name": "'unknown'"},
            where="id = 3",
        )
        result = daft.read_lance(populated_dataset).sort("id").to_pydict()
        assert result["name"][2] == "unknown"

    def test_update_no_match(self, populated_dataset):
        daft_lance.update(
            populated_dataset,
            {"score": "0"},
            where="id > 100",
        )
        result = daft.read_lance(populated_dataset).sort("id").to_pydict()
        assert result["score"] == [10.0, 20.0, 30.0, 40.0, 50.0]


class TestMergeInsert:
    def test_upsert_update_and_insert(self, populated_dataset):
        new_data = daft.from_pydict(
            {
                "id": [3, 6],
                "name": ["carol_updated", "frank"],
                "score": [300.0, 600.0],
            }
        )
        daft_lance.merge_insert(
            new_data,
            populated_dataset,
            on="id",
            when_matched_update_all=True,
            when_not_matched_insert_all=True,
        )
        result = daft.read_lance(populated_dataset).sort("id").to_pydict()
        assert result["id"] == [1, 2, 3, 4, 5, 6]
        assert result["name"][2] == "carol_updated"
        assert result["score"][2] == 300.0
        assert result["name"][5] == "frank"

    def test_upsert_insert_only(self, populated_dataset):
        new_data = daft.from_pydict(
            {
                "id": [6, 7],
                "name": ["frank", "grace"],
                "score": [60.0, 70.0],
            }
        )
        daft_lance.merge_insert(
            new_data,
            populated_dataset,
            on="id",
            when_not_matched_insert_all=True,
        )
        result = daft.read_lance(populated_dataset).sort("id").to_pydict()
        assert len(result["id"]) == 7
        assert result["id"][-2:] == [6, 7]

    def test_upsert_update_only(self, populated_dataset):
        new_data = daft.from_pydict(
            {
                "id": [1, 2],
                "name": ["alice_v2", "bob_v2"],
                "score": [100.0, 200.0],
            }
        )
        daft_lance.merge_insert(
            new_data,
            populated_dataset,
            on="id",
            when_matched_update_all=True,
        )
        result = daft.read_lance(populated_dataset).sort("id").to_pydict()
        assert len(result["id"]) == 5
        assert result["name"][0] == "alice_v2"
        assert result["name"][1] == "bob_v2"

    def test_merge_insert_delete_matched(self, populated_dataset):
        to_delete = daft.from_pydict(
            {
                "id": [2, 4],
                "name": ["bob", "dave"],
                "score": [20.0, 40.0],
            }
        )
        daft_lance.merge_insert(
            to_delete,
            populated_dataset,
            on="id",
            when_matched_delete=True,
        )
        result = daft.read_lance(populated_dataset).sort("id").to_pydict()
        assert result["id"] == [1, 3, 5]

    def test_merge_insert_delete_not_matched_by_source(self, populated_dataset):
        keep_data = daft.from_pydict(
            {
                "id": [1, 3],
                "name": ["alice", "carol"],
                "score": [10.0, 30.0],
            }
        )
        daft_lance.merge_insert(
            keep_data,
            populated_dataset,
            on="id",
            when_matched_update_all=True,
            when_not_matched_by_source_delete=True,
        )
        result = daft.read_lance(populated_dataset).sort("id").to_pydict()
        assert result["id"] == [1, 3]

    def test_merge_insert_with_condition(self, populated_dataset):
        new_data = daft.from_pydict(
            {
                "id": [1, 2],
                "name": ["alice_new", "bob_new"],
                "score": [5.0, 200.0],
            }
        )
        daft_lance.merge_insert(
            new_data,
            populated_dataset,
            on="id",
            when_matched_update_all=True,
            when_matched_update_all_condition="source.score > target.score",
        )
        result = daft.read_lance(populated_dataset).sort("id").to_pydict()
        # id=1: source score 5 < target 10, should NOT update
        assert result["name"][0] == "alice"
        # id=2: source score 200 > target 20, should update
        assert result["name"][1] == "bob_new"

    def test_merge_insert_composite_key(self, lance_dataset_path):
        df = daft.from_pydict(
            {
                "region": ["us", "us", "eu", "eu"],
                "id": [1, 2, 1, 2],
                "value": [10, 20, 30, 40],
            }
        )
        df.write_lance(lance_dataset_path, mode="create")

        new_data = daft.from_pydict(
            {
                "region": ["us", "eu"],
                "id": [1, 3],
                "value": [100, 300],
            }
        )
        daft_lance.merge_insert(
            new_data,
            lance_dataset_path,
            on=["region", "id"],
            when_matched_update_all=True,
            when_not_matched_insert_all=True,
        )
        result = daft.read_lance(lance_dataset_path).sort(["region", "id"]).to_pydict()
        assert len(result["id"]) == 5
        # eu,1 unchanged
        assert result["value"][0] == 30
        # eu,2 unchanged
        assert result["value"][1] == 40
        # eu,3 inserted
        assert result["value"][2] == 300
        # us,1 updated
        assert result["value"][3] == 100
        # us,2 unchanged
        assert result["value"][4] == 20
