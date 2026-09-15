from __future__ import annotations

import contextlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import lance
import pytest

import daft
import daft_lance
from daft.dependencies import pa
from daft_lance.lance_update_column import _FragmentUpdateHandler


@pytest.fixture(scope="module", autouse=True)
def _isolate_strict_filter_pushdown() -> Iterator[None]:
    """Keep this module independent of planning-config mutations in read tests."""
    previous = daft.context.get_context().daft_planning_config.enable_strict_filter_pushdown
    daft.context.set_planning_config(enable_strict_filter_pushdown=False)
    yield
    daft.context.set_planning_config(enable_strict_filter_pushdown=previous)


def _read_update_source(path: str) -> daft.DataFrame:
    return daft_lance.read_lance(
        path,
        default_scan_options={"with_row_address": True},
        include_fragment_id=True,
    )


def test_fragment_update_handler_reuses_pinned_dataset() -> None:
    dataset = object()

    class OpenContext:
        def __init__(self) -> None:
            self.opens = 0

        def open_pinned(self) -> Any:
            self.opens += 1
            return dataset

    open_context = OpenContext()
    handler = _FragmentUpdateHandler(cast(Any, open_context), ["value"])

    assert handler._dataset() is dataset
    assert handler._dataset() is dataset
    assert open_context.opens == 1


def test_update_columns_df_partial_multi_fragment(tmp_path: Path) -> None:
    path = str(tmp_path / "partial-update.lance")
    daft.from_pydict(
        {
            "id": [0, 1, 2, 3],
            "value": [10, 20, 30, 40],
            "score": [1.0, 2.0, 3.0, 4.0],
        }
    ).write_lance(path, max_rows_per_file=2)

    before = lance.dataset(path)
    before_version = before.version
    before_data_files = {
        data_file["path"] for fragment in before.get_fragments() for data_file in fragment.metadata.to_json()["files"]
    }
    before_rows = _read_update_source(path).select("id", "_rowaddr").sort("id").to_pydict()

    source = _read_update_source(path).where(daft.col("id").is_in([1, 2]))
    source = source.with_columns(
        {
            "value": daft.col("value") + 100,
            "score": daft.col("score") * 10,
        }
    )
    result = daft_lance.update_columns_df(
        source,
        path,
        columns=["value", "score"],
        max_concurrency=2,
    )

    assert result == daft_lance.UpdateColumnsResult(
        version=before_version + 1,
        rows_updated=2,
    )
    after = daft_lance.read_lance(path).sort("id").to_pydict()
    assert after == {
        "id": [0, 1, 2, 3],
        "value": [10, 120, 130, 40],
        "score": [1.0, 20.0, 30.0, 4.0],
    }

    after_rows = _read_update_source(path).select("id", "_rowaddr").sort("id").to_pydict()
    assert after_rows == before_rows
    after_data_files = {
        data_file["path"]
        for fragment in lance.dataset(path).get_fragments()
        for data_file in fragment.metadata.to_json()["files"]
    }
    assert before_data_files < after_data_files
    old = lance.dataset(path, version=before_version).to_table().sort_by("id").to_pydict()
    assert old["value"] == [10, 20, 30, 40]
    assert old["score"] == [1.0, 2.0, 3.0, 4.0]


def test_update_columns_df_empty_is_noop(tmp_path: Path) -> None:
    path = str(tmp_path / "empty-update.lance")
    daft.from_pydict({"id": [1, 2], "value": [10, 20]}).write_lance(path)
    version = lance.dataset(path).version

    result = daft_lance.update_columns_df(
        _read_update_source(path).limit(0),
        path,
        columns=["value"],
    )

    assert result == daft_lance.UpdateColumnsResult(version=version, rows_updated=0)
    assert lance.dataset(path).version == version


@pytest.mark.parametrize(
    ("source_data", "message"),
    [
        (
            {"_rowaddr": [999], "fragment_id": [0], "value": [100]},
            "not live rows",
        ),
        (
            {"_rowaddr": [0, 0], "fragment_id": [0, 0], "value": [100, 200]},
            "Duplicate _rowaddr",
        ),
    ],
)
def test_update_columns_df_rejects_invalid_addresses(
    tmp_path: Path,
    source_data: dict[str, list[Any]],
    message: str,
) -> None:
    path = str(tmp_path / "invalid-address.lance")
    daft.from_pydict({"id": [1, 2], "value": [10, 20]}).write_lance(path)
    version = lance.dataset(path).version

    with pytest.raises(Exception, match=message):
        daft_lance.update_columns_df(
            daft.from_pydict(cast(Any, source_data)),
            path,
            columns=["value"],
        )

    assert lance.dataset(path).version == version


@pytest.mark.parametrize(
    ("columns", "message"),
    [
        ("value", "bare string"),
        ([], "at least one"),
        (["missing"], "non-existent"),
        (["_rowaddr"], "metadata column"),
        (["value", "value"], "Duplicate column"),
    ],
)
def test_update_columns_df_validates_targets_before_execution(
    tmp_path: Path,
    columns: Any,
    message: str,
) -> None:
    path = str(tmp_path / "invalid-target.lance")
    daft.from_pydict({"id": [1], "value": [10]}).write_lance(path)
    source = _read_update_source(path)

    with pytest.raises((TypeError, ValueError), match=message):
        daft_lance.update_columns_df(source, path, columns=columns)


def test_update_columns_df_safe_casts_to_target_type(tmp_path: Path) -> None:
    path = str(tmp_path / "safe-cast.lance")
    lance.write_dataset(
        pa.table({"id": pa.array([1, 2], type=pa.int32()), "value": pa.array([10, 20], type=pa.int32())}),
        path,
    )
    source = _read_update_source(path).with_column("value", daft.lit(123))

    result = daft_lance.update_columns_df(source, path, columns=["value"])

    assert result.rows_updated == 2
    table = lance.dataset(path).to_table()
    assert table.schema.field("value").type == pa.int32()
    assert table.column("value").to_pylist() == [123, 123]


def test_update_columns_df_rejects_stable_row_ids_before_writing(tmp_path: Path) -> None:
    path = str(tmp_path / "stable-row-ids.lance")
    lance.write_dataset(
        pa.table({"id": [1, 2], "value": [10, 20]}),
        path,
        enable_stable_row_ids=True,
    )
    source = _read_update_source(path).with_column("value", daft.col("value") + 1)
    before = lance.dataset(path)
    before_version = before.version
    before_values = before.to_table().column("value").to_pylist()

    with pytest.raises(NotImplementedError, match="does not support datasets with stable row IDs"):
        daft_lance.update_columns_df(source, path, columns=["value"])

    after = lance.dataset(path)
    assert after.version == before_version
    assert after.to_table().column("value").to_pylist() == before_values


def test_update_columns_df_uses_commit_lock(tmp_path: Path) -> None:
    path = str(tmp_path / "commit-lock.lance")
    daft.from_pydict({"id": [1], "value": [10]}).write_lance(path)
    source = _read_update_source(path).with_column("value", daft.lit(20))
    before_version = lance.dataset(path).version
    lock_calls: list[int] = []
    lock_released = False

    @contextlib.contextmanager
    def commit_lock(version: int) -> Iterator[None]:
        nonlocal lock_released
        lock_calls.append(version)
        try:
            yield
        finally:
            lock_released = True

    result = daft_lance.update_columns_df(source, path, columns=["value"], commit_lock=commit_lock)

    assert lock_calls
    assert lock_released
    assert result.version == before_version + 1
    assert lance.dataset(path).to_table().column("value").to_pylist() == [20]


def test_update_columns_df_rejects_deleted_row_address(tmp_path: Path) -> None:
    path = str(tmp_path / "deleted-row-address.lance")
    daft.from_pydict({"id": [1, 2], "value": [10, 20]}).write_lance(path)
    stale_source = _read_update_source(path).where("id = 2").with_column("value", daft.lit(200))
    lance.dataset(path).delete("id = 2")
    version_after_delete = lance.dataset(path).version

    with pytest.raises(Exception, match="not live rows"):
        daft_lance.update_columns_df(stale_source, path, columns=["value"])

    assert lance.dataset(path).version == version_after_delete


def test_update_columns_df_rejects_null_for_non_nullable_target(tmp_path: Path) -> None:
    path = str(tmp_path / "non-nullable.lance")
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("value", pa.int64(), nullable=False),
        ]
    )
    lance.write_dataset(
        pa.Table.from_arrays(
            [
                pa.array([1], type=pa.int64()),
                pa.array([10], type=pa.int64()),
            ],
            schema=schema,
        ),
        path,
    )
    source = _read_update_source(path).select("_rowaddr", "fragment_id").with_column("value", daft.lit(None))

    with pytest.raises(Exception, match="non-nullable column 'value'"):
        daft_lance.update_columns_df(source, path, columns=["value"])


def test_update_columns_df_namespace_roundtrip(tmp_path: Path) -> None:
    namespace: dict[str, Any] = {
        "namespace_impl": "dir",
        "namespace_properties": {"root": str(tmp_path)},
        "table_id": ["updates"],
    }
    daft_lance.write_lance(
        daft.from_pydict({"id": [1, 2], "value": [10, 20]}),
        mode="create",
        **namespace,
    ).collect()
    source = daft_lance.read_lance(
        default_scan_options={"with_row_address": True},
        include_fragment_id=True,
        **namespace,
    ).with_column("value", daft.col("value") * 10)

    result = daft_lance.update_columns_df(source, columns=["value"], **namespace)

    assert result.rows_updated == 2
    assert daft_lance.read_lance(**namespace).sort("id").to_pydict()["value"] == [100, 200]


def test_update_columns_df_fixed_size_list_uses_leaf_field_ids(tmp_path: Path) -> None:
    path = str(tmp_path / "fixed-size-list.lance")
    vector_type = pa.list_(pa.float32(), 2)
    lance.write_dataset(
        pa.table(
            {
                "id": [1, 2],
                "embedding": pa.array([[1.0, 2.0], [3.0, 4.0]], type=vector_type),
            }
        ),
        path,
    )
    addresses = _read_update_source(path).select("_rowaddr", "fragment_id").to_pydict()
    source = daft.from_pydict(
        {
            "_rowaddr": addresses["_rowaddr"],
            "fragment_id": addresses["fragment_id"],
            "embedding": pa.array([[9.0, 8.0], [7.0, 6.0]], type=vector_type),
        }
    )

    result = daft_lance.update_columns_df(source, path, columns=["embedding"])

    assert result.rows_updated == 2
    assert lance.dataset(path).to_table().column("embedding").to_pylist() == [[9.0, 8.0], [7.0, 6.0]]


def test_update_columns_df_prunes_updated_fragments_from_index(tmp_path: Path) -> None:
    path = str(tmp_path / "indexed-update.lance")
    daft.from_pydict(
        {
            "id": list(range(10)),
            "value": [value * 10 for value in range(10)],
        }
    ).write_lance(path, max_rows_per_file=5)
    lance.dataset(path).create_scalar_index("value", index_type="BTREE")
    source = _read_update_source(path).where("id = 2").with_column("value", daft.lit(999))

    daft_lance.update_columns_df(source, path, columns=["value"])

    result = lance.dataset(path).scanner(columns=["id", "value"], filter="value = 999").to_table().to_pydict()
    assert result == {"id": [2], "value": [999]}
