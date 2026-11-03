from __future__ import annotations

import datetime
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import lance
import pyarrow as pa
import pytest

import daft_lance
import daft_lance.lance_delta as lance_delta
from daft.daft import PyRecordBatch
from daft_lance import read_lance_changes
from daft_lance._metadata import convert_lance_schema
from daft_lance.lance_delta import ChangeMode

_MODES: tuple[ChangeMode, ...] = ("inserted", "updated", "deleted")

_MODE_READERS: dict[str, Callable[[Any], Any]] = {
    "inserted": lambda delta: delta.get_inserted_rows(),
    "updated": lambda delta: delta.get_updated_rows(),
    "deleted": lambda delta: delta.get_deleted_row_ids(),
}

_ROW_META_NAMES = ["_rowid", "_row_created_at_version", "_row_last_updated_at_version"]


def _write(table: pa.Table, uri: str, mode: str = "create") -> None:
    lance.write_dataset(table, uri, mode=mode, enable_stable_row_ids=True)


@pytest.fixture
def versioned_uri(tmp_path: Path) -> str:
    """A five-version table exercising append, repeated update, and delete.

    v1: ids 1,2,3 | v2: append 4,5 | v3: update 2 | v4: delete 3 | v5: update 2 again
    """
    uri = str(tmp_path / "versioned.lance")
    _write(pa.table({"id": [1, 2, 3], "label": ["a", "b", "c"]}), uri)
    _write(pa.table({"id": [4, 5], "label": ["d", "e"]}), uri, mode="append")
    ds = lance.dataset(uri)
    ds.update({"label": "'B'"}, where="id = 2")
    ds = lance.dataset(uri)
    ds.delete("id = 3")
    ds = lance.dataset(uri)
    ds.update({"label": "'BB'"}, where="id = 2")
    assert lance.dataset(uri).version == 5
    return uri


def _pylance_batches(uri: str, begin: int, end: int, mode: ChangeMode) -> list[pa.RecordBatch]:
    ds = lance.dataset(uri, version=end)
    delta = ds.delta(begin_version=begin, end_version=end)
    return list(_MODE_READERS[mode](delta))


def _sorted_rows(table: pa.Table) -> list[dict[str, Any]]:
    return sorted(table.to_pylist(), key=lambda row: int(row["_rowid"]))


def _int_values(column: Any) -> list[int]:
    return [int(value) for value in column.to_pylist() if value is not None]


def _live_rowids(uri: str, version: int) -> set[int]:
    table = lance.dataset(uri, version=version).to_table(columns=[], with_row_id=True)
    return set(_int_values(table["_rowid"]))


def _deleted_ids(uri: str, begin: int, end: int) -> set[int]:
    arrow = read_lance_changes(uri, begin_version=begin, end_version=end, mode="deleted").to_arrow()
    return set(_int_values(arrow["_rowid"]))


def test_public_export() -> None:
    assert daft_lance.read_lance_changes is read_lance_changes
    assert "read_lance_changes" in daft_lance.__all__


@pytest.mark.parametrize("mode", _MODES)
@pytest.mark.parametrize(("begin", "end"), [(1, 2), (1, 3), (1, 4), (1, 5), (2, 5), (3, 5), (4, 5), (2, 2)])
def test_matches_pylance(versioned_uri: str, begin: int, end: int, mode: ChangeMode) -> None:
    batches = _pylance_batches(versioned_uri, begin, end, mode)
    actual = read_lance_changes(versioned_uri, begin_version=begin, end_version=end, mode=mode).to_arrow()

    if batches:
        expected = pa.Table.from_batches(batches)
        # Daft widens Arrow strings to large_string, so compare through the
        # same schema conversion the reader uses.
        assert actual.schema == convert_lance_schema(expected.schema).to_pyarrow_schema()
        assert _sorted_rows(actual) == _sorted_rows(expected)
    else:
        assert actual.num_rows == 0


@pytest.mark.parametrize(("begin", "end"), [(1, 2), (1, 3), (1, 4), (1, 5), (2, 5), (3, 5), (4, 5)])
def test_deleted_ids_match_begin_live_minus_end_live(versioned_uri: str, begin: int, end: int) -> None:
    expected = _live_rowids(versioned_uri, begin) - _live_rowids(versioned_uri, end)
    assert _deleted_ids(versioned_uri, begin, end) == expected


def test_result_schema_includes_row_metadata(versioned_uri: str) -> None:
    dataset_schema = lance.dataset(versioned_uri).schema
    meta_fields = [pa.field(name, pa.uint64()) for name in _ROW_META_NAMES]

    for mode in ("inserted", "updated"):
        arrow = read_lance_changes(versioned_uri, begin_version=1, end_version=5, mode=mode).to_arrow()
        expected = convert_lance_schema(pa.schema([*dataset_schema, *meta_fields])).to_pyarrow_schema()
        assert arrow.schema == expected

    deleted = read_lance_changes(versioned_uri, begin_version=1, end_version=5, mode="deleted").to_arrow()
    assert deleted.schema == convert_lance_schema(pa.schema([meta_fields[0]])).to_pyarrow_schema()


def test_equal_endpoints_return_empty_results(versioned_uri: str) -> None:
    for mode in _MODES:
        arrow = read_lance_changes(versioned_uri, begin_version=3, end_version=3, mode=mode).to_arrow()
        assert arrow.num_rows == 0

    inserted = read_lance_changes(versioned_uri, begin_version=3, end_version=3).to_arrow()
    assert inserted.schema.names[-3:] == _ROW_META_NAMES
    deleted = read_lance_changes(versioned_uri, begin_version=3, end_version=3, mode="deleted").to_arrow()
    assert deleted.schema.names == ["_rowid"]


def test_reversed_endpoints_raise(versioned_uri: str) -> None:
    with pytest.raises(ValueError, match="must be <="):
        read_lance_changes(versioned_uri, begin_version=4, end_version=2)


def test_begin_version_zero_raises(versioned_uri: str) -> None:
    with pytest.raises(ValueError, match="begin_version must be >= 1"):
        read_lance_changes(versioned_uri, begin_version=0, end_version=2)


def test_non_integer_versions_raise(versioned_uri: str) -> None:
    with pytest.raises(TypeError):
        read_lance_changes(versioned_uri, begin_version=True, end_version=2)
    with pytest.raises(TypeError):
        read_lance_changes(versioned_uri, begin_version=1.0, end_version=2)  # type: ignore[arg-type]


def test_unknown_mode_raises(versioned_uri: str) -> None:
    with pytest.raises(ValueError, match="mode must be one of"):
        read_lance_changes(versioned_uri, begin_version=1, end_version=2, mode="upserted")  # type: ignore[arg-type]


def test_unavailable_end_version_raises(versioned_uri: str) -> None:
    with pytest.raises(ValueError, match="was not found"):
        read_lance_changes(versioned_uri, begin_version=1, end_version=99)


def test_unavailable_begin_version_raises(tmp_path: Path) -> None:
    uri = str(tmp_path / "cleaned.lance")
    _write(pa.table({"id": [1, 2]}), uri)
    _write(pa.table({"id": [3]}), uri, mode="append")
    lance.dataset(uri).cleanup_old_versions(older_than=datetime.timedelta(0), delete_unverified=True)

    with pytest.raises(ValueError, match="was not found"):
        read_lance_changes(uri, begin_version=1, end_version=2)


def test_stable_row_ids_required(tmp_path: Path) -> None:
    uri = str(tmp_path / "unstable.lance")
    lance.write_dataset(pa.table({"id": [1]}), uri)
    lance.write_dataset(pa.table({"id": [2]}), uri, mode="append")

    with pytest.raises(ValueError, match="stable row ids"):
        read_lance_changes(uri, begin_version=1, end_version=2)
    with pytest.raises(ValueError, match="stable row ids"):
        read_lance_changes(uri, begin_version=1, end_version=2, mode="deleted")

    # An empty range compares nothing, so it needs no stable row ids.
    assert read_lance_changes(uri, begin_version=2, end_version=2).to_arrow().num_rows == 0


def test_insert_then_delete_is_not_reported_as_deleted(tmp_path: Path) -> None:
    uri = str(tmp_path / "insert_then_delete.lance")
    _write(pa.table({"id": [1, 2]}), uri)
    _write(pa.table({"id": [3, 4]}), uri, mode="append")
    ds = lance.dataset(uri)
    ds.delete("id = 3")

    # Row 3 was created and deleted inside the interval, so it never existed
    # at the begin version and is not a deletion of the whole range.
    assert _deleted_ids(uri, 1, 3) == set()
    assert _deleted_ids(uri, 2, 3) == _live_rowids(uri, 2) - _live_rowids(uri, 3)
    inserted = read_lance_changes(uri, begin_version=1, end_version=2, mode="inserted").to_arrow()
    assert sorted(_int_values(inserted["id"])) == [3, 4]


def test_delete_then_reinsert_reports_deletion_and_insertion(tmp_path: Path) -> None:
    uri = str(tmp_path / "delete_reinsert.lance")
    _write(pa.table({"key": ["x"]}), uri)
    old_rowid = next(iter(_live_rowids(uri, 1)))

    ds = lance.dataset(uri)
    ds.delete("key = 'x'")
    _write(pa.table({"key": ["x"]}), uri, mode="append")

    assert _deleted_ids(uri, 1, 3) == {old_rowid}

    inserted = read_lance_changes(uri, begin_version=1, end_version=3, mode="inserted").to_arrow()
    assert inserted["key"].to_pylist() == ["x"]
    # The re-inserted row gets a new id; restoring the business key is not a
    # restoration of the deleted row id.
    assert inserted["_rowid"].to_pylist() != [old_rowid]


def test_whole_fragment_deletion_matches_live_difference(tmp_path: Path) -> None:
    uri = str(tmp_path / "fragment_delete.lance")
    lance.write_dataset(pa.table({"id": [0, 1, 2, 3]}), uri, max_rows_per_file=2, enable_stable_row_ids=True)
    assert lance.dataset(uri).to_table().num_rows == 4
    assert len(lance.dataset(uri).get_fragments()) == 2

    lance.dataset(uri).delete("id < 2")

    expected = _live_rowids(uri, 1) - _live_rowids(uri, 2)
    assert expected == {0, 1}
    assert _deleted_ids(uri, 1, 2) == expected


def test_compaction_does_not_look_like_deletion(tmp_path: Path) -> None:
    uri = str(tmp_path / "compaction.lance")
    lance.write_dataset(pa.table({"id": [0, 1, 2, 3]}), uri, max_rows_per_file=2, enable_stable_row_ids=True)
    ds = lance.dataset(uri)
    ds.delete("id = 1")
    before_compaction = lance.dataset(uri).version
    deleted_before = _live_rowids(uri, 1) - _live_rowids(uri, before_compaction)
    assert deleted_before

    lance.dataset(uri).optimize.compact_files()
    after_compaction = lance.dataset(uri).version
    assert after_compaction > before_compaction

    # Rewriting fragments is invisible: no rows were deleted or inserted by
    # compaction itself, and earlier deletions keep their ids.
    for mode in _MODES:
        arrow = read_lance_changes(
            uri, begin_version=before_compaction, end_version=after_compaction, mode=mode
        ).to_arrow()
        assert arrow.num_rows == 0
    assert _deleted_ids(uri, 1, after_compaction) == deleted_before


def test_commit_after_planning_does_not_change_results(versioned_uri: str) -> None:
    frame = read_lance_changes(versioned_uri, begin_version=1, end_version=2)
    _write(pa.table({"id": [6], "label": ["f"]}), versioned_uri, mode="append")
    assert lance.dataset(versioned_uri).version == 6

    actual = frame.to_arrow()
    expected = pa.Table.from_batches(_pylance_batches(versioned_uri, 1, 2, "inserted"))
    assert _sorted_rows(actual) == _sorted_rows(expected)
    assert 6 not in actual["id"].to_pylist()


def test_factory_runs_when_the_dataframe_executes(versioned_uri: str, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[Any, ...]] = []
    real = lance_delta._lance_delta_factory_function

    def tracking(*args: Any, **kwargs: Any) -> Iterator[PyRecordBatch]:
        calls.append(args)
        yield from real(*args, **kwargs)

    # Daft resolves the task by module and function name, so keep both pointing
    # at this module even though the name now resolves to the wrapper.
    tracking.__module__ = real.__module__
    tracking.__name__ = real.__name__
    monkeypatch.setattr(lance_delta, "_lance_delta_factory_function", tracking)

    frame = read_lance_changes(versioned_uri, begin_version=1, end_version=2)
    assert calls == []
    frame.collect()
    assert len(calls) == 1


class _RecordingReader:
    def __init__(self, batches: list[pa.RecordBatch]) -> None:
        self._batches = batches
        self.consumed = 0

    def __iter__(self) -> Iterator[pa.RecordBatch]:
        for batch in self._batches:
            self.consumed += 1
            yield batch


class _FakeDelta:
    def __init__(self, reader: _RecordingReader) -> None:
        self._reader = reader

    def get_inserted_rows(self) -> _RecordingReader:
        return self._reader

    def get_updated_rows(self) -> _RecordingReader:
        return self._reader

    def get_deleted_row_ids(self) -> _RecordingReader:
        return self._reader


class _FakeDeltaDataset:
    def __init__(self, reader: _RecordingReader) -> None:
        self._reader = reader
        self.delta_calls = 0

    def delta(self, *, begin_version: int, end_version: int) -> _FakeDelta:
        self.delta_calls += 1
        return _FakeDelta(self._reader)


def test_factory_consumes_batches_incrementally(monkeypatch: pytest.MonkeyPatch) -> None:
    batches = [pa.record_batch({"id": [index]}) for index in range(3)]
    reader = _RecordingReader(batches)
    fake = _FakeDeltaDataset(reader)
    monkeypatch.setattr(lance_delta, "open_dataset_from_open_kwargs", lambda uri, kwargs: fake)

    stream = lance_delta._lance_delta_factory_function("memory://delta", None, 1, 2, "inserted")
    assert reader.consumed == 0

    next(stream)
    assert reader.consumed == 1
    assert len(list(stream)) == 2
    assert reader.consumed == 3


def test_factory_skips_the_reader_for_an_empty_range(monkeypatch: pytest.MonkeyPatch) -> None:
    reader = _RecordingReader([])
    fake = _FakeDeltaDataset(reader)
    monkeypatch.setattr(lance_delta, "open_dataset_from_open_kwargs", lambda uri, kwargs: fake)

    assert list(lance_delta._lance_delta_factory_function("memory://delta", None, 2, 2, "inserted")) == []
    assert fake.delta_calls == 0
    assert reader.consumed == 0


def test_namespace_context_reconstruction(tmp_path: Path) -> None:
    root = tmp_path / "namespace_root"
    root.mkdir()
    uri = str(root / "tbl.lance")
    _write(pa.table({"id": [1, 2]}), uri)
    _write(pa.table({"id": [3]}), uri, mode="append")

    ns: dict[str, Any] = {"namespace_impl": "dir", "namespace_properties": {"root": str(root)}}
    frame = read_lance_changes(table_id=["tbl"], begin_version=1, end_version=2, **ns)
    # A commit after planning must not leak into the namespace-addressed read.
    _write(pa.table({"id": [4]}), uri, mode="append")
    actual = frame.to_arrow()

    assert sorted(_int_values(actual["id"])) == [3]
    assert actual.schema.names[-3:] == _ROW_META_NAMES
