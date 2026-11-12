from __future__ import annotations

import pickle
from concurrent.futures import ThreadPoolExecutor

import lance
import pyarrow as pa
import pytest

from daft_lance._runtime import _reset_session, open_dataset, shared_session
from daft_lance.lance_data_sink import LanceDataSink
from daft_lance.namespace import open_dataset_from_open_kwargs
from daft_lance.utils import construct_lance_dataset_handle


@pytest.fixture(autouse=True)
def reset_global_session():
    _reset_session()
    yield
    _reset_session()


def test_concurrent_opens_share_session_and_keep_snapshot(tmp_path):
    uri = str(tmp_path / "table")
    lance.write_dataset(pa.table({"a": [1, 2]}), uri)
    handle = construct_lance_dataset_handle(uri)
    with ThreadPoolExecutor(max_workers=4) as executor:
        datasets = list(executor.map(lambda _: open_dataset_from_open_kwargs(uri, handle.open_kwargs), range(8)))
    assert len({id(ds) for ds in datasets}) == len(datasets)
    assert all(ds.session().is_same_as(datasets[0].session()) for ds in datasets)
    assert datasets[0].session().is_same_as(handle.dataset.session())

    lance.write_dataset(pa.table({"a": [99]}), uri, mode="overwrite")
    pinned = open_dataset_from_open_kwargs(uri, handle.open_kwargs)
    assert pinned.version == handle.dataset.version
    assert pinned.to_table().to_pydict() == {"a": [1, 2]}
    assert open_dataset(uri).to_table().to_pydict() == {"a": [99]}


@pytest.mark.parametrize("option", ["metadata_cache_size_bytes", "index_cache_size_bytes", "index_cache_size"])
@pytest.mark.parametrize("size", [0, 1048576])
def test_explicit_capacity_is_independent(tmp_path, option, size):
    uri = str(tmp_path / "table")
    lance.write_dataset(pa.table({"a": [1]}), uri)
    default = open_dataset(uri)
    custom = open_dataset(uri, **{option: size})
    assert not custom.session().is_same_as(default.session())
    assert open_dataset(uri).session().is_same_as(default.session())
    assert custom.to_table().to_pydict() == {"a": [1]}
    _reset_session()
    custom_first = open_dataset(uri, **{option: size})
    assert not open_dataset(uri).session().is_same_as(custom_first.session())


def test_invalid_shared_configuration_and_reset(tmp_path, monkeypatch):
    uri = str(tmp_path / "table")
    lance.write_dataset(pa.table({"a": [1]}), uri)
    monkeypatch.setenv("DAFT_LANCE_METADATA_CACHE_SIZE_BYTES", "1GiB")
    with pytest.raises(ValueError, match="DAFT_LANCE_METADATA_CACHE_SIZE_BYTES"):
        open_dataset(uri)
    custom = open_dataset(uri, metadata_cache_size_bytes=1024)
    assert custom.to_table().num_rows == 1
    monkeypatch.setenv("DAFT_LANCE_METADATA_CACHE_SIZE_BYTES", "1048576")
    first = open_dataset(uri)
    _reset_session()
    assert not first.session().is_same_as(open_dataset(uri).session())
    assert first.to_table().num_rows == 1


def test_maintenance_explicit_capacity_is_independent(tmp_path):
    uri = str(tmp_path / "maintenance-custom")
    lance.write_dataset(pa.table({"a": [1]}), uri)
    default = shared_session()
    handle = construct_lance_dataset_handle(uri, metadata_cache_size_bytes=1024)
    worker = handle.worker_open_context().open_pinned()
    assert not worker.session().is_same_as(default)
    assert not worker.session().is_same_as(handle.dataset.session())
    assert worker.to_table().to_pydict() == {"a": [1]}


def test_fragment_writes_share_worker_session_without_serializing_it(tmp_path, monkeypatch):
    uri = str(tmp_path / "writes")
    schema = pa.schema([("a", pa.int64())])
    lance.write_dataset(pa.table({"a": [0]}), uri)
    sink = LanceDataSink(uri=uri, schema=schema, mode="append")
    sink.start()
    worker = pickle.loads(pickle.dumps(sink))
    sessions = []
    original_write = lance.fragment.write_fragments

    def write(*args, **kwargs):
        sessions.append(kwargs["session"])
        return original_write(*args, **kwargs)

    monkeypatch.setattr(lance.fragment, "write_fragments", write)
    results = [worker._write_arrow_table(pa.table({"a": [value]})) for value in [1, 2]]
    assert sessions[0].is_same_as(sessions[1])
    assert sessions[0].is_same_as(open_dataset(uri).session())
    fragments = [fragment for result in results for fragment in result.result]
    committed = lance.LanceDataset.commit(uri, lance.LanceOperation.Append(fragments), read_version=1)
    assert sorted(committed.to_table()["a"].to_pylist()) == [0, 1, 2]


def test_explicit_session_is_preserved(tmp_path):
    uri = str(tmp_path / "explicit")
    lance.write_dataset(pa.table({"a": [1]}), uri)
    session = lance.Session(metadata_cache_size_bytes=1024)
    assert open_dataset(uri, session=session).session().is_same_as(session)


def test_maintenance_context_reuses_session_and_latest_still_advances(tmp_path):
    uri = str(tmp_path / "maintenance")
    lance.write_dataset(pa.table({"a": [1]}), uri)
    handle = construct_lance_dataset_handle(uri)
    context = pickle.loads(pickle.dumps(handle.worker_open_context()))
    first = context.open_pinned()
    second = context.open_pinned()
    assert first.session().is_same_as(second.session())
    assert first.session().is_same_as(handle.dataset.session())
    lance.write_dataset(pa.table({"a": [2]}), uri, mode="append")
    assert context.open_pinned().to_table().to_pydict() == {"a": [1]}
    assert context.open_latest().to_table().to_pydict() == {"a": [1, 2]}


def test_namespace_read_and_maintenance_share_session(tmp_path):
    from tests.io.lance.test_namespace import _ns_handle

    handle = _ns_handle(tmp_path)
    scan = open_dataset_from_open_kwargs(handle.uri, handle.open_kwargs)
    maintenance = handle.worker_open_context().open_pinned()
    assert scan.session().is_same_as(maintenance.session())
