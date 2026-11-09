from __future__ import annotations

import pickle
from concurrent.futures import ThreadPoolExecutor

import lance
import pyarrow as pa

from daft_lance._runtime import DatasetOpenOptions, open_dataset, shared_session
from daft_lance.lance_data_sink import LanceDataSink
from daft_lance.namespace import open_dataset_from_open_kwargs
from daft_lance.utils import construct_lance_dataset_handle


def test_concurrent_opens_share_session_and_keep_snapshot(tmp_path):
    uri = str(tmp_path / "table")
    lance.write_dataset(pa.table({"a": [1, 2]}), uri)
    handle = construct_lance_dataset_handle(uri, metadata_cache_size_bytes=32 * 1024 * 1024)
    with ThreadPoolExecutor(max_workers=4) as executor:
        datasets = list(
            executor.map(
                lambda _: open_dataset_from_open_kwargs(uri, handle.open_kwargs, read_scope=handle.read_scope), range(8)
            )
        )
    assert all(ds.session().is_same_as(datasets[0].session()) for ds in datasets)
    assert datasets[0].session().is_same_as(handle.dataset.session())

    lance.write_dataset(pa.table({"a": [99]}), uri, mode="overwrite")
    pinned = open_dataset_from_open_kwargs(uri, handle.open_kwargs, read_scope=handle.read_scope)
    assert pinned.version == handle.dataset.version
    assert pinned.to_table().to_pydict() == {"a": [1, 2]}
    assert open_dataset(uri).to_table().to_pydict() == {"a": [99]}


def test_cache_scope_and_capacity(tmp_path, monkeypatch):
    uri = str(tmp_path / "scope")
    first = shared_session(uri, storage_options={"region": "a", "endpoint": "b"})
    assert first.is_same_as(shared_session(uri, storage_options={"endpoint": "b", "region": "a"}))
    assert not first.is_same_as(shared_session(uri, storage_options={"region": "other"}))
    assert not first.is_same_as(shared_session(uri, metadata_cache_size_bytes=1024))
    monkeypatch.setenv("DAFT_LANCE_INDEX_CACHE_SIZE_BYTES", "1048576")
    configured = shared_session(uri)
    assert configured.is_same_as(shared_session(uri, index_cache_size_bytes=1048576))
    assert not configured.is_same_as(shared_session(uri, index_cache_size_bytes=2097152))
    # Keep pylance's public entry-count option equivalent to its byte option.
    assert shared_session(uri, index_cache_size=1).is_same_as(
        shared_session(uri, index_cache_size_bytes=20 * 1024 * 1024)
    )


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
    assert open_dataset(uri, DatasetOpenOptions(session=session)).session().is_same_as(session)


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


def test_v1_scan_snapshot_opens_once_per_query(tmp_path, monkeypatch):
    import daft_lance._runtime as runtime

    uri = str(tmp_path / "v1")
    lance.write_dataset(pa.table({"a": [1, 2]}), uri, enable_v2_manifest_paths=False)
    handle = construct_lance_dataset_handle(uri)
    opens = []
    original = lance.dataset

    def track_open(*args, **kwargs):
        opens.append(args)
        return original(*args, **kwargs)

    monkeypatch.setattr(runtime.lance, "dataset", track_open)
    with ThreadPoolExecutor(max_workers=4) as executor:
        datasets = list(
            executor.map(
                lambda _: open_dataset_from_open_kwargs(uri, handle.open_kwargs, read_scope=handle.read_scope), range(8)
            )
        )
    assert len(opens) == 1
    assert all(ds is datasets[0] for ds in datasets)
    assert datasets[0].to_table().to_pydict() == {"a": [1, 2]}
    # A new query must resolve its own snapshot, even at the same version.
    another = construct_lance_dataset_handle(uri)
    assert open_dataset_from_open_kwargs(uri, another.open_kwargs, read_scope=another.read_scope) is not datasets[0]
    # Maintenance code never gets the mutable Dataset shared with scanners.
    assert handle.worker_open_context().open_pinned() is not datasets[0]


def test_namespace_read_and_maintenance_share_session(tmp_path):
    from tests.io.lance.test_namespace import _ns_handle

    handle = _ns_handle(tmp_path)
    scan = open_dataset_from_open_kwargs(handle.uri, handle.open_kwargs)
    maintenance = handle.worker_open_context().open_pinned()
    assert scan.session().is_same_as(maintenance.session())
