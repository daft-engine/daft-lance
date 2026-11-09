from __future__ import annotations

import json
import os
from collections import OrderedDict
from dataclasses import dataclass, fields, replace
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import TYPE_CHECKING, Any

import lance

# pylance exposes this public alias at runtime but omits its stub export.
from lance import Session  # type: ignore[attr-defined]
from lance_namespace import LanceNamespace

if TYPE_CHECKING:
    from lance.commit import CommitLock

# Keep native handles in the executing process, never in serialized task state.
_SESSION_LIMIT = 16
_sessions: OrderedDict[tuple[Any, ...], tuple[Session, Any]] = OrderedDict()
_session_lock = Lock()
# Scan/count consumers never mutate these datasets. Keep only one query snapshot.
_read_datasets: OrderedDict[str, tuple[lance.LanceDataset, Session]] = OrderedDict()
_read_lock = Lock()


def _reset_after_fork() -> None:
    global _sessions, _session_lock, _read_datasets, _read_lock
    _sessions = OrderedDict()
    _session_lock = Lock()
    _read_datasets = OrderedDict()
    _read_lock = Lock()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_after_fork)


DatasetUri = str | Path | None


def shared_session(
    uri: DatasetUri,
    *,
    storage_options: dict[str, str] | None = None,
    base_store_params: dict[str, dict[str, str]] | None = None,
    namespace_client: LanceNamespace | None = None,
    table_id: list[str] | None = None,
    index_cache_size: int | None = None,
    index_cache_size_bytes: int | None = None,
    metadata_cache_size_bytes: int | None = None,
) -> Session:
    """Reuse caches for the same destination, credentials and cache configuration.

    Version is deliberately absent: Lance keys immutable metadata by version.
    Retain the namespace client alongside the session so its identity cannot be
    reused while the cache entry is alive. No credentials are logged.
    """
    index_bytes = index_cache_size_bytes
    if index_bytes is None and index_cache_size is not None:
        # Match pylance's deprecated entry-count conversion.
        index_bytes = index_cache_size * 20 * 1024 * 1024
    metadata_bytes = metadata_cache_size_bytes
    if index_bytes is None:
        value = os.environ.get("DAFT_LANCE_INDEX_CACHE_SIZE_BYTES")
        index_bytes = int(value) if value is not None else None
    if metadata_bytes is None:
        value = os.environ.get("DAFT_LANCE_METADATA_CACHE_SIZE_BYTES")
        metadata_bytes = int(value) if value is not None else None
    client = namespace_client
    key = (
        os.getpid(),
        None if client is not None and table_id is not None else str(uri),
        json.dumps(storage_options, sort_keys=True),
        json.dumps(base_store_params, sort_keys=True),
        id(client),
        tuple(table_id or ()),
        index_bytes,
        metadata_bytes,
    )
    # lru_cache can invoke its factory twice on concurrent misses. Serialize the
    # lookup and creation so concurrent tasks actually share the first session.
    with _session_lock:
        if key not in _sessions:
            _sessions[key] = (
                Session(index_cache_size_bytes=index_bytes, metadata_cache_size_bytes=metadata_bytes),
                client,
            )
            if len(_sessions) > _SESSION_LIMIT:
                _sessions.popitem(last=False)
        _sessions.move_to_end(key)
        return _sessions[key][0]


@dataclass(frozen=True)
class DatasetOpenOptions:
    """Typed pylance open parameters; live handles stay in the current process."""

    version: int | str | None = None
    asof: str | datetime | None = None
    block_size: int | None = None
    commit_lock: CommitLock | None = None
    index_cache_size: int | None = None
    storage_options: dict[str, str] | None = None
    default_scan_options: dict[str, Any] | None = None
    metadata_cache_size_bytes: int | None = None
    index_cache_size_bytes: int | None = None
    read_params: dict[str, Any] | None = None
    session: Session | None = None
    namespace_client: LanceNamespace | None = None
    table_id: list[str] | None = None
    base_store_params: dict[str, dict[str, str]] | None = None

    def resolve_session(self, uri: DatasetUri) -> Session:
        if self.session is not None:
            return self.session
        return shared_session(
            uri,
            storage_options=self.storage_options,
            base_store_params=self.base_store_params,
            namespace_client=self.namespace_client,
            table_id=self.table_id,
            index_cache_size=self.index_cache_size,
            index_cache_size_bytes=self.index_cache_size_bytes,
            metadata_cache_size_bytes=self.metadata_cache_size_bytes,
        )


def open_dataset(uri: DatasetUri, options: DatasetOpenOptions | None = None) -> lance.LanceDataset:
    options = options if options is not None else DatasetOpenOptions()
    return lance.dataset(
        uri,
        version=options.version,
        asof=options.asof,
        block_size=options.block_size,
        commit_lock=options.commit_lock,
        index_cache_size=options.index_cache_size,
        storage_options=options.storage_options,
        default_scan_options=options.default_scan_options,
        metadata_cache_size_bytes=options.metadata_cache_size_bytes,
        index_cache_size_bytes=options.index_cache_size_bytes,
        read_params=options.read_params,
        session=options.resolve_session(uri),
        namespace_client=options.namespace_client,
        table_id=options.table_id,
        base_store_params=options.base_store_params,
    )


def open_read_dataset(
    uri: DatasetUri, options: DatasetOpenOptions, *, read_scope: str | None = None
) -> lance.LanceDataset:
    """Reuse one immutable scan snapshot, including V1 manifests uncached by Lance.

    A driver-generated scope separates independent queries even if a table is
    deleted and recreated at the same URI/version. Full open options separate
    different credentials and scanner defaults within a scope. Only numeric
    pinned versions are eligible; maintenance and latest opens never use this.
    """
    if read_scope is None or not isinstance(options.version, int):
        return open_dataset(uri, options)
    serializable = {
        field.name: getattr(options, field.name)
        for field in fields(options)
        if field.name not in {"namespace_client", "session"}
    }
    try:
        key = json.dumps([read_scope, str(uri), id(options.namespace_client), serializable], sort_keys=True)
    except (TypeError, ValueError):
        # Custom scanner defaults/callbacks need not be JSON serializable.
        return open_dataset(uri, options)
    session = options.resolve_session(uri)
    key += f"/{id(session)}"
    with _read_lock:
        if key not in _read_datasets:
            _read_datasets[key] = (open_dataset(uri, replace(options, session=session)), session)
            if len(_read_datasets) > 1:
                _read_datasets.popitem(last=False)
        return _read_datasets[key][0]
