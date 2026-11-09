from __future__ import annotations

import json
import os
from collections import OrderedDict
from threading import Lock
from typing import Any

import lance

# pylance exposes this public alias at runtime but omits its stub export.
from lance import Session  # type: ignore[attr-defined]

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


def shared_session(uri: Any, **options: Any) -> Session:
    """Reuse caches for the same destination, credentials and cache configuration.

    Version is deliberately absent: Lance keys immutable metadata by version.
    Retain the namespace client alongside the session so its identity cannot be
    reused while the cache entry is alive. No credentials are logged.
    """
    index_bytes = options.get("index_cache_size_bytes")
    if index_bytes is None and options.get("index_cache_size") is not None:
        # Match pylance's deprecated entry-count conversion.
        index_bytes = options["index_cache_size"] * 20 * 1024 * 1024
    metadata_bytes = options.get("metadata_cache_size_bytes")
    if index_bytes is None:
        value = os.environ.get("DAFT_LANCE_INDEX_CACHE_SIZE_BYTES")
        index_bytes = int(value) if value is not None else None
    if metadata_bytes is None:
        value = os.environ.get("DAFT_LANCE_METADATA_CACHE_SIZE_BYTES")
        metadata_bytes = int(value) if value is not None else None
    client = options.get("namespace_client")
    key = (
        os.getpid(),
        None if client is not None and options.get("table_id") is not None else str(uri),
        json.dumps(options.get("storage_options"), sort_keys=True),
        json.dumps(options.get("base_store_params"), sort_keys=True),
        id(client),
        tuple(options.get("table_id") or ()),
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


def session_kwargs(uri: Any, options: dict[str, Any]) -> dict[str, Any]:
    """Respect explicit sessions and legacy metadata-cache configuration."""
    if options.get("session") is not None or options.get("metadata_cache_size") is not None:
        return options
    return {**options, "session": shared_session(uri, **options)}


def open_dataset(uri: Any, **options: Any) -> lance.LanceDataset:
    return lance.dataset(uri, **session_kwargs(uri, options))


def open_read_dataset(uri: Any, *, read_scope: str | None = None, **options: Any) -> lance.LanceDataset:
    """Reuse one immutable scan snapshot, including V1 manifests uncached by Lance.

    A driver-generated scope separates independent queries even if a table is
    deleted and recreated at the same URI/version. Full open options separate
    different credentials and scanner defaults within a scope. Only numeric
    pinned versions are eligible; maintenance and latest opens never use this.
    """
    if (
        read_scope is None
        or not isinstance(options.get("version"), int)
        or options.get("metadata_cache_size") is not None
    ):
        return open_dataset(uri, **options)
    namespace_client = options.get("namespace_client")
    serializable = {k: v for k, v in options.items() if k not in {"namespace_client", "session"}}
    try:
        key = json.dumps([read_scope, str(uri), id(namespace_client), serializable], sort_keys=True)
    except (TypeError, ValueError):
        # Custom scanner defaults/callbacks need not be JSON serializable.
        return open_dataset(uri, **options)
    session = options.get("session") or shared_session(uri, **options)
    key += f"/{id(session)}"
    with _read_lock:
        if key not in _read_datasets:
            _read_datasets[key] = (open_dataset(uri, **{**options, "session": session}), session)
            if len(_read_datasets) > 1:
                _read_datasets.popitem(last=False)
        return _read_datasets[key][0]
