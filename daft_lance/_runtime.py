from __future__ import annotations

import json
import os
from collections import OrderedDict
from collections.abc import Callable, Hashable
from concurrent.futures import Future
from dataclasses import dataclass, fields, replace
from datetime import datetime
from pathlib import Path
from threading import Lock, Thread
from time import monotonic
from typing import TYPE_CHECKING, Any, Generic, TypeVar

import lance

# pylance exposes this public alias at runtime but omits its stub export.
from lance import Session  # type: ignore[attr-defined]
from lance_namespace import LanceNamespace

if TYPE_CHECKING:
    from lance.commit import CommitLock

T = TypeVar("T")


class _Cache(Generic[T]):
    def __init__(self) -> None:
        self.lock = Lock()
        self.entries: OrderedDict[Hashable, tuple[T, float]] = OrderedDict()
        self.pending: dict[Hashable, Future[T]] = {}
        self.generation = 0

    def expire(self) -> None:
        released = []
        with self.lock:
            now = monotonic()
            for key, (_, deadline) in list(self.entries.items()):
                if deadline <= now:
                    released.append(self.entries.pop(key))
        # Drop native references outside the lock.
        released.clear()

    def clear(self) -> None:
        with self.lock:
            released = self.entries
            self.entries = OrderedDict()
            self.pending = {}
            self.generation += 1
        released.clear()

    def get(self, key: Hashable, load: Callable[[], T], capacity: int, ttl: int) -> T:
        _start_reaper()
        self.expire()
        with self.lock:
            if key in self.entries:
                value, _ = self.entries[key]
                self.entries[key] = (value, monotonic() + ttl)
                self.entries.move_to_end(key)
                return value
            future = self.pending.get(key)
            owner = future is None
            if future is None:
                future = Future()
                self.pending[key] = future
            generation = self.generation
        if not owner:
            return future.result()
        try:
            value = load()
        except BaseException as error:
            with self.lock:
                if self.pending.get(key) is future:
                    del self.pending[key]
            future.set_exception(error)
            raise
        released = []
        with self.lock:
            if generation == self.generation:
                self.entries[key] = (value, monotonic() + ttl)
                self.entries.move_to_end(key)
                while len(self.entries) > capacity:
                    released.append(self.entries.popitem(last=False))
            if self.pending.get(key) is future:
                del self.pending[key]
        future.set_result(value)
        released.clear()
        return value


_session_cache: _Cache[tuple[Session, LanceNamespace | None]] = _Cache()
_dataset_cache: _Cache[tuple[lance.LanceDataset, Session]] = _Cache()
_reaper_lock = Lock()
_reaper: Thread | None = None


def _start_reaper() -> None:
    global _reaper
    with _reaper_lock:
        if _reaper is None:

            def sweep() -> None:
                from time import sleep

                while True:
                    sleep(30)
                    _dataset_cache.expire()
                    _session_cache.expire()

            _reaper = Thread(target=sweep, name="daft-lance-cache-cleanup", daemon=True)
            _reaper.start()


def clear_caches() -> None:
    """Release retained references without closing datasets used by active tasks."""
    _dataset_cache.clear()
    _session_cache.clear()


def _reset_after_fork() -> None:
    global _session_cache, _dataset_cache, _reaper_lock, _reaper
    _session_cache = _Cache()
    _dataset_cache = _Cache()
    _reaper_lock = Lock()
    _reaper = None


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_after_fork)


class CacheConfigurationError(ValueError):
    """Invalid runtime configuration; never a missing-table signal."""


def _env_integer(name: str, default: int | None = None, *, minimum: int = 0) -> int | None:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = int(raw)
        if value < minimum:
            raise ValueError
    except ValueError:
        raise CacheConfigurationError(f"{name}={raw!r}: expected an integer >= {minimum}") from None
    return value


def _retention() -> tuple[int, int]:
    capacity = _env_integer("DAFT_LANCE_DATASET_CACHE_SIZE", 4, minimum=1)
    ttl = _env_integer("DAFT_LANCE_CACHE_TTL_SECONDS", 300, minimum=1)
    assert capacity is not None and ttl is not None
    return capacity, ttl


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
        index_bytes = _env_integer("DAFT_LANCE_INDEX_CACHE_SIZE_BYTES")
    if metadata_bytes is None:
        metadata_bytes = _env_integer("DAFT_LANCE_METADATA_CACHE_SIZE_BYTES")
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
    _, ttl = _retention()
    return _session_cache.get(
        key,
        lambda: (Session(index_cache_size_bytes=index_bytes, metadata_cache_size_bytes=metadata_bytes), client),
        16,
        ttl,
    )[0]


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
    capacity, ttl = _retention()
    index_bytes = options.index_cache_size_bytes
    if index_bytes is None and options.session is None:
        index_bytes = (
            options.index_cache_size * 20 * 1024 * 1024
            if options.index_cache_size is not None
            else _env_integer("DAFT_LANCE_INDEX_CACHE_SIZE_BYTES")
        )
    metadata_bytes = options.metadata_cache_size_bytes
    if metadata_bytes is None and options.session is None:
        metadata_bytes = _env_integer("DAFT_LANCE_METADATA_CACHE_SIZE_BYTES")
    # Automatic Session pool eviction must not invalidate a retained Dataset.
    # Explicit sessions remain isolated by identity and are retained in the value.
    key += f"/{index_bytes}/{metadata_bytes}/{id(options.session) if options.session is not None else 'managed'}"

    def load() -> tuple[lance.LanceDataset, Session]:
        session = options.resolve_session(uri)
        return open_dataset(uri, replace(options, session=session)), session

    return _dataset_cache.get(key, load, capacity, ttl)[0]
