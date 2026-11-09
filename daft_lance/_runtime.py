from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import Lock, Thread
from time import monotonic
from typing import TYPE_CHECKING, Any

import lance

# pylance exposes this public alias at runtime but omits its stub export.
from lance import Session  # type: ignore[attr-defined]
from lance_namespace import LanceNamespace

if TYPE_CHECKING:
    from lance.commit import CommitLock

DatasetUri = str | Path | None
_session: Session | None = None
_configuration: tuple[int | None, int | None] | None = None
_deadline = 0.0
_session_lock = Lock()
_reaper: Thread | None = None


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


def _expire_session() -> None:
    global _session
    released = None
    with _session_lock:
        if _session is not None and _deadline <= monotonic():
            released, _session = _session, None
    del released


def _sweep() -> None:
    from time import sleep

    while True:
        sleep(30)
        _expire_session()


def clear_caches() -> None:
    """Release the global Session and configuration; active operations retain it."""
    global _session, _configuration
    with _session_lock:
        released, _session = _session, None
        _configuration = None
    del released


def _reset_after_fork() -> None:
    global _session, _configuration, _session_lock, _reaper, _deadline
    _session = None
    _configuration = None
    _session_lock = Lock()
    _reaper = None
    _deadline = 0.0


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_after_fork)


def shared_session(
    *,
    index_cache_size: int | None = None,
    index_cache_size_bytes: int | None = None,
    metadata_cache_size_bytes: int | None = None,
) -> Session:
    """One automatically managed Session per process, shared by all tables.

    The first call establishes process-wide capacities. Later unspecified sizes
    reuse them; conflicting explicit sizes raise instead of being
    silently ignored. Idle expiration keeps the configuration for the next open.
    """
    global _session, _configuration, _deadline, _reaper
    index_bytes = index_cache_size_bytes
    if index_bytes is None and index_cache_size is not None:
        index_bytes = index_cache_size * 20 * 1024 * 1024
    metadata_bytes = metadata_cache_size_bytes
    for name, value in (("index_cache_size_bytes", index_bytes), ("metadata_cache_size_bytes", metadata_bytes)):
        if value is not None and value < 0:
            raise CacheConfigurationError(f"{name}={value!r}: expected a nonnegative integer")
    ttl = _env_integer("DAFT_LANCE_CACHE_TTL_SECONDS", 300, minimum=1)
    assert ttl is not None
    with _session_lock:
        if _configuration is not None:
            for name, requested, configured in zip(
                ("index_cache_size_bytes", "metadata_cache_size_bytes"),
                (index_bytes, metadata_bytes),
                _configuration,
            ):
                if requested is not None and requested != configured:
                    raise CacheConfigurationError(
                        f"{name}={requested}: global Session already configured with {configured!r} "
                        "(None means Lance default); use consistent process-wide cache sizes"
                    )
            index_bytes, metadata_bytes = _configuration
        if _configuration is None:
            if index_bytes is None:
                index_bytes = _env_integer("DAFT_LANCE_INDEX_CACHE_SIZE_BYTES")
            if metadata_bytes is None:
                metadata_bytes = _env_integer("DAFT_LANCE_METADATA_CACHE_SIZE_BYTES")
        if _session is None:
            # Only Session creation is locked; dataset/network operations run outside.
            _session = Session(index_cache_size_bytes=index_bytes, metadata_cache_size_bytes=metadata_bytes)
            _configuration = (index_bytes, metadata_bytes)
        _deadline = monotonic() + ttl
        if _reaper is None:
            _reaper = Thread(target=_sweep, name="daft-lance-cache-cleanup", daemon=True)
            _reaper.start()
        return _session


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

    def resolve_session(self) -> Session:
        if self.session is not None:
            return self.session
        return shared_session(
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
        session=options.resolve_session(),
        namespace_client=options.namespace_client,
        table_id=options.table_id,
        base_store_params=options.base_store_params,
    )
