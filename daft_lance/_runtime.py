from __future__ import annotations

import os
from pathlib import Path
from threading import Lock, Thread
from time import monotonic
from typing import Any

import lance

# pylance exposes this public alias at runtime but omits its stub export.
from lance import Session as Session  # type: ignore[attr-defined]

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
    if os.environ.get("DAFT_LANCE_SHARE_SESSION", "1").strip().lower() in {"0", "false", "no"}:
        # Preserve the original PR's worker-side A/B baseline for every path.
        if index_bytes is None:
            index_bytes = _env_integer("DAFT_LANCE_INDEX_CACHE_SIZE_BYTES")
        if metadata_bytes is None:
            metadata_bytes = _env_integer("DAFT_LANCE_METADATA_CACHE_SIZE_BYTES")
        return Session(index_cache_size_bytes=index_bytes, metadata_cache_size_bytes=metadata_bytes)
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


def open_dataset(uri: DatasetUri, **kwargs: Any) -> lance.LanceDataset:
    """Open through the process Session, forwarding all other options to Lance."""
    if kwargs.get("session") is None:
        kwargs["session"] = shared_session(
            index_cache_size=kwargs.get("index_cache_size"),
            index_cache_size_bytes=kwargs.get("index_cache_size_bytes"),
            metadata_cache_size_bytes=kwargs.get("metadata_cache_size_bytes"),
        )
    return lance.dataset(uri, **kwargs)
