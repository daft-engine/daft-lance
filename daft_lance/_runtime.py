from __future__ import annotations

import os
from pathlib import Path
from threading import Lock
from typing import Any

import lance

# pylance exposes this public alias at runtime but omits its stub export.
from lance import Session as Session  # type: ignore[attr-defined]

DatasetUri = str | Path | None
_session: Session | None = None
_session_lock = Lock()


def _env_integer(name: str) -> int | None:
    raw = os.environ.get(name)
    if raw is None:
        return None
    try:
        value = int(raw)
    except ValueError:
        raise ValueError(f"{name}={raw!r}: expected a nonnegative integer") from None
    if value < 0:
        raise ValueError(f"{name}={raw!r}: expected a nonnegative integer")
    return value


def _reset_session() -> None:
    """Release the global reference; active Dataset handles retain their Session."""
    global _session
    with _session_lock:
        released, _session = _session, None
    del released


def _reset_after_fork() -> None:
    global _session, _session_lock
    _session = None
    _session_lock = Lock()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_after_fork)


def _sharing_enabled() -> bool:
    return os.environ.get("DAFT_LANCE_SHARE_SESSION", "1").strip().lower() not in {"0", "false", "no"}


def shared_session() -> Session:
    """Return the process Session, or a fresh Lance-default Session when disabled."""
    global _session
    if not _sharing_enabled():
        return Session()
    with _session_lock:
        if _session is None:
            _session = Session(
                index_cache_size_bytes=_env_integer("DAFT_LANCE_INDEX_CACHE_SIZE_BYTES"),
                metadata_cache_size_bytes=_env_integer("DAFT_LANCE_METADATA_CACHE_SIZE_BYTES"),
            )
        return _session


def session_for_open(
    *,
    session: Session | None = None,
    index_cache_size: int | None = None,
    index_cache_size_bytes: int | None = None,
    metadata_cache_size_bytes: int | None = None,
) -> Session | None:
    """Leave explicitly configured opens to Lance; share only default opens."""
    if session is not None:
        return session
    if not _sharing_enabled() or any(
        size is not None for size in (index_cache_size, index_cache_size_bytes, metadata_cache_size_bytes)
    ):
        return None
    return shared_session()


def open_dataset(uri: DatasetUri, **kwargs: Any) -> lance.LanceDataset:
    """Forward Lance options, injecting a Session only for default opens."""
    selected = session_for_open(
        session=kwargs.get("session"),
        index_cache_size=kwargs.get("index_cache_size"),
        index_cache_size_bytes=kwargs.get("index_cache_size_bytes"),
        metadata_cache_size_bytes=kwargs.get("metadata_cache_size_bytes"),
    )
    if selected is not None:
        kwargs["session"] = selected
    return lance.dataset(uri, **kwargs)
