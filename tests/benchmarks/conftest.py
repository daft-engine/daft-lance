"""Local fixtures for the LanceDataSink benchmark suite.

These helpers are intentionally minimal so they stay out of the measured path of
``pytest-benchmark``. Heavy work (Arrow table construction, Lance dataset
inspection) is exposed as plain callables so tests can run it once outside the
``benchmark(...)`` call.
"""

from __future__ import annotations

import os
import string
from collections.abc import Callable
from pathlib import Path
from typing import Any

import lance
import numpy as np
import pyarrow as pa
import pytest

import daft

# Reserved subdirectories within a Lance dataset that are NOT data files.
_LANCE_META_DIRS = {"_versions", "_transactions", "_indices", "_deletions"}


@pytest.fixture(scope="function")
def tmp_dataset_dir(tmp_path: Path) -> Path:
    """Return a fresh, empty directory per test."""
    d = tmp_path / "ds"
    # Lance expects the directory NOT to pre-exist for create mode in some paths;
    # we just return the path and let the sink create it.
    return d


def _deterministic_strings(n: int, width: int, seed: int) -> pa.Array:
    """Fixed-width ASCII strings, seeded for reproducibility."""
    rng = np.random.default_rng(seed)
    alphabet = np.array(list(string.ascii_lowercase + string.digits), dtype="U1")
    # Sample width chars per row; join via .view trick for speed.
    chars = rng.choice(alphabet, size=(n, width))
    arr = np.array(["".join(row) for row in chars], dtype=object)
    return pa.array(arr, type=pa.string())


def _build_random_table(rows: int, cols: dict[str, str], seed: int = 0xDA47) -> pa.Table:
    rng = np.random.default_rng(seed)
    arrays: list[pa.Array] = []
    names: list[str] = []
    for name, dtype in cols.items():
        if dtype == "int64":
            arrays.append(pa.array(rng.integers(-(2**31), 2**31, size=rows, dtype=np.int64)))
        elif dtype == "float64":
            arrays.append(pa.array(rng.standard_normal(rows, dtype=np.float64)))
        elif dtype == "string":
            # Use 16-char fixed-width strings to keep the benchmark fast; this is
            # documented in 02_benchmarks.md.
            arrays.append(_deterministic_strings(rows, width=16, seed=seed ^ hash(name) & 0xFFFFFFFF))
        else:
            raise ValueError(f"unsupported column dtype: {dtype}")
        names.append(name)
    return pa.table(dict(zip(names, arrays)))


@pytest.fixture(scope="session")
def random_table() -> Callable[[int, dict[str, str]], pa.Table]:
    """Factory: build a deterministic Arrow table with the requested columns."""
    return _build_random_table


def _build_blob_table(rows: int, blob_size: int, seed: int = 0xB10B) -> pa.Table:
    """Build a table with blob payloads.

    2 columns: ``id: int64``, ``payload: binary``. Each payload is exactly
    ``blob_size`` bytes, deterministically derived from the row id.
    """
    ids = pa.array(np.arange(rows, dtype=np.int64))
    # Use a single buffer and slice it: each row gets the same bytes but with the
    # row index hashed into the first 8 bytes for determinism + uniqueness.
    rng = np.random.default_rng(seed)
    template = rng.bytes(blob_size)
    payloads: list[bytes] = []
    for i in range(rows):
        prefix = int(i).to_bytes(8, "little", signed=False)
        if blob_size >= 8:
            payloads.append(prefix + template[8:])
        else:
            payloads.append(prefix[:blob_size])
    return pa.table({"id": ids, "payload": pa.array(payloads, type=pa.binary())})


@pytest.fixture(scope="session")
def blob_table() -> Callable[[int, int], pa.Table]:
    """Factory: build a deterministic ``id, payload`` blob table."""
    return _build_blob_table


@pytest.fixture
def set_morsel_size(request: pytest.FixtureRequest) -> Callable[[int], None]:
    """Return a setter that applies default_morsel_size and restores previous value.

    Return a setter that applies ``default_morsel_size`` and restores the
    previous value at teardown.

    ``daft.set_execution_config`` mutates a process-global. We snapshot the
    current value and restore it via ``request.addfinalizer``.
    """
    prev = daft.context.get_context().daft_execution_config.default_morsel_size

    def restore() -> None:
        daft.set_execution_config(default_morsel_size=prev)

    request.addfinalizer(restore)

    def _set(n: int) -> None:
        daft.set_execution_config(default_morsel_size=n)

    return _set


def count_artifacts(uri: str | os.PathLike[str]) -> dict[str, Any]:
    """Inspect a Lance dataset directory and return artifact counts/sizes.

    Returns a dict with keys: ``n_fragments``, ``n_lance_files``, ``n_blob_files``,
    ``on_disk_bytes``. Internal metadata dirs (``_versions``, ``_transactions``,
    ``_indices``, ``_deletions``) are excluded from byte and file counts.
    """
    p = Path(uri)
    if not p.exists():
        return {"n_fragments": 0, "n_lance_files": 0, "n_blob_files": 0, "on_disk_bytes": 0}

    try:
        ds = lance.dataset(str(p))
        n_fragments = len(ds.get_fragments())
    except Exception:
        n_fragments = 0

    n_lance_files = 0
    n_blob_files = 0
    on_disk_bytes = 0
    for f in p.rglob("*"):
        if not f.is_file():
            continue
        # Skip Lance internal metadata dirs.
        parts = set(f.relative_to(p).parts)
        if parts & _LANCE_META_DIRS:
            continue
        try:
            on_disk_bytes += f.stat().st_size
        except OSError:
            pass
        suffix = f.suffix
        if suffix == ".lance":
            n_lance_files += 1
        elif suffix == ".blob":
            n_blob_files += 1

    return {
        "n_fragments": n_fragments,
        "n_lance_files": n_lance_files,
        "n_blob_files": n_blob_files,
        "on_disk_bytes": on_disk_bytes,
    }
