from __future__ import annotations

import pytest

# Try to import lance; if it fails, all tests in this directory will be skipped.
lance = pytest.importorskip("lance")

import daft_lance  # noqa: F401  -- ensure monkey-patches are applied


def sort_pydict(d):
    return {k: sorted(v) for k, v in d.items()}
