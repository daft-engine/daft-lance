from __future__ import annotations

import os

import pytest

# Tests should not emit analytics requests. In addition to avoiding external
# network access, this prevents Daft's daemon telemetry threads from racing
# with interpreter shutdown after a test suite creates many runners.
os.environ.setdefault("DO_NOT_TRACK", "1")

# Try to import lance; if it fails, all tests in this directory will be skipped.
lance = pytest.importorskip("lance")
