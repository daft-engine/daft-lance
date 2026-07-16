from __future__ import annotations

import os
import sys

import pytest

# Try to import lance; if it fails, all tests in this directory will be skipped.
lance = pytest.importorskip("lance")

_NATIVE_TEARDOWN_CRASH_MARKER = "lance_native_teardown_crash_workaround"
_exitstatus: int | None = None
_hard_exit_after_session = False


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        f"{_NATIVE_TEARDOWN_CRASH_MARKER}: hard-exit after reported results to avoid a known Lance native teardown crash",
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    global _hard_exit_after_session
    _hard_exit_after_session = any(item.get_closest_marker(_NATIVE_TEARDOWN_CRASH_MARKER) for item in items)


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    global _exitstatus
    _exitstatus = exitstatus


@pytest.hookimpl(trylast=True)
def pytest_unconfigure(config: pytest.Config) -> None:
    if not _hard_exit_after_session or _exitstatus is None:
        return
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(_exitstatus)
