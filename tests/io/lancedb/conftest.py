from __future__ import annotations

import os
import sys

import pytest

# Try to import lance; if it fails, all tests in this directory will be skipped.
lance = pytest.importorskip("lance")


_exitstatus = 0


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    global _exitstatus
    _exitstatus = exitstatus


@pytest.hookimpl(trylast=True)
def pytest_unconfigure(config: pytest.Config) -> None:
    """Hard-exit after reporting to dodge an upstream teardown crash.

    Running enough ``daft`` + ``lance`` write/read cycles in one process makes the
    interpreter SIGSEGV during native finalization (in lance's runtime, inside
    OpenSSL cert teardown) *after* every test has already run and been reported.
    This reproduces with plain ``daft.DataFrame.write_lance`` and is unrelated to
    test outcomes. Runs last, once tracebacks and the summary are already flushed,
    so it never hides a failure — it only skips the crashing native finalization.
    """
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(_exitstatus)
