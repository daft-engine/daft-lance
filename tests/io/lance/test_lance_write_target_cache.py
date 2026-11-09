from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

import lance
import pytest

import daft
from daft_lance import lance_data_sink

if TYPE_CHECKING:
    import pathlib
    from collections.abc import Iterator

pytestmark = pytest.mark.skipif(
    os.environ.get("DAFT_RUNNER") == "ray",
    reason="the spies patch the driver process, while Ray writes from worker processes",
)


@pytest.fixture(autouse=True)
def clear_write_targets() -> Iterator[None]:
    lance_data_sink._WRITE_TARGETS.clear()
    yield
    lance_data_sink._WRITE_TARGETS.clear()


@pytest.fixture
def write_targets(monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    """Record the destination of every lance.fragment.write_fragments call."""
    targets: list[Any] = []
    original = lance.fragment.write_fragments

    def spy(data: Any, dataset_uri: Any, *args: Any, **kwargs: Any) -> Any:
        targets.append(dataset_uri)
        return original(data, dataset_uri, *args, **kwargs)

    monkeypatch.setattr(lance.fragment, "write_fragments", spy)
    return targets


@pytest.fixture
def pinned_opens(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """Count lance.dataset calls that pin a version, i.e. write-target opens."""
    opens: list[int] = []
    original = lance.dataset

    def spy(*args: Any, **kwargs: Any) -> Any:
        if kwargs.get("version") is not None:
            opens.append(kwargs["version"])
        return original(*args, **kwargs)

    monkeypatch.setattr(lance, "dataset", spy)
    return opens


def _ids(path: str) -> list[int]:
    return sorted(lance.dataset(path).to_table(columns=["id"]).to_pydict()["id"])


def test_append_reuses_one_pinned_dataset_across_writes(
    tmp_path: pathlib.Path, write_targets: list[Any], pinned_opens: list[int]
) -> None:
    path = str(tmp_path / "t.lance")
    daft.from_pydict({"id": [0, 1]}).write_lance(path, mode="create")
    version_before = lance.dataset(path).version
    write_targets.clear()

    df = daft.from_pydict({"id": list(range(2, 12))}).into_batches(2)
    df.write_lance(path, mode="append", max_rows_per_file=2)

    assert len(write_targets) >= 2
    assert all(isinstance(t, lance.LanceDataset) for t in write_targets)
    assert {t.version for t in write_targets} == {version_before}
    assert pinned_opens == [version_before]
    assert _ids(path) == list(range(12))


def test_sequential_appends_refresh_pinned_version(tmp_path: pathlib.Path, write_targets: list[Any]) -> None:
    path = str(tmp_path / "t.lance")
    daft.from_pydict({"id": [0]}).write_lance(path, mode="create")
    v1 = lance.dataset(path).version

    daft.from_pydict({"id": [1]}).write_lance(path, mode="append")
    v2 = lance.dataset(path).version
    daft.from_pydict({"id": [2]}).write_lance(path, mode="append")

    assert [t.version for t in write_targets[1:]] == [v1, v2]
    assert len(lance_data_sink._WRITE_TARGETS) == 1
    assert _ids(path) == [0, 1, 2]


def test_create_writes_to_uri(tmp_path: pathlib.Path, write_targets: list[Any], pinned_opens: list[int]) -> None:
    path = str(tmp_path / "t.lance")
    daft.from_pydict({"id": list(range(6))}).into_batches(2).write_lance(path, mode="create", max_rows_per_file=2)

    assert write_targets
    assert all(isinstance(t, str) for t in write_targets)
    assert pinned_opens == []
    assert _ids(path) == list(range(6))


def test_overwrite_existing_uses_pinned_dataset(tmp_path: pathlib.Path, write_targets: list[Any]) -> None:
    path = str(tmp_path / "t.lance")
    daft.from_pydict({"id": [0, 1, 2]}).write_lance(path, mode="create")
    version_before = lance.dataset(path).version
    write_targets.clear()

    daft.from_pydict({"id": [7, 8]}).write_lance(path, mode="overwrite")

    assert write_targets
    assert all(isinstance(t, lance.LanceDataset) and t.version == version_before for t in write_targets)
    assert _ids(path) == [7, 8]
