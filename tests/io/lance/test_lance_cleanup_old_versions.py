from __future__ import annotations

import warnings
from pathlib import Path

import lance
import pytest

import daft
import daft_lance
from daft.dependencies import pa

warnings.filterwarnings("ignore", category=DeprecationWarning, module="lance")


def _make_dataset(path: Path) -> str:
    lance.write_dataset(pa.table({"id": list(range(20))}), str(path), mode="create", max_rows_per_file=10)
    return str(path)


def _data_files(uri: str) -> set[Path]:
    """Absolute paths of the data files referenced by the dataset's current version."""
    files: set[Path] = set()
    for fragment in lance.dataset(uri).get_fragments():
        for data_file in fragment.data_files():
            files.add(Path(uri) / "data" / data_file.path())
    return files


def test_cleanup_removes_unreferenced_files(tmp_path: Path) -> None:
    """Overwrite orphans old data files; cleanup removes them and the old version."""
    uri = _make_dataset(tmp_path / "cleanup.lance")
    old_files = _data_files(uri)
    lance.write_dataset(pa.table({"id": list(range(100, 120))}), uri, mode="overwrite", max_rows_per_file=10)

    stats = daft_lance.cleanup_old_versions(uri, retain_versions=1)

    assert stats.data_files_removed == len(old_files)
    assert stats.bytes_removed > 0
    # The orphaned files are really gone from disk.
    assert all(not f.exists() for f in old_files)
    # The old version is no longer restorable.
    with pytest.raises(Exception, match=""):
        lance.dataset(uri, version=1)
    # Latest data stays correct.
    assert lance.dataset(uri).to_table().column("id").to_pylist() == list(range(100, 120))


def test_cleanup_default_keeps_recent_versions(tmp_path: Path) -> None:
    """Without older_than/retain_versions nothing recent is deleted (two-week default)."""
    uri = _make_dataset(tmp_path / "recent.lance")
    files_before = _data_files(uri)
    lance.write_dataset(pa.table({"id": list(range(100, 120))}), uri, mode="overwrite", max_rows_per_file=10)

    stats = daft_lance.cleanup_old_versions(uri)

    assert stats.data_files_removed == 0
    assert stats.bytes_removed == 0
    # Orphaned files stay on disk within the retention window.
    assert all(f.exists() for f in files_before)
    # The previous version is still restorable within the retention window.
    assert lance.dataset(uri, version=1).to_table().column("id").to_pylist() == list(range(20))


def test_cleanup_via_namespace_entry(tmp_path: Path) -> None:
    """The namespace entry resolves the same dataset as the URI entry."""
    ns = {"namespace_impl": "dir", "namespace_properties": {"root": str(tmp_path)}}
    table_id = ["tbl"]

    daft_lance.write_lance(
        daft.from_pydict({"id": list(range(20))}),
        table_id=table_id,
        mode="create",
        **ns,
    ).collect()
    uri = str(tmp_path / "tbl.lance")
    old_files = _data_files(uri)
    daft_lance.write_lance(
        daft.from_pydict({"id": list(range(100, 120))}),
        table_id=table_id,
        mode="overwrite",
        **ns,
    ).collect()

    stats = daft_lance.cleanup_old_versions(table_id=table_id, retain_versions=1, **ns)

    assert stats.data_files_removed == len(old_files)
    assert all(not f.exists() for f in old_files)
