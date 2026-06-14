from __future__ import annotations

from pathlib import Path

import lance
import pyarrow as pa

from daft_lance import cleanup_old_versions


def test_cleanup_old_versions_retains_latest_version(tmp_path: Path) -> None:
    dataset_path = tmp_path / "cleanup_dataset"
    lance.write_dataset(pa.table({"id": [1]}), dataset_path)
    lance.write_dataset(pa.table({"id": [2]}), dataset_path, mode="overwrite")

    assert [version["version"] for version in lance.dataset(str(dataset_path)).versions()] == [1, 2]

    stats = cleanup_old_versions(
        str(dataset_path),
        retain_versions=1,
        delete_unverified=True,
    )

    assert stats.old_versions == 1
    assert stats.data_files_removed == 1
    ds = lance.dataset(str(dataset_path))
    assert [version["version"] for version in ds.versions()] == [2]
    assert ds.to_table().to_pydict() == {"id": [2]}
