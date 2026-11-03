from __future__ import annotations

from pathlib import Path
from typing import Any

import lance
import pyarrow as pa
import pytest

import daft
import daft_lance
from daft_lance.lance_fragment_validation import FragmentValidationUDF
from daft_lance.namespace import DatasetOpenContext


def _write_fragments(path: Path, num_fragments: int = 3) -> lance.LanceDataset:
    for fragment in range(num_fragments):
        table = pa.table({"id": list(range(fragment * 10, (fragment + 1) * 10))})
        lance.write_dataset(
            table,
            str(path),
            mode="create" if fragment == 0 else "append",
            max_rows_per_file=10,
            max_rows_per_group=10,
        )
    return lance.dataset(str(path))


def _rows_by_fragment(report: daft.DataFrame) -> dict[int, dict[str, Any]]:
    return {row["fragment_id"]: row for row in report.to_pylist()}


def test_validates_every_fragment_of_a_healthy_dataset(tmp_path: Path) -> None:
    path = tmp_path / "healthy.lance"
    dataset = _write_fragments(path)

    report = daft_lance.validate_fragments(str(path))

    assert report.schema().column_names() == ["version", "fragment_id", "valid", "error"]
    rows = report.to_pylist()
    assert [row["fragment_id"] for row in rows] == [0, 1, 2]
    assert {row["version"] for row in rows} == {dataset.version}
    assert all(row["valid"] for row in rows)
    assert all(row["error"] is None for row in rows)
    # Validation is read-only.
    assert lance.dataset(str(path)).version == dataset.version


def test_validates_only_selected_fragments(tmp_path: Path) -> None:
    path = tmp_path / "subset.lance"
    _write_fragments(path)

    rows = daft_lance.validate_fragments(str(path), fragment_ids=[2, 0, 2]).to_pylist()

    assert sorted(row["fragment_id"] for row in rows) == [0, 2]
    assert all(row["valid"] for row in rows)


def test_unknown_fragment_ids_raise(tmp_path: Path) -> None:
    path = tmp_path / "unknown.lance"
    _write_fragments(path)

    with pytest.raises(ValueError, match="do not exist"):
        daft_lance.validate_fragments(str(path), fragment_ids=[999])


def test_empty_fragment_selection_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "empty.lance"
    _write_fragments(path)

    with pytest.raises(ValueError, match="must not be empty"):
        daft_lance.validate_fragments(str(path), fragment_ids=[])


def test_non_positive_concurrency_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "concurrency.lance"
    _write_fragments(path, num_fragments=1)

    with pytest.raises(ValueError, match="concurrency must be positive"):
        daft_lance.validate_fragments(str(path), concurrency=0)


def test_fragment_with_multiple_column_files_is_valid(tmp_path: Path) -> None:
    path = tmp_path / "multi_file.lance"
    dataset = _write_fragments(path, num_fragments=1)
    dataset.add_columns({"doubled": "id * 2"})

    fragment = lance.dataset(str(path)).get_fragment(0)
    assert fragment is not None
    assert len(fragment.metadata.files) == 2

    rows = daft_lance.validate_fragments(str(path)).to_pylist()
    assert [row["valid"] for row in rows] == [True]


def test_fragment_with_deletion_vector_is_valid(tmp_path: Path) -> None:
    path = tmp_path / "deletions.lance"
    dataset = _write_fragments(path)
    dataset.delete("id < 5")

    assert [fragment.fragment_id for fragment in lance.dataset(str(path)).get_fragments()] == [0, 1, 2]
    rows = daft_lance.validate_fragments(str(path)).to_pylist()

    assert len(rows) == 3
    assert all(row["valid"] for row in rows)


def test_fully_deleted_fragment_is_not_reported_as_corrupt(tmp_path: Path) -> None:
    path = tmp_path / "fully_deleted.lance"
    dataset = _write_fragments(path)
    # Deleting every row of a fragment removes it from the manifest, so the
    # report follows the manifest rather than failing on the absent fragment.
    dataset.delete("id < 10")
    remaining = [fragment.fragment_id for fragment in lance.dataset(str(path)).get_fragments()]
    assert 0 not in remaining

    rows = daft_lance.validate_fragments(str(path)).to_pylist()

    assert sorted(row["fragment_id"] for row in rows) == remaining
    assert all(row["valid"] for row in rows)


def test_missing_data_file_is_attributed_to_its_fragment(tmp_path: Path) -> None:
    path = tmp_path / "missing_file.lance"
    dataset = _write_fragments(path)
    victim = dataset.get_fragment(1)
    assert victim is not None
    (path / "data" / victim.metadata.files[0].path).unlink()

    rows = _rows_by_fragment(daft_lance.validate_fragments(str(path)))

    assert rows[0]["valid"] is True
    assert rows[1]["valid"] is False
    assert rows[1]["error"]
    assert rows[2]["valid"] is True


def test_validation_snapshot_is_pinned_against_concurrent_appends(tmp_path: Path) -> None:
    path = tmp_path / "pinned.lance"
    dataset = _write_fragments(path, num_fragments=2)

    planned = daft_lance.validate_fragments(str(path))
    # Append between planning and execution: the worker must still validate
    # the snapshot the driver planned against.
    lance.write_dataset(pa.table({"id": [100]}), str(path), mode="append")
    assert lance.dataset(str(path)).version > dataset.version

    rows = planned.to_pylist()
    assert {row["version"] for row in rows} == {dataset.version}
    assert sorted(row["fragment_id"] for row in rows) == [0, 1]


def test_explicit_version_validates_that_snapshot(tmp_path: Path) -> None:
    path = tmp_path / "versioned.lance"
    first = _write_fragments(path, num_fragments=1)
    first_version = first.version
    lance.write_dataset(pa.table({"id": [99]}), str(path), mode="append")
    assert lance.dataset(str(path)).version > first_version

    rows = daft_lance.validate_fragments(str(path), version=first_version).to_pylist()

    assert {row["version"] for row in rows} == {first_version}
    assert len(rows) == 1


def test_namespace_addressed_table(tmp_path: Path) -> None:
    namespace: dict[str, Any] = {"namespace_impl": "dir", "namespace_properties": {"root": str(tmp_path)}}
    daft_lance.write_lance(
        daft.from_pydict({"id": [1, 2, 3]}),
        table_id=["ns_table"],
        mode="create",
        **namespace,
    ).collect()

    rows = daft_lance.validate_fragments(table_id=["ns_table"], **namespace).to_pylist()

    assert len(rows) == 1
    assert rows[0]["valid"] is True


def test_worker_open_failure_is_distinguished_from_corruption(tmp_path: Path) -> None:
    context = DatasetOpenContext(uri=str(tmp_path / "missing.lance"), version=7)

    message = FragmentValidationUDF(context, context.version)(0)

    assert message is not None
    assert message.startswith("failed to open the dataset at version 7")
