from __future__ import annotations

import warnings
from pathlib import Path

import lance
import numpy as np
import pytest

import daft
import daft_lance
from daft.dependencies import pa
from daft_lance import create_scalar_index, create_vector_index, drop_index

warnings.filterwarnings("ignore", category=DeprecationWarning, module="lance")


def _make_dataset(path: Path, n_rows: int = 40, rows_per_file: int = 20) -> str:
    table = pa.table(
        {
            "id": list(range(n_rows)),
            "name": [f"name-{i % 8}" for i in range(n_rows)],
        }
    )
    lance.write_dataset(table, str(path), mode="create", max_rows_per_file=rows_per_file)
    return str(path)


def _index_names(uri: str) -> list[str]:
    return [desc.name for desc in lance.dataset(uri).describe_indices()]


def test_drop_scalar_index(tmp_path: Path) -> None:
    """Dropping a scalar index removes it and commits exactly one new version."""
    uri = _make_dataset(tmp_path / "scalar.lance")
    create_scalar_index(uri, column="name", index_type="INVERTED", name="name_idx")
    version_before = lance.dataset(uri).version
    assert _index_names(uri) == ["name_idx"]

    updated = drop_index(uri, name="name_idx")

    assert isinstance(updated, lance.LanceDataset)
    assert updated.version == version_before + 1
    assert _index_names(uri) == []
    # Queries fall back to full scans and stay correct after the drop.
    assert lance.dataset(uri).scanner(filter="name = 'name-3'").to_table().num_rows == 5


def test_drop_vector_index(tmp_path: Path) -> None:
    """Dropping works for vector indexes the same as scalar ones."""
    rng = np.random.default_rng(1)
    vectors = rng.standard_normal((240, 8)).astype(np.float32)
    table = pa.table(
        {
            "id": list(range(240)),
            "vector": pa.FixedSizeListArray.from_arrays(vectors.reshape(-1), 8),
        }
    )
    uri = str(tmp_path / "vector.lance")
    lance.write_dataset(table, uri, mode="create", max_rows_per_file=40)
    create_vector_index(uri, column="vector", index_type="IVF_FLAT", num_partitions=4, sample_rate=32)
    assert _index_names(uri) == ["vector_idx"]

    drop_index(uri, name="vector_idx")

    assert _index_names(uri) == []


def test_drop_unknown_index_raises(tmp_path: Path) -> None:
    """An unknown name raises before any work, listing the available indexes."""
    uri = _make_dataset(tmp_path / "unknown.lance")
    create_scalar_index(uri, column="name", index_type="INVERTED", name="name_idx")
    version_before = lance.dataset(uri).version

    with pytest.raises(ValueError, match=r"'ghost_idx' does not exist.*Available index names: \['name_idx'\]"):
        drop_index(uri, name="ghost_idx")

    assert lance.dataset(uri).version == version_before
    assert _index_names(uri) == ["name_idx"]


def test_drop_missing_with_if_missing_is_noop(tmp_path: Path) -> None:
    """if_missing=True on a missing index (or index-free dataset) commits nothing."""
    uri = _make_dataset(tmp_path / "missing.lance")
    create_scalar_index(uri, column="name", index_type="INVERTED")
    version_before = lance.dataset(uri).version

    updated = drop_index(uri, name="ghost_idx", if_missing=True)

    assert updated.version == version_before
    assert _index_names(uri) == ["name_idx"]

    plain = _make_dataset(tmp_path / "plain.lance")
    assert drop_index(plain, name="ghost_idx", if_missing=True).version == lance.dataset(plain).version


def test_drop_existing_with_if_missing_still_drops(tmp_path: Path) -> None:
    """if_missing=True only relaxes the missing case; existing indexes drop."""
    uri = _make_dataset(tmp_path / "existing.lance")
    create_scalar_index(uri, column="name", index_type="INVERTED", name="name_idx")

    drop_index(uri, name="name_idx", if_missing=True)

    assert _index_names(uri) == []


def test_drop_via_namespace_entry(tmp_path: Path) -> None:
    """The namespace entry resolves the same dataset as the URI entry."""
    ns = {"namespace_impl": "dir", "namespace_properties": {"root": str(tmp_path)}}
    table_id = ["tbl"]

    daft_lance.write_lance(
        daft.from_pydict({"id": list(range(40)), "name": [f"name-{i % 8}" for i in range(40)]}),
        table_id=table_id,
        mode="create",
        **ns,
    ).collect()
    uri = str(tmp_path / "tbl.lance")
    create_scalar_index(table_id=table_id, column="name", index_type="INVERTED", name="name_idx", **ns)

    updated = daft_lance.drop_index(table_id=table_id, name="name_idx", **ns)

    assert isinstance(updated, lance.LanceDataset)
    assert _index_names(uri) == []
