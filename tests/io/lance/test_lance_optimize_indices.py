from __future__ import annotations

import warnings
from pathlib import Path
from typing import Any

import lance
import pytest

import daft
import daft_lance
from daft.dependencies import pa
from daft_lance import create_scalar_index, optimize_indices

warnings.filterwarnings("ignore", category=DeprecationWarning, module="lance")


def _make_dataset(path: Path, n_rows: int = 80, rows_per_file: int = 20) -> str:
    table = pa.table(
        {
            "id": list(range(n_rows)),
            "name": [f"name-{i % 8}" for i in range(n_rows)],
        }
    )
    lance.write_dataset(table, str(path), mode="create", max_rows_per_file=rows_per_file)
    return str(path)


def _fragment_ids(uri: str) -> list[int]:
    return sorted(f.fragment_id for f in lance.dataset(uri).get_fragments())


def _covered_fragments(uri: str, index_name: str) -> set[int]:
    for desc in lance.dataset(uri).describe_indices():
        if desc.name == index_name:
            covered: set[int] = set()
            for segment in desc.segments or []:
                covered.update(segment.fragment_ids or [])
            return covered
    raise AssertionError(f"index {index_name} not found")


def _segment_count(uri: str, index_name: str) -> int:
    for desc in lance.dataset(uri).describe_indices():
        if desc.name == index_name:
            return len(desc.segments or [])
    raise AssertionError(f"index {index_name} not found")


def _sorted_rows(uri: str, predicate: str) -> dict[str, list[Any]]:
    table = lance.dataset(uri).scanner(filter=predicate).to_table().sort_by("id")
    return table.to_pydict()


PREDICATES = ["name = 'name-3'", "name in ('name-0', 'name-7')", "id >= 40", "id = 55", "name = 'nope'"]


def test_optimize_covers_new_fragments_and_keeps_query_correct(tmp_path: Path) -> None:
    """Appended fragments are indexed in one transaction; results stay identical."""
    uri = _make_dataset(tmp_path / "incremental.lance")
    reference = _make_dataset(tmp_path / "reference.lance")

    create_scalar_index(uri, column="name", index_type="INVERTED")
    extra = pa.table({"id": list(range(100, 140)), "name": [f"name-{100 + i}" for i in range(40)]})
    lance.write_dataset(extra, uri, mode="append", max_rows_per_file=20)
    lance.write_dataset(extra, reference, mode="append", max_rows_per_file=20)

    assert _covered_fragments(uri, "name_idx") < set(_fragment_ids(uri))
    version_before = lance.dataset(uri).version

    updated = optimize_indices(uri)

    assert isinstance(updated, lance.LanceDataset)
    assert updated.version == version_before + 1
    assert _covered_fragments(uri, "name_idx") == set(_fragment_ids(uri))

    for predicate in PREDICATES:
        assert _sorted_rows(uri, predicate) == _sorted_rows(reference, predicate), predicate


def test_optimize_merges_small_segments(tmp_path: Path) -> None:
    """num_indices_to_merge compacts fragmented segment coverage."""
    uri = _make_dataset(tmp_path / "merge.lance")

    create_scalar_index(uri, column="name", index_type="INVERTED", fragment_group_size=1)
    assert _segment_count(uri, "name_idx") == 4

    optimize_indices(uri, indices=["name_idx"], num_indices_to_merge=4)

    assert _segment_count(uri, "name_idx") == 1
    assert lance.dataset(uri).scanner(filter="name = 'name-3'").to_table().num_rows == 10


def test_optimize_noop_commits_no_version(tmp_path: Path) -> None:
    """A healthy index and an index-free dataset both optimize to a no-op."""
    uri = _make_dataset(tmp_path / "noop.lance")
    create_scalar_index(uri, column="name", index_type="INVERTED")
    version_before = lance.dataset(uri).version

    updated = optimize_indices(uri)

    assert updated.version == version_before

    plain = _make_dataset(tmp_path / "plain.lance")
    assert optimize_indices(plain).version == lance.dataset(plain).version
    assert lance.dataset(plain).describe_indices() == []


def test_optimize_heals_stale_fragment_ids_after_delete(tmp_path: Path) -> None:
    """A fully deleted fragment inside a mixed segment is dropped from coverage.

    Deletes retire fully-dead segments but cannot remove one dead fragment
    from a segment that also covers live ones; the optimizer heals it — but
    only as part of a commit that indexes or merges new data, so this test
    appends first (test_stale_coverage_without_new_data_is_not_healed pins
    the other side).
    """
    uri = str(tmp_path / "heal.lance")
    lance.write_dataset(
        pa.table({"id": list(range(40)), "name": [f"row {i}" for i in range(40)]}),
        uri,
        mode="create",
        max_rows_per_file=10,  # fragments 0..3
    )
    create_scalar_index(uri, column="name", index_type="INVERTED", name="s_idx", fragment_group_size=4)

    rows_of_fragment_0 = [f"row {i}" for i in range(10)]
    in_list = ", ".join(f"'{r}'" for r in rows_of_fragment_0)
    lance.dataset(uri).delete(f"name in ({in_list})")
    assert 0 not in [f.fragment_id for f in lance.dataset(uri).get_fragments()]
    assert 0 in _covered_fragments(uri, "s_idx")  # stale id still in coverage

    lance.write_dataset(
        pa.table({"id": list(range(100, 110)), "name": [f"row {100 + i}" for i in range(10)]}),
        uri,
        mode="append",
        max_rows_per_file=10,
    )

    optimize_indices(uri, indices=["s_idx"])

    covered = _covered_fragments(uri, "s_idx")
    assert 0 not in covered
    assert covered == set(_fragment_ids(uri))
    ds = lance.dataset(uri)
    assert ds.count_rows() == 40
    assert ds.scanner(filter="name = 'row 25'").to_table().num_rows == 1


def test_stale_coverage_without_new_data_is_not_healed(tmp_path: Path) -> None:
    """Healing rides along with commits that index or merge new data.

    With nothing new to index, optimize commits nothing and leaves the
    stale-only coverage as-is (documented behavior: use a replace=True
    rebuild to clean it up deterministically).
    """
    uri = str(tmp_path / "stale.lance")
    lance.write_dataset(
        pa.table({"id": list(range(40)), "name": [f"row {i}" for i in range(40)]}),
        uri,
        mode="create",
        max_rows_per_file=10,
    )
    create_scalar_index(uri, column="name", index_type="INVERTED", name="s_idx", fragment_group_size=4)
    rows_of_fragment_0 = [f"row {i}" for i in range(10)]
    in_list = ", ".join(f"'{r}'" for r in rows_of_fragment_0)
    lance.dataset(uri).delete(f"name in ({in_list})")
    version_before = lance.dataset(uri).version
    assert 0 in _covered_fragments(uri, "s_idx")

    updated = optimize_indices(uri)

    assert updated.version == version_before
    assert 0 in _covered_fragments(uri, "s_idx")


def test_optimize_after_delete_all_is_noop(tmp_path: Path) -> None:
    """After deleting every row the index and its stale ids stay as-is."""
    uri = str(tmp_path / "gone.lance")
    lance.write_dataset(pa.table({"name": [f"row {i}" for i in range(20)]}), uri, mode="create", max_rows_per_file=10)
    create_scalar_index(uri, column="name", index_type="INVERTED", name="s_idx")
    lance.dataset(uri).delete("name != ''")
    assert lance.dataset(uri).count_rows() == 0
    version_before = lance.dataset(uri).version

    updated = optimize_indices(uri)

    assert updated.version == version_before
    assert [d.name for d in lance.dataset(uri).describe_indices()] == ["s_idx"]


def test_indices_filter_targets_one_index(tmp_path: Path) -> None:
    """The name filter optimizes only the named index."""
    uri = _make_dataset(tmp_path / "filter.lance")
    create_scalar_index(uri, column="name", index_type="INVERTED", name="name_idx")
    create_scalar_index(uri, column="id", index_type="BTREE", name="id_idx")
    extra = pa.table({"id": list(range(100, 120)), "name": [f"name-{100 + i}" for i in range(20)]})
    lance.write_dataset(extra, uri, mode="append", max_rows_per_file=20)

    optimize_indices(uri, indices=["id_idx"])

    assert _covered_fragments(uri, "id_idx") == set(_fragment_ids(uri))
    assert _covered_fragments(uri, "name_idx") < set(_fragment_ids(uri))  # untouched
    assert lance.dataset(uri).scanner(filter="id = 105").to_table().num_rows == 1


def test_unknown_index_names_raise(tmp_path: Path) -> None:
    uri = _make_dataset(tmp_path / "unknown.lance")
    create_scalar_index(uri, column="name", index_type="INVERTED")
    version_before = lance.dataset(uri).version

    with pytest.raises(ValueError, match=r"\['ghost_idx'\].*Available index names: \['name_idx'\]"):
        optimize_indices(uri, indices=["ghost_idx"])

    # Rejected before any work: the index and version are untouched.
    assert lance.dataset(uri).version == version_before
    assert _covered_fragments(uri, "name_idx") == set(_fragment_ids(uri))


def test_empty_indices_raise(tmp_path: Path) -> None:
    uri = _make_dataset(tmp_path / "empty.lance")
    with pytest.raises(ValueError, match="non-empty"):
        optimize_indices(uri, indices=[])


def test_optimize_via_namespace_entry(tmp_path: Path) -> None:
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
    create_scalar_index(table_id=table_id, column="name", index_type="INVERTED", **ns)
    daft_lance.write_lance(
        daft.from_pydict({"id": list(range(100, 120)), "name": [f"name-{100 + i}" for i in range(20)]}),
        table_id=table_id,
        mode="append",
        **ns,
    ).collect()

    updated = daft_lance.optimize_indices(table_id=table_id, **ns)

    assert isinstance(updated, lance.LanceDataset)
    assert _covered_fragments(uri, "name_idx") == set(_fragment_ids(uri))
