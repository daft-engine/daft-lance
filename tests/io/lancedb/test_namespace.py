from __future__ import annotations

from types import SimpleNamespace

import pytest

import daft
import daft_lance
import daft_lance.namespace as namespace_mod

pytestmark = pytest.mark.lance_native_teardown_crash_workaround


def _dir_ns(tmp_path):
    return {"namespace_impl": "dir", "namespace_properties": {"root": str(tmp_path)}}


def test_namespace_write_read_append_roundtrip(tmp_path):
    ns = _dir_ns(tmp_path)
    table_id = ["roundtrip"]

    df1 = daft.from_pydict({"id": [1, 2], "label": ["a", "b"]})
    df2 = daft.from_pydict({"id": [3], "label": ["c"]})

    daft_lance.write_lance(df1, table_id=table_id, mode="create", **ns).collect()
    daft_lance.write_lance(df2, table_id=table_id, mode="append", **ns).collect()

    result = daft_lance.read_lance(table_id=table_id, **ns).to_pydict()

    assert result == {"id": [1, 2, 3], "label": ["a", "b", "c"]}


def test_namespace_overwrite(tmp_path):
    ns = _dir_ns(tmp_path)
    table_id = ["overwrite_tbl"]

    daft_lance.write_lance(daft.from_pydict({"id": [1, 2]}), table_id=table_id, mode="create", **ns).collect()
    daft_lance.write_lance(daft.from_pydict({"id": [7, 8, 9]}), table_id=table_id, mode="overwrite", **ns).collect()

    result = daft_lance.read_lance(table_id=table_id, **ns).to_pydict()
    assert result == {"id": [7, 8, 9]}


def test_namespace_overwrite_missing_table_declares(tmp_path):
    ns = _dir_ns(tmp_path)
    table_id = ["overwrite_fresh"]

    daft_lance.write_lance(daft.from_pydict({"id": [1]}), table_id=table_id, mode="overwrite", **ns).collect()

    result = daft_lance.read_lance(table_id=table_id, **ns).to_pydict()
    assert result == {"id": [1]}


def test_namespace_overwrite_does_not_declare_on_ambiguous_error(monkeypatch, tmp_path):
    class FakeNamespace:
        declared = False

        def describe_table(self, request):
            raise RuntimeError("permission denied: parent catalog does not exist")

        def declare_table(self, request):
            self.declared = True
            return SimpleNamespace(location=str(tmp_path / "should_not_exist.lance"))

    namespace = FakeNamespace()
    monkeypatch.setattr(namespace_mod, "get_or_create_namespace", lambda *args: namespace)

    with pytest.raises(RuntimeError, match="permission denied"):
        namespace_mod.resolve_namespace_table(
            namespace_impl="rest",
            namespace_properties={"uri": "http://namespace.example"},
            table_id=["catalog", "schema", "table"],
            mode="overwrite",
        )

    assert not namespace.declared


def test_namespace_untyped_404_table_not_found_is_missing():
    class RestTableNotFound(RuntimeError):
        status = 404

    assert namespace_mod.is_table_not_found(RestTableNotFound("table catalog.schema.table not found"))


def test_namespace_read_supports_pushdowns(tmp_path):
    ns = _dir_ns(tmp_path)
    table_id = ["pushdowns"]

    daft_lance.write_lance(
        daft.from_pydict(
            {
                "id": [1, 2, 3],
                "label": ["a", "b", "c"],
                "score": [10, 20, 30],
            }
        ),
        table_id=table_id,
        mode="create",
        **ns,
    ).collect()

    result = daft_lance.read_lance(table_id=table_id, **ns).where(daft.col("score") > 10).select("label").to_pydict()

    assert result == {"label": ["b", "c"]}


def test_namespace_count_pushdown(tmp_path):
    ns = _dir_ns(tmp_path)
    table_id = ["count_tbl"]

    daft_lance.write_lance(daft.from_pydict({"id": list(range(10))}), table_id=table_id, mode="create", **ns).collect()

    assert daft_lance.read_lance(table_id=table_id, **ns).count_rows() == 10


def test_namespace_patch_daft(tmp_path):
    ns = _dir_ns(tmp_path)
    table_id = ["patched"]

    daft_lance.patch_daft()
    daft_lance.patch_daft()  # idempotent

    daft.from_pydict({"id": [1, 2]}).write_lance(table_id=table_id, mode="create", **ns).collect()
    result = daft.read_lance(table_id=table_id, **ns).to_pydict()
    assert result == {"id": [1, 2]}

    # Plain-URI writes keep working through the patched method.
    uri = str(tmp_path / "plain.lance")
    daft.from_pydict({"id": [5]}).write_lance(uri).collect()
    assert daft.read_lance(uri).to_pydict() == {"id": [5]}


def test_namespace_write_lance_merge_new_columns(tmp_path):
    ns = _dir_ns(tmp_path)
    table_id = ["merge_tbl"]

    daft_lance.write_lance(
        daft.from_pydict({"id": [1, 2, 3], "score": [10, 20, 30]}),
        table_id=table_id,
        mode="create",
        **ns,
    ).collect()

    df = daft_lance.read_lance(
        table_id=table_id,
        default_scan_options={"with_row_address": True},
        include_fragment_id=True,
        **ns,
    )
    df = df.with_column("doubled", df["score"] * 2)
    daft_lance.write_lance(df, table_id=table_id, mode="merge", **ns).collect()

    result = daft_lance.read_lance(table_id=table_id, **ns).sort("id").to_pydict()
    assert result["doubled"] == [20, 40, 60]


def test_namespace_merge_columns_df(tmp_path):
    ns = _dir_ns(tmp_path)
    table_id = ["merge_cols_df"]

    daft_lance.write_lance(
        daft.from_pydict({"id": [1, 2, 3], "score": [1, 2, 3]}),
        table_id=table_id,
        mode="create",
        **ns,
    ).collect()

    df = daft_lance.read_lance(
        table_id=table_id,
        default_scan_options={"with_row_address": True},
        include_fragment_id=True,
        **ns,
    )
    df = df.with_column("tripled", df["score"] * 3)
    daft_lance.merge_columns_df(df.select("fragment_id", "_rowaddr", "tripled"), table_id=table_id, **ns)

    result = daft_lance.read_lance(table_id=table_id, **ns).sort("id").to_pydict()
    assert result["tripled"] == [3, 6, 9]


def test_namespace_create_scalar_index(tmp_path):
    ns = _dir_ns(tmp_path)
    table_id = ["indexed"]

    daft_lance.write_lance(
        daft.from_pydict({"id": list(range(100)), "price": [i * 2 for i in range(100)]}),
        table_id=table_id,
        mode="create",
        **ns,
    ).collect()

    daft_lance.create_scalar_index(table_id=table_id, column="price", index_type="BTREE", **ns)

    import lance
    import lance_namespace as ln
    from lance_namespace import DescribeTableRequest

    namespace = ln.connect("dir", {"root": str(tmp_path)})
    location = namespace.describe_table(DescribeTableRequest(id=table_id)).location
    indices = lance.dataset(location).list_indices()
    assert any(idx["fields"] == ["price"] for idx in indices)


def test_namespace_compact_files(tmp_path):
    ns = _dir_ns(tmp_path)
    table_id = ["compacted"]

    daft_lance.write_lance(daft.from_pydict({"id": [1]}), table_id=table_id, mode="create", **ns).collect()
    for i in range(3):
        daft_lance.write_lance(daft.from_pydict({"id": [i + 2]}), table_id=table_id, mode="append", **ns).collect()

    daft_lance.compact_files(table_id=table_id, compaction_options={"target_rows_per_fragment": 1024}, **ns)

    result = daft_lance.read_lance(table_id=table_id, **ns).sort("id").to_pydict()
    assert result == {"id": [1, 2, 3, 4]}


def test_namespace_rejects_uri_and_namespace(tmp_path):
    with pytest.raises(ValueError, match="Cannot provide both 'uri' and namespace parameters"):
        daft_lance.read_lance(
            str(tmp_path / "dataset"),
            namespace_impl="dir",
            namespace_properties={"root": str(tmp_path)},
            table_id=["tbl"],
        )


def test_namespace_requires_table_id(tmp_path):
    with pytest.raises(ValueError, match="'table_id' must be provided"):
        daft_lance.read_lance(
            namespace_impl="dir",
            namespace_properties={"root": str(tmp_path)},
        )


def test_namespace_requires_impl():
    with pytest.raises(ValueError, match="'namespace_impl' must be provided"):
        daft_lance.read_lance(table_id=["tbl"])


def test_namespace_requires_uri_or_namespace():
    with pytest.raises(ValueError, match="Must provide either 'uri' OR"):
        daft_lance.read_lance()
