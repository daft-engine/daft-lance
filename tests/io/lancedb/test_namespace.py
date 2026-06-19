from __future__ import annotations

import pytest

import daft
import daft_lance


def test_namespace_write_read_append_roundtrip(tmp_path):
    namespace_properties = {"root": str(tmp_path)}
    table_id = ["roundtrip"]

    df1 = daft.from_pydict({"id": [1, 2], "label": ["a", "b"]})
    df2 = daft.from_pydict({"id": [3], "label": ["c"]})

    df1.write_lance(
        namespace_impl="dir",
        namespace_properties=namespace_properties,
        table_id=table_id,
        mode="create",
    ).collect()
    df2.write_lance(
        namespace_impl="dir",
        namespace_properties=namespace_properties,
        table_id=table_id,
        mode="append",
    ).collect()

    result = daft.read_lance(
        namespace_impl="dir",
        namespace_properties=namespace_properties,
        table_id=table_id,
    ).to_pydict()

    assert result == {"id": [1, 2, 3], "label": ["a", "b", "c"]}


def test_namespace_read_supports_pushdowns(tmp_path):
    namespace_properties = {"root": str(tmp_path)}
    table_id = ["pushdowns"]

    daft.from_pydict(
        {
            "id": [1, 2, 3],
            "label": ["a", "b", "c"],
            "score": [10, 20, 30],
        }
    ).write_lance(
        namespace_impl="dir",
        namespace_properties=namespace_properties,
        table_id=table_id,
        mode="create",
    ).collect()

    result = (
        daft.read_lance(
            namespace_impl="dir",
            namespace_properties=namespace_properties,
            table_id=table_id,
        )
        .where(daft.col("score") > 10)
        .select("label")
        .to_pydict()
    )

    assert result == {"label": ["b", "c"]}


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
