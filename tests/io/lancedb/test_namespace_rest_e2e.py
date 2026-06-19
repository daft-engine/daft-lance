from __future__ import annotations

import os
import uuid

import pytest

import daft
import daft_lance  # noqa: F401 - patches daft.read_lance and DataFrame.write_lance.

pytestmark = pytest.mark.skipif(
    os.environ.get("DAFT_LANCE_REST_URI") is None,
    reason="Set DAFT_LANCE_REST_URI to run the Lance REST namespace integration test.",
)


def test_rest_namespace_write_read_append_roundtrip() -> None:
    import lance
    import lance_namespace as ln
    from lance_namespace import CreateNamespaceRequest, DescribeTableRequest, NamespaceExistsRequest

    namespace_properties = {"uri": os.environ["DAFT_LANCE_REST_URI"]}
    catalog = os.environ.get("DAFT_LANCE_REST_CATALOG", "lance_catalog")
    schema = os.environ.get("DAFT_LANCE_REST_SCHEMA", "daft_ns_e2e")
    table_id = [catalog, schema, f"orders_{uuid.uuid4().hex[:8]}"]

    namespace = ln.connect("rest", namespace_properties)
    try:
        namespace.namespace_exists(NamespaceExistsRequest(id=[catalog, schema]))
    except Exception:
        namespace.create_namespace(CreateNamespaceRequest(id=[catalog, schema], mode="CREATE"))

    daft.from_pydict(
        {
            "id": [1, 2, 3],
            "label": ["a", "b", "c"],
            "score": [10, 20, 30],
        }
    ).write_lance(
        namespace_impl="rest",
        namespace_properties=namespace_properties,
        table_id=table_id,
        mode="create",
    ).collect()

    daft.from_pydict(
        {
            "id": [4, 5],
            "label": ["d", "e"],
            "score": [40, 50],
        }
    ).write_lance(
        namespace_impl="rest",
        namespace_properties=namespace_properties,
        table_id=table_id,
        mode="append",
    ).collect()

    describe = namespace.describe_table(DescribeTableRequest(id=table_id))
    location = getattr(describe, "location", None) or getattr(describe, "table_uri", None)
    assert location

    result = daft.read_lance(
        namespace_impl="rest",
        namespace_properties=namespace_properties,
        table_id=table_id,
    ).to_pydict()
    assert result == {
        "id": [1, 2, 3, 4, 5],
        "label": ["a", "b", "c", "d", "e"],
        "score": [10, 20, 30, 40, 50],
    }

    filtered = (
        daft.read_lance(
            namespace_impl="rest",
            namespace_properties=namespace_properties,
            table_id=table_id,
        )
        .where(daft.col("score") >= 30)
        .select("id", "label")
        .to_pydict()
    )
    assert filtered == {"id": [3, 4, 5], "label": ["c", "d", "e"]}

    assert (
        daft.read_lance(
            namespace_impl="rest",
            namespace_properties=namespace_properties,
            table_id=table_id,
        ).count_rows()
        == 5
    )
    assert lance.dataset(None, namespace_client=namespace, table_id=table_id).count_rows() == 5
