from __future__ import annotations

import os
import uuid
from typing import Any

import pytest

import daft
import daft_lance

pytestmark = pytest.mark.skipif(
    os.environ.get("DAFT_LANCE_REST_URI") is None,
    reason="Set DAFT_LANCE_REST_URI to run the Lance REST namespace integration test.",
)


def test_rest_namespace_write_read_append_roundtrip() -> None:
    import lance
    import lance_namespace as ln
    from lance_namespace import CreateNamespaceRequest, DescribeTableRequest, NamespaceExistsRequest
    from lance_namespace.errors import TableAlreadyExistsError

    namespace_properties = {"uri": os.environ["DAFT_LANCE_REST_URI"]}
    catalog = os.environ.get("DAFT_LANCE_REST_CATALOG", "lance_catalog")
    schema = os.environ.get("DAFT_LANCE_REST_SCHEMA", "daft_ns_e2e")
    table_id = [catalog, schema, f"orders_{uuid.uuid4().hex[:8]}"]
    ns: dict[str, Any] = {"namespace_impl": "rest", "namespace_properties": namespace_properties}

    namespace = ln.connect("rest", namespace_properties)
    try:
        namespace.namespace_exists(NamespaceExistsRequest(id=[catalog, schema]))
    except Exception:
        namespace.create_namespace(CreateNamespaceRequest(id=[catalog, schema], mode="CREATE"))

    daft_lance.write_lance(
        daft.from_pydict({"id": [1, 2, 3], "label": ["a", "b", "c"], "score": [10, 20, 30]}),
        table_id=table_id,
        mode="create",
        **ns,
    ).collect()

    daft_lance.write_lance(
        daft.from_pydict({"id": [4, 5], "label": ["d", "e"], "score": [40, 50]}),
        table_id=table_id,
        mode="append",
        **ns,
    ).collect()

    describe = namespace.describe_table(DescribeTableRequest(id=table_id))
    location = getattr(describe, "location", None) or getattr(describe, "table_uri", None)
    assert location

    result = daft_lance.read_lance(table_id=table_id, **ns).to_pydict()
    assert result == {
        "id": [1, 2, 3, 4, 5],
        "label": ["a", "b", "c", "d", "e"],
        "score": [10, 20, 30, 40, 50],
    }

    predicate = daft.col("score") >= 30  # type: ignore[operator]
    filtered = daft_lance.read_lance(table_id=table_id, **ns).where(predicate).select("id", "label").to_pydict()
    assert filtered == {"id": [3, 4, 5], "label": ["c", "d", "e"]}

    assert daft_lance.read_lance(table_id=table_id, **ns).count_rows() == 5
    assert lance.dataset(None, namespace_client=namespace, table_id=table_id).count_rows() == 5

    with pytest.raises(TableAlreadyExistsError):
        daft_lance.write_lance(
            daft.from_pydict({"id": [99], "label": ["duplicate"], "score": [990]}),
            table_id=table_id,
            mode="create",
            **ns,
        ).collect()
    assert daft_lance.read_lance(table_id=table_id, **ns).count_rows() == 5

    daft_lance.write_lance(
        daft.from_pydict({"id": [6], "label": ["overwritten"], "score": [60]}),
        table_id=table_id,
        mode="overwrite",
        **ns,
    ).collect()
    assert daft_lance.read_lance(table_id=table_id, **ns).to_pydict() == {
        "id": [6],
        "label": ["overwritten"],
        "score": [60],
    }

    missing_table_id = [catalog, schema, f"overwrite_missing_{uuid.uuid4().hex[:8]}"]
    daft_lance.write_lance(
        daft.from_pydict({"id": [7], "label": ["created-by-overwrite"], "score": [70]}),
        table_id=missing_table_id,
        mode="overwrite",
        **ns,
    ).collect()
    assert daft_lance.read_lance(table_id=missing_table_id, **ns).to_pydict() == {
        "id": [7],
        "label": ["created-by-overwrite"],
        "score": [70],
    }
