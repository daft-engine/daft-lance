from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import daft
import daft_lance
import daft_lance.namespace as namespace_mod

pytestmark = pytest.mark.lance_native_teardown_crash_workaround


def _dir_ns(tmp_path: Path) -> dict[str, Any]:
    return {"namespace_impl": "dir", "namespace_properties": {"root": str(tmp_path)}}


def test_namespace_write_read_append_roundtrip(tmp_path: Path) -> None:
    ns = _dir_ns(tmp_path)
    table_id = ["roundtrip"]

    df1 = daft.from_pydict({"id": [1, 2], "label": ["a", "b"]})
    df2 = daft.from_pydict({"id": [3], "label": ["c"]})

    daft_lance.write_lance(df1, table_id=table_id, mode="create", **ns).collect()
    daft_lance.write_lance(df2, table_id=table_id, mode="append", **ns).collect()

    result = daft_lance.read_lance(table_id=table_id, **ns).to_pydict()

    assert result == {"id": [1, 2, 3], "label": ["a", "b", "c"]}


def test_namespace_overwrite(tmp_path: Path) -> None:
    ns = _dir_ns(tmp_path)
    table_id = ["overwrite_tbl"]

    daft_lance.write_lance(daft.from_pydict({"id": [1, 2]}), table_id=table_id, mode="create", **ns).collect()
    daft_lance.write_lance(daft.from_pydict({"id": [7, 8, 9]}), table_id=table_id, mode="overwrite", **ns).collect()

    result = daft_lance.read_lance(table_id=table_id, **ns).to_pydict()
    assert result == {"id": [7, 8, 9]}


def test_namespace_overwrite_missing_table_declares(tmp_path: Path) -> None:
    ns = _dir_ns(tmp_path)
    table_id = ["overwrite_fresh"]

    daft_lance.write_lance(daft.from_pydict({"id": [1]}), table_id=table_id, mode="overwrite", **ns).collect()

    result = daft_lance.read_lance(table_id=table_id, **ns).to_pydict()
    assert result == {"id": [1]}


def test_namespace_overwrite_does_not_declare_on_ambiguous_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    class FakeNamespace:
        declared = False

        def describe_table(self, request: Any) -> Any:
            raise RuntimeError("permission denied: parent catalog does not exist")

        def declare_table(self, request: Any) -> Any:
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


def test_namespace_untyped_404_table_not_found_is_missing() -> None:
    class RestTableNotFound(RuntimeError):
        status = 404

    assert namespace_mod.is_table_not_found(RestTableNotFound("table catalog.schema.table not found"))


def test_namespace_read_supports_pushdowns(tmp_path: Path) -> None:
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

    predicate = daft.col("score") > 10  # type: ignore[operator]
    result = daft_lance.read_lance(table_id=table_id, **ns).where(predicate).select("label").to_pydict()

    assert result == {"label": ["b", "c"]}


def test_namespace_count_pushdown(tmp_path: Path) -> None:
    ns = _dir_ns(tmp_path)
    table_id = ["count_tbl"]

    daft_lance.write_lance(daft.from_pydict({"id": list(range(10))}), table_id=table_id, mode="create", **ns).collect()

    assert daft_lance.read_lance(table_id=table_id, **ns).count_rows() == 10


def test_namespace_patch_daft(tmp_path: Path) -> None:
    ns = _dir_ns(tmp_path)
    table_id = ["patched"]

    daft_lance.patch_daft()
    daft_lance.patch_daft()  # idempotent

    daft.from_pydict({"id": [1, 2]}).write_lance(table_id=table_id, mode="create", **ns).collect()
    result = daft.read_lance(table_id=table_id, **ns).to_pydict()  # type: ignore[call-arg]
    assert result == {"id": [1, 2]}

    # Plain-URI writes keep working through the patched method.
    uri = str(tmp_path / "plain.lance")
    daft.from_pydict({"id": [5]}).write_lance(uri).collect()
    assert daft.read_lance(uri).to_pydict() == {"id": [5]}


def test_namespace_write_lance_merge_new_columns(tmp_path: Path) -> None:
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


def test_namespace_merge_columns_df(tmp_path: Path) -> None:
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


def test_namespace_create_scalar_index(tmp_path: Path) -> None:
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
    assert any(idx["fields"] == ["price"] for idx in indices)  # type: ignore[index]


def test_namespace_compact_files(tmp_path: Path) -> None:
    ns = _dir_ns(tmp_path)
    table_id = ["compacted"]

    daft_lance.write_lance(daft.from_pydict({"id": [1]}), table_id=table_id, mode="create", **ns).collect()
    for i in range(3):
        daft_lance.write_lance(daft.from_pydict({"id": [i + 2]}), table_id=table_id, mode="append", **ns).collect()

    daft_lance.compact_files(table_id=table_id, compaction_options={"target_rows_per_fragment": 1024}, **ns)

    result = daft_lance.read_lance(table_id=table_id, **ns).sort("id").to_pydict()
    assert result == {"id": [1, 2, 3, 4]}


def test_sink_construction_is_side_effect_free(tmp_path: Path) -> None:
    ns = _dir_ns(tmp_path)
    schema = daft.from_pydict({"id": [1]}).schema()

    sink = daft_lance.LanceDataSink(None, schema, "create", table_id=["deferred"], **ns)
    assert not (tmp_path / "deferred.lance").exists()

    sink.start()
    assert (tmp_path / "deferred.lance").exists()


def test_sink_invalid_params_do_not_declare_table(tmp_path: Path) -> None:
    ns = _dir_ns(tmp_path)
    schema = daft.from_pydict({"id": [1]}).schema()

    with pytest.raises(ValueError, match="blob_columns"):
        daft_lance.LanceDataSink(None, schema, "create", table_id=["orphan"], blob_columns=["missing"], **ns)

    assert not (tmp_path / "orphan.lance").exists()


def test_namespace_create_on_existing_table_raises(tmp_path: Path) -> None:
    ns = _dir_ns(tmp_path)
    table_id = ["exists"]

    daft_lance.write_lance(daft.from_pydict({"id": [1]}), table_id=table_id, mode="create", **ns).collect()

    with pytest.raises(ValueError, match="already exists"):
        daft_lance.write_lance(daft.from_pydict({"id": [2]}), table_id=table_id, mode="create", **ns).collect()


def test_namespace_create_over_declared_placeholder(tmp_path: Path) -> None:
    import lance_namespace as ln
    from lance_namespace import DeclareTableRequest

    ns = _dir_ns(tmp_path)
    table_id = ["declared_first"]

    namespace = ln.connect("dir", {"root": str(tmp_path)})
    namespace.declare_table(DeclareTableRequest(id=table_id, location=None))

    daft_lance.write_lance(daft.from_pydict({"id": [1, 2]}), table_id=table_id, mode="create", **ns).collect()

    assert daft_lance.read_lance(table_id=table_id, **ns).to_pydict() == {"id": [1, 2]}


def test_create_declare_race_recovers_placeholder(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from lance_namespace.errors import TableAlreadyExistsError, TableNotFoundError

    class RacingNamespace:
        def __init__(self) -> None:
            self.describe_calls = 0

        def describe_table(self, request: Any) -> Any:
            self.describe_calls += 1
            if self.describe_calls == 1:
                raise TableNotFoundError("table not found: t")
            return SimpleNamespace(location=str(tmp_path / "t.lance"), storage_options=None, is_only_declared=True)

        def declare_table(self, request: Any) -> Any:
            raise TableAlreadyExistsError("Table already exists: t")

    fake = RacingNamespace()
    monkeypatch.setattr(namespace_mod, "get_or_create_namespace", lambda *args: fake)

    resolved = namespace_mod.resolve_namespace_table(
        namespace_impl="rest",
        namespace_properties=None,
        table_id=["t"],
        mode="create",
    )
    assert resolved is not None
    assert resolved.is_declared_placeholder
    assert fake.describe_calls == 2


def test_create_declare_race_lost_to_real_table_raises(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from lance_namespace.errors import TableAlreadyExistsError, TableNotFoundError

    class RacingNamespace:
        def __init__(self) -> None:
            self.describe_calls = 0

        def describe_table(self, request: Any) -> Any:
            self.describe_calls += 1
            if self.describe_calls == 1:
                raise TableNotFoundError("table not found: t")
            return SimpleNamespace(location=str(tmp_path / "t.lance"), storage_options=None, is_only_declared=False)

        def declare_table(self, request: Any) -> Any:
            raise TableAlreadyExistsError("Table already exists: t")

    monkeypatch.setattr(namespace_mod, "get_or_create_namespace", lambda *args: RacingNamespace())

    with pytest.raises(ValueError, match="already exists"):
        namespace_mod.resolve_namespace_table(
            namespace_impl="rest",
            namespace_properties=None,
            table_id=["t"],
            mode="create",
        )


def test_declared_placeholder_response_fallback_property() -> None:
    assert namespace_mod.is_declared_placeholder_response(SimpleNamespace(is_only_declared=True))
    assert namespace_mod.is_declared_placeholder_response(
        SimpleNamespace(is_only_declared=None, properties={"lance.declared": "TRUE"})
    )
    assert not namespace_mod.is_declared_placeholder_response(
        SimpleNamespace(is_only_declared=None, properties={}, metadata=None)
    )


def test_construct_lance_dataset_storage_options_priority(monkeypatch: pytest.MonkeyPatch) -> None:
    import daft_lance.utils as utils_mod
    from daft.io import IOConfig, S3Config

    captured = {}

    class FakeDataset:
        pass

    def fake_dataset(uri: Any, storage_options: Any = None, version: Any = None, **kwargs: Any) -> Any:
        captured["uri"] = uri
        captured["storage_options"] = storage_options
        return FakeDataset()

    monkeypatch.setattr("daft_lance.utils.lance.dataset", fake_dataset)
    monkeypatch.setattr(utils_mod, "get_namespace_kwargs", lambda *args: {})
    monkeypatch.setattr(
        utils_mod,
        "resolve_namespace_table",
        lambda **kwargs: namespace_mod.ResolvedNamespaceTable(
            uri="s3://bucket/t.lance",
            storage_options={"access_key_id": "vended-key", "session_token": "vended-token"},
        ),
    )

    io_config = IOConfig(s3=S3Config(key_id="io-key", access_key="io-secret", region_name="us-east-1"))
    utils_mod.construct_lance_dataset(
        None,
        io_config=io_config,
        storage_options={"access_key_id": "user-key", "user_option": "kept"},
        namespace_impl="rest",
        namespace_properties={"uri": "http://namespace.example"},
        table_id=["t"],
    )

    merged = captured["storage_options"]
    assert merged is not None
    assert merged["access_key_id"] == "vended-key"  # namespace-vended beats user-provided
    assert merged["session_token"] == "vended-token"
    assert merged["user_option"] == "kept"  # user-provided keys survive
    assert merged["secret_access_key"] == "io-secret"  # io_config fills the gaps
    assert captured["uri"] is None  # namespace addressing passes uri=None


def test_construct_lance_dataset_io_config_reaches_namespace_location(monkeypatch: pytest.MonkeyPatch) -> None:
    import daft_lance.utils as utils_mod
    from daft.io import IOConfig, S3Config

    captured = {}

    class FakeDataset:
        pass

    def fake_dataset(uri: Any, storage_options: Any = None, version: Any = None, **kwargs: Any) -> Any:
        captured["storage_options"] = storage_options
        return FakeDataset()

    monkeypatch.setattr("daft_lance.utils.lance.dataset", fake_dataset)
    monkeypatch.setattr(utils_mod, "get_namespace_kwargs", lambda *args: {})
    monkeypatch.setattr(
        utils_mod,
        "resolve_namespace_table",
        lambda **kwargs: namespace_mod.ResolvedNamespaceTable(uri="s3://bucket/t.lance", storage_options=None),
    )

    io_config = IOConfig(s3=S3Config(key_id="io-key", access_key="io-secret", region_name="us-east-1"))
    utils_mod.construct_lance_dataset(
        None,
        io_config=io_config,
        namespace_impl="rest",
        namespace_properties={"uri": "http://namespace.example"},
        table_id=["t"],
    )

    merged = captured["storage_options"]
    assert merged is not None
    assert merged["access_key_id"] == "io-key"
    assert merged["secret_access_key"] == "io-secret"


def test_sink_storage_options_priority(tmp_path: Path) -> None:
    from daft.io import IOConfig, S3Config

    ns = _dir_ns(tmp_path)
    schema = daft.from_pydict({"id": [1]}).schema()
    io_config = IOConfig(s3=S3Config(key_id="io-key", access_key="io-secret", region_name="us-east-1"))

    sink = daft_lance.LanceDataSink(
        None,
        schema,
        "create",
        io_config,
        table_id=["t"],
        storage_options={"user_option": "kept", "access_key_id": "user-key"},
        **ns,
    )
    resolved = namespace_mod.ResolvedNamespaceTable(
        uri="s3://bucket/t.lance", storage_options={"access_key_id": "vended-key"}
    )

    merged = sink._merged_storage_options(resolved)
    assert merged is not None
    assert merged["access_key_id"] == "vended-key"
    assert merged["user_option"] == "kept"
    assert merged["secret_access_key"] == "io-secret"


def test_namespace_rejects_uri_and_namespace(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Cannot provide both 'uri' and namespace parameters"):
        daft_lance.read_lance(
            str(tmp_path / "dataset"),
            namespace_impl="dir",
            namespace_properties={"root": str(tmp_path)},
            table_id=["tbl"],
        )


def test_namespace_requires_table_id(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="'table_id' must be provided"):
        daft_lance.read_lance(
            namespace_impl="dir",
            namespace_properties={"root": str(tmp_path)},
        )


def test_namespace_requires_impl() -> None:
    with pytest.raises(ValueError, match="'namespace_impl' must be provided"):
        daft_lance.read_lance(table_id=["tbl"])


def test_namespace_requires_uri_or_namespace() -> None:
    with pytest.raises(ValueError, match="Must provide either 'uri' OR"):
        daft_lance.read_lance()
