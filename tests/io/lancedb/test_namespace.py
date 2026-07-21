from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import daft
import daft_lance
import daft_lance.namespace as namespace_mod
from daft_lance.lance_data_sink import LanceDataSink


def _dir_ns(tmp_path: Path) -> dict[str, Any]:
    return {"namespace_impl": "dir", "namespace_properties": {"root": str(tmp_path)}}


def _double_score(batch: Any) -> Any:
    import pyarrow as pa
    import pyarrow.compute as pc

    return pa.RecordBatch.from_arrays([pc.multiply(batch["score"], 2)], ["doubled"])


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


def test_namespace_merge_columns_transform(tmp_path: Path) -> None:
    ns = _dir_ns(tmp_path)
    table_id = ["merge_cols_transform"]

    daft_lance.write_lance(
        daft.from_pydict({"id": [1, 2, 3], "score": [10, 20, 30]}),
        table_id=table_id,
        mode="create",
        **ns,
    ).collect()

    daft_lance.merge_columns(table_id=table_id, transform=_double_score, read_columns=["score"], **ns)

    result = daft_lance.read_lance(table_id=table_id, **ns).sort("id").to_pydict()
    assert result["doubled"] == [20, 40, 60]


def test_namespace_merge_columns_df_slow_path(tmp_path: Path) -> None:
    ns = _dir_ns(tmp_path)
    table_id = ["merge_cols_slow"]

    daft_lance.write_lance(
        daft.from_pydict({"id": [1, 2, 3], "score": [10, 20, 30]}),
        table_id=table_id,
        mode="create",
        **ns,
    ).collect()

    source = daft_lance.read_lance(
        table_id=table_id,
        default_scan_options={"with_row_address": True},
        include_fragment_id=True,
        **ns,
    ).limit(2)
    source = source.with_column("partial_score", source["score"] * 10)
    daft_lance.merge_columns_df(
        source.select("fragment_id", "_rowaddr", "partial_score"),
        table_id=table_id,
        **ns,
    )

    result = daft_lance.read_lance(table_id=table_id, **ns).sort("id").to_pydict()
    assert result["partial_score"] == [100, 200, None]


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
    assert any(idx["fields"] == ["price"] for idx in indices)


@pytest.mark.parametrize("segmented", [False, True])
def test_namespace_create_distributed_inverted_index(tmp_path: Path, segmented: bool) -> None:
    ns = _dir_ns(tmp_path)
    table_id = [f"inverted_{segmented}"]

    daft_lance.write_lance(
        daft.from_pydict({"id": list(range(20)), "text": [f"document {i}" for i in range(20)]}),
        table_id=table_id,
        mode="create",
        **ns,
    ).collect()

    daft_lance.create_scalar_index(
        table_id=table_id,
        column="text",
        index_type="INVERTED",
        name="text_idx",
        segmented=segmented,
        **ns,
    )

    import lance
    import lance_namespace as ln
    from lance_namespace import DescribeTableRequest

    location = ln.connect("dir", {"root": str(tmp_path)}).describe_table(DescribeTableRequest(id=table_id)).location
    indices = lance.dataset(location).list_indices()
    assert any(idx["name"] == "text_idx" and idx["fields"] == ["text"] for idx in indices)


def test_namespace_compact_files(tmp_path: Path) -> None:
    ns = _dir_ns(tmp_path)
    table_id = ["compacted"]

    daft_lance.write_lance(daft.from_pydict({"id": [1]}), table_id=table_id, mode="create", **ns).collect()
    for i in range(3):
        daft_lance.write_lance(daft.from_pydict({"id": [i + 2]}), table_id=table_id, mode="append", **ns).collect()

    import lance
    import lance_namespace as ln
    from lance_namespace import DescribeTableRequest

    location = ln.connect("dir", {"root": str(tmp_path)}).describe_table(DescribeTableRequest(id=table_id)).location
    fragments_before = len(lance.dataset(location).get_fragments())

    daft_lance.compact_files(table_id=table_id, compaction_options={"target_rows_per_fragment": 1024}, **ns)

    assert len(lance.dataset(location).get_fragments()) < fragments_before

    result = daft_lance.read_lance(table_id=table_id, **ns).sort("id").to_pydict()
    assert result == {"id": [1, 2, 3, 4]}


def test_sink_construction_is_side_effect_free(tmp_path: Path) -> None:
    ns = _dir_ns(tmp_path)
    schema = daft.from_pydict({"id": [1]}).schema()

    sink = LanceDataSink(None, schema, "create", table_id=["deferred"], **ns)
    assert not (tmp_path / "deferred.lance").exists()

    sink.start()
    assert (tmp_path / "deferred.lance").exists()


def test_sink_invalid_params_do_not_declare_table(tmp_path: Path) -> None:
    ns = _dir_ns(tmp_path)
    schema = daft.from_pydict({"id": [1]}).schema()

    with pytest.raises(ValueError, match="blob_columns"):
        LanceDataSink(None, schema, "create", table_id=["orphan"], blob_columns=["missing"], **ns)

    assert not (tmp_path / "orphan.lance").exists()


def test_namespace_create_on_existing_table_raises(tmp_path: Path) -> None:
    from lance_namespace.errors import TableAlreadyExistsError

    ns = _dir_ns(tmp_path)
    table_id = ["exists"]

    daft_lance.write_lance(daft.from_pydict({"id": [1]}), table_id=table_id, mode="create", **ns).collect()

    with pytest.raises(TableAlreadyExistsError, match="already exists"):
        daft_lance.write_lance(daft.from_pydict({"id": [2]}), table_id=table_id, mode="create", **ns).collect()


def test_namespace_create_on_declared_table_raises(tmp_path: Path) -> None:
    import lance_namespace as ln
    from lance_namespace import DeclareTableRequest
    from lance_namespace.errors import TableAlreadyExistsError

    ns = _dir_ns(tmp_path)
    table_id = ["declared_first"]

    namespace = ln.connect("dir", {"root": str(tmp_path)})
    namespace.declare_table(DeclareTableRequest(id=table_id, location=None))

    with pytest.raises(TableAlreadyExistsError, match="already exists"):
        daft_lance.write_lance(daft.from_pydict({"id": [1, 2]}), table_id=table_id, mode="create", **ns).collect()


def test_namespace_create_declares_without_describe(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    requests: list[Any] = []

    class RecordingNamespace:
        def describe_table(self, request: Any) -> Any:
            raise AssertionError("create must not describe before declaring")

        def declare_table(self, request: Any) -> Any:
            requests.append(request)
            return SimpleNamespace(location=str(tmp_path / "t.lance"), storage_options=None)

    monkeypatch.setattr(namespace_mod, "get_or_create_namespace", lambda *args: RecordingNamespace())

    resolved = namespace_mod.resolve_namespace_table(
        namespace_impl="rest", namespace_properties=None, table_id=["t"], mode="create"
    )

    assert resolved is not None
    assert resolved.uri.endswith("t.lance")
    assert len(requests) == 1
    assert requests[0].vend_credentials is True


def test_namespace_overwrite_uses_plain_describe(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    requests: list[Any] = []

    class RecordingNamespace:
        def describe_table(self, request: Any) -> Any:
            requests.append(request)
            return SimpleNamespace(location=str(tmp_path / "t.lance"), storage_options=None)

        def declare_table(self, request: Any) -> Any:
            raise AssertionError("an existing overwrite target must not be declared")

    monkeypatch.setattr(namespace_mod, "get_or_create_namespace", lambda *args: RecordingNamespace())

    resolved = namespace_mod.resolve_namespace_table(
        namespace_impl="rest", namespace_properties=None, table_id=["t"], mode="overwrite"
    )

    assert resolved is not None
    assert len(requests) == 1
    assert requests[0].model_fields_set == {"id", "vend_credentials"}
    assert requests[0].vend_credentials is True


def test_construct_lance_dataset_empty_storage_options_falls_back_to_io_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import daft_lance.utils as utils_mod
    from daft.io import IOConfig, S3Config

    captured = {}

    class FakeDataset:
        pass

    def fake_dataset(uri: Any, storage_options: Any = None, version: Any = None, **kwargs: Any) -> Any:
        captured["storage_options"] = storage_options
        return FakeDataset()

    monkeypatch.setattr("daft_lance.utils.lance.dataset", fake_dataset)

    io_config = IOConfig(s3=S3Config(key_id="io-key", access_key="io-secret", region_name="us-east-1"))
    dataset = utils_mod.construct_lance_dataset("s3://bucket/t.lance", storage_options={}, io_config=io_config)

    merged = captured["storage_options"]
    assert isinstance(dataset, FakeDataset)
    assert merged is not None
    assert merged["access_key_id"] == "io-key"


def test_sink_survives_pickle_after_start(tmp_path: Path) -> None:
    """start() state must round-trip through pickle: workers run write() on a copy."""
    import pickle

    ns = _dir_ns(tmp_path)
    schema = daft.from_pydict({"id": [1]}).schema()

    sink = LanceDataSink(None, schema, "create", table_id=["pickled"], **ns)
    sink.start()

    worker_sink = pickle.loads(pickle.dumps(sink))
    assert worker_sink._table_uri == sink._table_uri
    assert worker_sink._storage_options == sink._storage_options
    assert worker_sink._effective_pyarrow_schema == sink._effective_pyarrow_schema

    from daft.recordbatch import MicroPartition

    results = list(worker_sink.write(iter([MicroPartition.from_pydict({"id": [1, 2]})])))
    stats = sink.finalize(results).to_pydict()
    assert stats["version"] == [1]
    assert daft_lance.read_lance(table_id=["pickled"], **ns).to_pydict() == {"id": [1, 2]}


def test_namespace_requests_explicitly_vend_credentials(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Whether a namespace vends credentials is implementation-defined unless requested."""
    from lance_namespace.errors import TableNotFoundError

    requests: list[Any] = []

    class RecordingNamespace:
        def describe_table(self, request: Any) -> Any:
            requests.append(request)
            raise TableNotFoundError("table not found: t")

        def declare_table(self, request: Any) -> Any:
            requests.append(request)
            return SimpleNamespace(location=str(tmp_path / "t.lance"), storage_options=None)

    monkeypatch.setattr(namespace_mod, "get_or_create_namespace", lambda *args: RecordingNamespace())

    for mode in ("create", "overwrite"):
        namespace_mod.resolve_namespace_table(
            namespace_impl="rest", namespace_properties=None, table_id=["t"], mode=mode
        )
    with pytest.raises(TableNotFoundError):
        namespace_mod.resolve_namespace_table(
            namespace_impl="rest", namespace_properties=None, table_id=["t"], mode="read"
        )

    assert requests, "expected describe/declare requests to be issued"
    assert all(request.vend_credentials is True for request in requests)


def test_sink_empty_storage_options_falls_back_to_io_config(tmp_path: Path) -> None:
    """storage_options={} must behave like the read entry points: fall through to io_config."""
    from daft.io import IOConfig, S3Config

    io_config = IOConfig(s3=S3Config(key_id="io-key", access_key="io-secret", region_name="us-east-1"))
    schema = daft.from_pydict({"id": [1]}).schema()

    sink = LanceDataSink("s3://bucket/t.lance", schema, "create", io_config, storage_options={})
    merged = sink._merged_storage_options(namespace_mod.ResolvedNamespaceTable(uri="s3://bucket/t.lance"))
    assert merged is not None
    assert merged["access_key_id"] == "io-key"


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
    handle = utils_mod.construct_lance_dataset_handle(
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
    assert handle.storage_options == merged
    assert handle.uri == "s3://bucket/t.lance"
    assert not hasattr(handle.dataset, "_lance_open_kwargs")


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

    sink = LanceDataSink(
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


def test_namespace_properties_require_impl(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="'namespace_impl' must be provided when 'namespace_properties'"):
        daft_lance.read_lance(str(tmp_path / "dataset"), namespace_properties={"root": str(tmp_path)})


def test_namespace_requires_impl() -> None:
    with pytest.raises(ValueError, match="'namespace_impl' must be provided"):
        daft_lance.read_lance(table_id=["tbl"])


def test_namespace_requires_uri_or_namespace() -> None:
    with pytest.raises(ValueError, match="Must provide either 'uri' OR"):
        daft_lance.read_lance()
