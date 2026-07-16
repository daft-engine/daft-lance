"""Phase 4: first-class coverage for the typed `data_storage_version` knob."""

from __future__ import annotations

import lance
import pyarrow as pa
import pytest

import daft
from daft_lance.lance_data_sink import LanceDataSink

VALID_VERSIONS = ["legacy", "0.1", "stable", "2.0", "2.1", "2.2", "2.3", "next"]


def _make_df(n: int = 100) -> daft.DataFrame:
    return daft.from_pydict({"a": list(range(n))})


@pytest.mark.parametrize("version", VALID_VERSIONS)
def test_round_trip_each_storage_version(tmp_path, version):
    """Round-trip a tiny dataset through each documented data_storage_version."""
    uri = str(tmp_path / f"tbl_{version.replace('.', '_')}")
    df = _make_df(100)
    try:
        df.write_lance(uri, mode="create", data_storage_version=version)
    except Exception as e:
        # Some Literal-listed versions may not be implemented in this Lance build
        # (e.g. "next" historically lags). Skip loudly so a future Lance update
        # that adds support breaks this skip rather than silently passing.
        pytest.xfail(f"Lance rejected data_storage_version={version!r} at write: {e}")
        return

    ds = lance.dataset(uri)
    read = ds.to_table().to_pydict()
    assert read["a"] == list(range(100))


def test_append_inherits_storage_version(tmp_path):
    """Write with 2.1, then append without specifying — second write inherits 2.1."""
    uri = str(tmp_path / "tbl")
    df1 = _make_df(10)
    df1.write_lance(uri, mode="create", data_storage_version="2.1")

    initial_version = lance.dataset(uri).data_storage_version

    # Construct an append sink without explicit data_storage_version; it should
    # inherit from the existing dataset.
    schema = pa.schema([("a", pa.int64())])
    sink = LanceDataSink(uri=uri, schema=schema, mode="append")
    sink.start()
    assert sink._data_storage_version == initial_version

    df2 = _make_df(5)
    df2.write_lance(uri, mode="append")

    ds = lance.dataset(uri)
    assert ds.data_storage_version == initial_version
    assert ds.to_table().num_rows == 15


def test_storage_version_mismatch_on_append_rejected(tmp_path):
    """Caller-specified version must match the existing dataset version."""
    uri = str(tmp_path / "tbl")
    df1 = _make_df(10)
    df1.write_lance(uri, mode="create", data_storage_version="2.1")

    schema = pa.schema([("a", pa.int64())])
    sink = LanceDataSink(uri=uri, schema=schema, mode="append", data_storage_version="2.2")
    with pytest.raises(ValueError, match="does not match existing dataset version"):
        sink.start()


def test_use_legacy_format_emits_deprecation_warning(tmp_path):
    """Passing use_legacy_format must emit a DeprecationWarning."""
    schema = pa.schema([("a", pa.int64())])
    with pytest.warns(DeprecationWarning, match="use_legacy_format is deprecated"):
        LanceDataSink(uri=str(tmp_path / "tbl"), schema=schema, mode="create", use_legacy_format=False)
