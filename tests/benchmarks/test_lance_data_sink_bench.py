"""Baseline benchmark suite for ``daft_lance.lance_data_sink.LanceDataSink``.

We drive every write through the public ``DataFrame.write_lance`` entry point so
that we measure the sink as configured by ``daft-lance`` — not raw Lance writes.

To fit within a ~5 minute wall-clock budget on a laptop we use 250K rows
(W1/W2/R1) and 16-char strings instead of the 1M rows / 64-char strings called
out in the original plan. Sweeps and assertions are unchanged.

Per pytest-benchmark conventions:
- write paths use ``benchmark.pedantic(target, rounds=3, warmup_rounds=1)``
- read paths use the default ``benchmark(...)`` call

``benchmark.extra_info`` is populated with the artifact counts and sweep
parameters so they land in ``--benchmark-json`` output.
"""

from __future__ import annotations

import shutil
from collections.abc import Callable
from pathlib import Path
from typing import Any

import lance
import pyarrow as pa
import pytest

import daft

from .conftest import count_artifacts

# Knobs scaled to fit a ~5-minute budget on a laptop.
W1_ROWS = 250_000
W2_ROWS = 250_000
W3_ROWS = 50_000
W1_COLS = {"a": "int64", "b": "float64", "c": "string"}

# Number of input chunks for W1/R1. Splitting the input into many small
# partitions before write reproduces the production "many small fragments"
# pathology that H1 is asking about. Without this, ``from_arrow(big_table)``
# produces a single partition and therefore a single fragment regardless of
# morsel size, which is not the pathology under test.
W1_N_CHUNKS = 10

# Defaults that we use as the "default" morsel value (mirrors daft's default).
_DEFAULT_MORSEL = 131_072


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write(uri: Path, table: pa.Table, **kwargs: Any) -> None:
    """Drive a write through ``DataFrame.write_lance`` from a single table."""
    df = daft.from_arrow(table)
    df.write_lance(str(uri), mode="create", **kwargs).collect()


def _write_chunked(uri: Path, table: pa.Table, n_chunks: int, **kwargs: Any) -> None:
    """Drive a write through ``DataFrame.write_lance`` from many chunks.

    Splitting the input into ``n_chunks`` separate ``from_arrow`` DataFrames and
    concat'ing them mirrors how the sink behaves in production when fed many
    input partitions (e.g. reading from many parquet files). On the Daft native
    runner this produces a multi-partition DataFrame; each input partition
    becomes its own fragment when ``max_rows_per_file`` is not set. That is the
    pathology H1 is testing.
    """
    n = table.num_rows
    step = (n + n_chunks - 1) // n_chunks
    dfs = [daft.from_arrow(table.slice(i, min(step, n - i))) for i in range(0, n, step)]
    df = dfs[0]
    for other in dfs[1:]:
        df = df.concat(other)
    df.write_lance(str(uri), mode="create", **kwargs).collect()


def _wipe(uri: Path) -> None:
    """Remove the dataset between rounds so each round starts from ``create``."""
    if uri.exists():
        shutil.rmtree(uri)


def _attach_extra_info(
    benchmark: Any,
    *,
    uri: Path,
    rows_written: int,
    morsel_size: int,
    max_rows_per_file: int | None,
    data_storage_version: str | None,
    **extra: Any,
) -> dict[str, Any]:
    artifacts = count_artifacts(str(uri))
    info: dict[str, Any] = {
        **artifacts,
        "rows_written": rows_written,
        "morsel_size": morsel_size,
        "max_rows_per_file": max_rows_per_file,
        "data_storage_version": data_storage_version,
        **extra,
    }
    benchmark.extra_info.update(info)
    return info


# ---------------------------------------------------------------------------
# W1: max_rows_per_file sweep
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "morsel",
    [_DEFAULT_MORSEL, 1_000_000],
    ids=["morsel-default", "morsel-1M"],
)
@pytest.mark.parametrize(
    "max_rows_per_file",
    [None, 100_000, 250_000, 1_000_000],
    ids=["mrpf-none", "mrpf-100k", "mrpf-250k", "mrpf-1M"],
)
def test_write_w1_max_rows_per_file_sweep(
    tmp_dataset_dir: Path,
    random_table: Callable[..., pa.Table],
    set_morsel_size: Callable[[int], None],
    benchmark: Any,
    morsel: int,
    max_rows_per_file: int | None,
) -> None:
    set_morsel_size(morsel)
    table = random_table(W1_ROWS, W1_COLS)

    kwargs: dict[str, Any] = {}
    if max_rows_per_file is not None:
        kwargs["max_rows_per_file"] = max_rows_per_file

    def target() -> None:
        _wipe(tmp_dataset_dir)
        _write_chunked(tmp_dataset_dir, table, W1_N_CHUNKS, **kwargs)

    benchmark.pedantic(target, rounds=3, warmup_rounds=1)

    info = _attach_extra_info(
        benchmark,
        uri=tmp_dataset_dir,
        rows_written=W1_ROWS,
        morsel_size=morsel,
        max_rows_per_file=max_rows_per_file,
        data_storage_version=None,
        n_input_chunks=W1_N_CHUNKS,
    )

    # H1 (post-fix): each executor partition runs ``write()`` with its own
    # in-sink accumulator. With ``W1_N_CHUNKS`` partitions and per-chunk row
    # counts well below ``max_rows_per_file``, each partition emits exactly one
    # fragment when ``max_rows_per_file`` is left at the default. Coalescing
    # across partitions is out of scope for the buffer (it lives below the
    # executor); see ``test_default_path_coalesces_multi_partition_input`` in
    # ``tests/io/lancedb/test_lance_data_sink_internals.py`` for the in-sink
    # invariant that the buffer collapses *within* a single ``write()`` call.
    if max_rows_per_file is None and morsel == _DEFAULT_MORSEL:
        assert info["n_fragments"] == W1_N_CHUNKS, info


# ---------------------------------------------------------------------------
# W2: storage version sweep
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "data_storage_version",
    [None, "2.0", "2.1", "2.2", "2.3"],
    ids=["dsv-default", "dsv-2.0", "dsv-2.1", "dsv-2.2", "dsv-2.3"],
)
def test_write_w2_storage_version_sweep(
    tmp_dataset_dir: Path,
    random_table: Callable[..., pa.Table],
    benchmark: Any,
    data_storage_version: str | None,
) -> None:
    table = random_table(W2_ROWS, W1_COLS)
    kwargs: dict[str, Any] = {}
    if data_storage_version is not None:
        kwargs["data_storage_version"] = data_storage_version

    def target() -> None:
        _wipe(tmp_dataset_dir)
        _write(tmp_dataset_dir, table, **kwargs)

    benchmark.pedantic(target, rounds=3, warmup_rounds=1)

    _attach_extra_info(
        benchmark,
        uri=tmp_dataset_dir,
        rows_written=W2_ROWS,
        morsel_size=_DEFAULT_MORSEL,
        max_rows_per_file=None,
        data_storage_version=data_storage_version,
    )


# ---------------------------------------------------------------------------
# W3: blob sweep
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "with_blob_columns",
    [False, True],
    ids=["blob-off", "blob-on"],
)
@pytest.mark.parametrize(
    "blob_size",
    [1024, 262_144, 1_048_576],
    ids=["blob-1k", "blob-256k", "blob-1M"],
)
def test_write_w3_blob_sweep(
    tmp_dataset_dir: Path,
    blob_table: Callable[..., pa.Table],
    benchmark: Any,
    blob_size: int,
    with_blob_columns: bool,
) -> None:
    # Cap row count so total bytes stay bounded: rows * blob_size <= ~512 MB.
    rows = min(W3_ROWS, max(1, 512 * 1024 * 1024 // blob_size))
    table = blob_table(rows, blob_size)

    kwargs: dict[str, Any] = {}
    if with_blob_columns:
        kwargs["blob_columns"] = ["payload"]

    def target() -> None:
        _wipe(tmp_dataset_dir)
        _write(tmp_dataset_dir, table, **kwargs)

    benchmark.pedantic(target, rounds=3, warmup_rounds=1)

    info = _attach_extra_info(
        benchmark,
        uri=tmp_dataset_dir,
        rows_written=rows,
        morsel_size=_DEFAULT_MORSEL,
        max_rows_per_file=None,
        data_storage_version="2.2" if with_blob_columns else None,
        blob_size=blob_size,
        with_blob_columns=with_blob_columns,
    )

    if with_blob_columns and blob_size >= 1_048_576:
        # Large blobs land in dedicated sidecar files; the 1 KiB and 256 KiB
        # cases hit inline/packed and won't always produce .blob files.
        assert info["n_blob_files"] >= 1, info


# ---------------------------------------------------------------------------
# R1: full scan over W1-style datasets
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "max_rows_per_file",
    [None, 100_000, 250_000, 1_000_000],
    ids=["mrpf-none", "mrpf-100k", "mrpf-250k", "mrpf-1M"],
)
def test_read_r1_full_scan(
    tmp_dataset_dir: Path,
    random_table: Callable[..., pa.Table],
    benchmark: Any,
    max_rows_per_file: int | None,
) -> None:
    table = random_table(W1_ROWS, W1_COLS)
    kwargs: dict[str, Any] = {}
    if max_rows_per_file is not None:
        kwargs["max_rows_per_file"] = max_rows_per_file
    _wipe(tmp_dataset_dir)
    _write_chunked(tmp_dataset_dir, table, W1_N_CHUNKS, **kwargs)

    def target() -> pa.Table:
        return daft.read_lance(str(tmp_dataset_dir)).to_arrow()

    result = benchmark(target)
    assert result.num_rows == W1_ROWS

    _attach_extra_info(
        benchmark,
        uri=tmp_dataset_dir,
        rows_written=W1_ROWS,
        morsel_size=_DEFAULT_MORSEL,
        max_rows_per_file=max_rows_per_file,
        data_storage_version=None,
        n_input_chunks=W1_N_CHUNKS,
    )


# ---------------------------------------------------------------------------
# R2: blob take from W3-style datasets
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "take_n",
    [10, 1000],
    ids=["take-10", "take-1000"],
)
@pytest.mark.parametrize(
    "blob_size",
    [1024, 1_048_576],
    ids=["blob-1k", "blob-1M"],
)
def test_read_r2_blob_take(
    tmp_dataset_dir: Path,
    blob_table: Callable[..., pa.Table],
    benchmark: Any,
    blob_size: int,
    take_n: int,
) -> None:
    rows = min(W3_ROWS, max(1, 512 * 1024 * 1024 // blob_size))
    if take_n > rows:
        pytest.skip(f"take_n {take_n} > rows {rows}")
    table = blob_table(rows, blob_size)
    _wipe(tmp_dataset_dir)
    _write(tmp_dataset_dir, table, blob_columns=["payload"])

    def target() -> pa.Table:
        return daft.read_lance(str(tmp_dataset_dir)).limit(take_n).to_arrow()

    result = benchmark(target)
    assert result.num_rows == take_n

    _attach_extra_info(
        benchmark,
        uri=tmp_dataset_dir,
        rows_written=rows,
        morsel_size=_DEFAULT_MORSEL,
        max_rows_per_file=None,
        data_storage_version="2.2",
        blob_size=blob_size,
        take_n=take_n,
    )


# ---------------------------------------------------------------------------
# W4 / R3: 10M-row customer-scale comparison (slow; opt-in via `-m slow`)
#
# These reproduce the customer-report workload (10M rows, mixed schema, default
# knobs) so we can compare LanceDataSink before/after the refactor head-to-head
# against `lance.write_dataset` as a fixed baseline. Single round per test —
# we care about ratios, not micro-variance.
# ---------------------------------------------------------------------------

W4_ROWS = 10_000_000
W4_COLS = W1_COLS  # int64 + float64 + string(16)

# Number of input chunks for W4 chunked write. Mirrors the customer scenario
# where input data arrives as many small partitions (e.g. one per source
# parquet file). 80 chunks at 10M rows = ~125K rows per chunk, which falls
# well below Lance's 1M-row default `max_rows_per_file` — so without the
# in-sink buffer, each chunk's micropartitions each become their own fragment.
W4_N_CHUNKS = 80


@pytest.mark.slow
def test_w4_10m_default_write(
    tmp_dataset_dir: Path,
    random_table: Callable[..., pa.Table],
    benchmark: Any,
) -> None:
    """10M-row write through ``daft.DataFrame.write_lance`` with default knobs."""
    table = random_table(W4_ROWS, W4_COLS)
    uri = tmp_dataset_dir

    def target() -> None:
        _wipe(uri)
        _write(uri, table)

    benchmark.pedantic(target, rounds=1, warmup_rounds=0)
    _attach_extra_info(
        benchmark,
        uri=uri,
        rows_written=W4_ROWS,
        morsel_size=_DEFAULT_MORSEL,
        max_rows_per_file=None,
        data_storage_version=None,
        path="daft",
    )


@pytest.mark.slow
def test_w4_10m_chunked_write(
    tmp_dataset_dir: Path,
    random_table: Callable[..., pa.Table],
    benchmark: Any,
) -> None:
    """10M rows fed to ``write_lance`` as ``W4_N_CHUNKS`` separate partitions.

    Mirrors the customer scenario where an upstream operation produces many
    small partitions (e.g. one per source file). Before the refactor, each
    partition's micropartitions each emitted a separate fragment; after the
    refactor, the in-sink buffer coalesces them up to ``max_rows_per_file``.
    """
    table = random_table(W4_ROWS, W4_COLS)
    uri = tmp_dataset_dir

    def target() -> None:
        _wipe(uri)
        _write_chunked(uri, table, n_chunks=W4_N_CHUNKS)

    benchmark.pedantic(target, rounds=1, warmup_rounds=0)
    _attach_extra_info(
        benchmark,
        uri=uri,
        rows_written=W4_ROWS,
        morsel_size=_DEFAULT_MORSEL,
        max_rows_per_file=None,
        data_storage_version=None,
        path="daft-chunked",
        n_chunks=W4_N_CHUNKS,
    )


@pytest.mark.slow
def test_w4_10m_lance_native(
    tmp_dataset_dir: Path,
    random_table: Callable[..., pa.Table],
    benchmark: Any,
) -> None:
    """Same 10M rows via ``lance.write_dataset`` to anchor a Lance-native baseline."""
    table = random_table(W4_ROWS, W4_COLS)
    uri = tmp_dataset_dir

    def target() -> None:
        _wipe(uri)
        lance.write_dataset(table, str(uri), mode="create")

    benchmark.pedantic(target, rounds=1, warmup_rounds=0)
    _attach_extra_info(
        benchmark,
        uri=uri,
        rows_written=W4_ROWS,
        morsel_size=_DEFAULT_MORSEL,
        max_rows_per_file=None,
        data_storage_version=None,
        path="lance-native",
    )


@pytest.mark.slow
def test_r3_10m_full_scan(
    tmp_dataset_dir: Path,
    random_table: Callable[..., pa.Table],
    benchmark: Any,
) -> None:
    """Full scan of a 10M-row dataset via ``daft.read_lance().to_arrow()``."""
    table = random_table(W4_ROWS, W4_COLS)
    uri = tmp_dataset_dir
    _wipe(uri)
    _write(uri, table)

    def target() -> pa.Table:
        return daft.read_lance(str(uri)).to_arrow()

    result = benchmark(target)
    assert result.num_rows == W4_ROWS
    _attach_extra_info(
        benchmark,
        uri=uri,
        rows_written=W4_ROWS,
        morsel_size=_DEFAULT_MORSEL,
        max_rows_per_file=None,
        data_storage_version=None,
        path="daft-read",
    )
