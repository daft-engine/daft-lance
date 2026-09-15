from __future__ import annotations

import os
import pickle
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import daft
from daft.daft import CheckpointStatus
from daft.io.sink import WriteResult
from daft.recordbatch import MicroPartition
from daft.schema import Schema
from daft_lance.lance_data_sink import LanceDataSink


class FakeCheckpointStore:
    """Small in-memory stand-in for the checkpoint store API used by finalize().

    Unit tests use this to control pending checkpoints and staged file metadata
    without creating a real checkpoint directory.
    """

    def __init__(self, files=None, checkpoints=None):
        self._files = files or []
        self._checkpoints = checkpoints or []
        self.mark_committed = Mock()

    def get_checkpointed_files(self):
        return self._files

    def list_checkpoints(self):
        return self._checkpoints


def _schema() -> Schema:
    """Shared one-column schema used by fake and real Lance tables."""
    return Schema.from_pyarrow_schema(pa.schema([pa.field("a", pa.int64())]))


def _existing_dataset(version: int = 1):
    """Fake Lance dataset object with just the methods LanceDataSink calls."""
    return SimpleNamespace(
        schema=_schema().to_pyarrow_schema(),
        version=version,
        latest_version=version,
        stats=SimpleNamespace(
            dataset_stats=Mock(return_value={"num_fragments": 0, "num_deleted_rows": 0, "num_small_files": 0})
        ),
        get_transactions=Mock(return_value=[]),
    )


def _checkpoint_file(write_results: list[WriteResult[list[object]]]):
    """Encode write_results in the same shape Daft core stages to the store.

    BlockingSinkNode stores DataSink output as an IPC MicroPartition with a
    write_results column. The fake object returned here mimics one checkpointed
    FileMetadata entry.
    """
    mp = MicroPartition.from_pydict({"write_results": write_results})
    return SimpleNamespace(format=SimpleNamespace(name="Lance"), data=mp.to_ipc_stream())


def _pending_checkpoints(store: daft.CheckpointStore) -> list[object]:
    """Return sealed checkpoint entries that have not been marked committed."""
    return [checkpoint for checkpoint in store.list_checkpoints() if checkpoint.status == CheckpointStatus.Checkpointed]


def test_checkpoint_file_format_is_lance_when_checkpoint_enabled(tmp_path):
    """A checkpointed Lance sink opts in to Daft core staging its write_results.

    This is the small hook that lets Rust Daft know the DataSink output should
    be written to the checkpoint store as Lance metadata.
    """
    checkpoint = SimpleNamespace(store=FakeCheckpointStore(), idempotence_key="run-1")

    with patch("daft_lance.lance_data_sink.lance.dataset", return_value=_existing_dataset()):
        sink = LanceDataSink(tmp_path / "tbl", _schema(), "append", checkpoint=checkpoint)

    assert sink.checkpoint_file_format() == "lance"


def test_checkpoint_sink_pickles_without_driver_store_for_workers(tmp_path):
    """Ray workers can receive the sink without receiving the driver store.

    Workers only run write(). They need to keep _checkpoint_enabled=True so
    Daft core stages write_results, but the actual CheckpointStore is used
    later on the driver in finalize().
    """
    checkpoint = daft.IdempotentCommit(
        store=daft.CheckpointStore(f"file://{tmp_path}/ckpt"),
        idempotence_key="run-1",
    )

    with patch("daft_lance.lance_data_sink.lance.dataset", return_value=_existing_dataset()):
        sink = LanceDataSink(tmp_path / "tbl", _schema(), "append", checkpoint=checkpoint)

    restored = pickle.loads(pickle.dumps(sink))

    # Worker copies keep the checkpoint opt-in signal so Daft core can stage
    # write_results, but they must not carry the driver-side CheckpointStore.
    # Only driver-side finalize() is allowed to read or update the store.
    assert restored.checkpoint_file_format() == "lance"
    assert restored._checkpoint is None
    with pytest.raises(RuntimeError, match="driver-side CheckpointStore"):
        restored.finalize([])


def test_checkpoint_commit_exists_checks_lance_transaction_history(tmp_path):
    """The check-first path looks for the idempotence key in Lance history.

    If the key is already present, a previous attempt has already committed the
    logical append. The retry can skip worker writes and only clean up pending
    checkpoint entries.
    """
    other_tx = SimpleNamespace(transaction_properties={"daft.idempotence-key": "other-run"})
    committed_tx = SimpleNamespace(transaction_properties={"daft.idempotence-key": "run-1"})
    dataset = _existing_dataset(version=3)
    dataset.get_transactions = Mock(return_value=[other_tx, committed_tx])
    checkpoint = SimpleNamespace(store=FakeCheckpointStore(), idempotence_key="run-1")

    with patch("daft_lance.lance_data_sink.lance.dataset", return_value=dataset):
        sink = LanceDataSink(tmp_path / "tbl", _schema(), "append", checkpoint=checkpoint)

    with patch("daft_lance.lance_data_sink.lance.dataset", return_value=dataset):
        assert sink.checkpoint_commit_exists()

    # The pre-check should match the configured idempotence key. Seeing a
    # different Daft idempotence key in the same history is not enough.
    dataset.get_transactions = Mock(return_value=[other_tx])
    with patch("daft_lance.lance_data_sink.lance.dataset", return_value=dataset):
        assert not sink.checkpoint_commit_exists()


def test_checkpoint_requires_append_mode(tmp_path):
    """Checkpoint support is append-only for now.

    Create, overwrite, and merge have different recovery semantics. Rejecting
    them early keeps this implementation narrow and predictable.
    """
    checkpoint = SimpleNamespace(store=FakeCheckpointStore(), idempotence_key="run-1")

    with pytest.raises(NotImplementedError, match="mode='append'"):
        LanceDataSink(tmp_path / "tbl", _schema(), "create", checkpoint=checkpoint)


def test_checkpoint_rejects_mem_wal(tmp_path):
    """Checkpoint recovery uses staged Lance fragments, not MemWAL write results."""
    checkpoint = SimpleNamespace(store=FakeCheckpointStore(), idempotence_key="run-1")

    with pytest.raises(NotImplementedError, match="use_mem_wal=True"):
        LanceDataSink(tmp_path / "tbl", _schema(), "append", checkpoint=checkpoint, use_mem_wal=True)


def test_decode_checkpointed_write_results(tmp_path):
    """Decode the exact payload shape Daft core stores for Lance.

    Daft core stages DataSink output as an IPC MicroPartition with a
    write_results column. daft_lance must read that back before it can recover
    the Lance fragments.
    """
    fragment = SimpleNamespace(path="/tmp/frag", rows=3)
    write_result = WriteResult(result=[fragment], bytes_written=128, rows_written=3)
    checkpoint = SimpleNamespace(
        store=FakeCheckpointStore(files=[_checkpoint_file([write_result])]),
        idempotence_key="run-1",
    )
    with patch("daft_lance.lance_data_sink.lance.dataset", return_value=_existing_dataset()):
        sink = LanceDataSink(tmp_path / "tbl", _schema(), "append", checkpoint=checkpoint)

    decoded = sink._checkpointed_write_results()

    assert decoded == [write_result]


def test_checkpoint_finalize_skips_when_idempotence_key_exists(tmp_path):
    """If Lance already has the idempotence key, do not append again.

    This models a retry after the append transaction landed. The checkpoint
    store may still have pending entries, so finalize only marks them committed.
    """
    committed_tx = SimpleNamespace(transaction_properties={"daft.idempotence-key": "run-1"})
    dataset = _existing_dataset(version=2)
    dataset.get_transactions = Mock(return_value=[committed_tx])
    dataset.stats.dataset_stats = Mock(return_value={"num_fragments": 1, "num_deleted_rows": 0, "num_small_files": 0})
    checkpoint = SimpleNamespace(
        store=FakeCheckpointStore(
            files=[_checkpoint_file([WriteResult(result=["fragment"], bytes_written=1, rows_written=1)])],
            checkpoints=[SimpleNamespace(id="input-0-checkpoint-a", status=SimpleNamespace(name="Checkpointed"))],
        ),
        idempotence_key="run-1",
    )
    with patch("daft_lance.lance_data_sink.lance.dataset", return_value=dataset):
        sink = LanceDataSink(tmp_path / "tbl", _schema(), "append", checkpoint=checkpoint)

    with (
        patch("daft_lance.lance_data_sink.lance.dataset", return_value=dataset),
        patch("daft_lance.lance_data_sink.lance.LanceDataset.commit") as commit,
    ):
        result = sink.finalize([])

    assert result.to_pydict()["version"] == [2]
    # Since the idempotence key is already in Lance history, finalize only
    # marks the pending checkpoint entry committed. It must not append again.
    checkpoint.store.mark_committed.assert_called_once_with(["input-0-checkpoint-a"])
    commit.assert_not_called()


def test_checkpoint_finalize_commits_pending_fragments_with_idempotence_key(tmp_path):
    """Pending checkpointed fragments are committed through a Lance transaction.

    This is the normal recovery path when the store has sealed write_results
    and Lance history does not yet have the idempotence key.
    """
    fragment = SimpleNamespace(path="/tmp/frag", rows=3)
    checkpointed = WriteResult(result=[fragment], bytes_written=128, rows_written=3)
    dataset_before = _existing_dataset(version=4)
    dataset_before.get_transactions = Mock(return_value=[])
    dataset_after = _existing_dataset(version=5)
    dataset_after.stats.dataset_stats = Mock(
        return_value={"num_fragments": 2, "num_deleted_rows": 0, "num_small_files": 0}
    )
    checkpoint = SimpleNamespace(
        store=FakeCheckpointStore(
            files=[_checkpoint_file([checkpointed])],
            checkpoints=[SimpleNamespace(id="input-0-checkpoint-a", status=SimpleNamespace(name="Checkpointed"))],
        ),
        idempotence_key="run-1",
    )
    # Creating the sink opens the current Lance dataset to validate the target
    # table shape.
    with patch("daft_lance.lance_data_sink.lance.dataset", return_value=dataset_before):
        sink = LanceDataSink(tmp_path / "tbl", _schema(), "append", checkpoint=checkpoint)

    # finalize() opens dataset_before, sees no idempotence key in history,
    # builds a Lance Append operation from checkpointed fragments, then commits
    # it.
    with (
        patch("daft_lance.lance_data_sink.lance.dataset", return_value=dataset_before),
        patch("daft_lance.lance_data_sink.lance.LanceOperation.Append", return_value="append-op"),
        patch("daft_lance.lance_data_sink.lance.LanceDataset.commit", return_value=dataset_after) as commit,
    ):
        result = sink.finalize([])

    # LanceDataset.commit(table_uri, transaction, ...) receives the transaction
    # as its second positional argument. It should carry the Daft idempotence key.
    committed_transaction = commit.call_args.args[1]
    assert committed_transaction.transaction_properties == {"daft.idempotence-key": "run-1"}
    assert result.to_pydict()["version"] == [5]
    # After committing the recovered fragments to Lance, finalize marks the
    # sealed checkpoint entry committed so future retries do not process it.
    checkpoint.store.mark_committed.assert_called_once_with(["input-0-checkpoint-a"])


def test_checkpoint_finalize_recovers_after_commit_before_mark_committed(tmp_path):
    """Recover when Lance commit succeeds but mark_committed fails.

    The first finalize appends to Lance and then crashes during
    mark_committed(). The second finalize finds the idempotence key in Lance history and
    must not append the fragments again.
    """
    fragment = SimpleNamespace(path="/tmp/frag", rows=3)
    checkpointed = WriteResult(result=[fragment], bytes_written=128, rows_written=3)
    dataset_before = _existing_dataset(version=4)
    dataset_before.get_transactions = Mock(return_value=[])
    committed_tx = SimpleNamespace(transaction_properties={"daft.idempotence-key": "run-1"})
    dataset_after = _existing_dataset(version=5)
    dataset_after.get_transactions = Mock(return_value=[committed_tx])
    checkpoint = SimpleNamespace(
        store=FakeCheckpointStore(
            files=[_checkpoint_file([checkpointed])],
            checkpoints=[SimpleNamespace(id="input-0-checkpoint-a", status=SimpleNamespace(name="Checkpointed"))],
        ),
        idempotence_key="run-1",
    )
    # The first finalize will commit to Lance, then fail while calling
    # mark_committed(). The retry should use Lance history to avoid appending
    # the same fragments again.
    checkpoint.store.mark_committed.side_effect = [RuntimeError("simulated crash"), None]

    # Creating the sink opens the pre-commit dataset, just like the real
    # driver would before finalize() runs.
    with patch("daft_lance.lance_data_sink.lance.dataset", return_value=dataset_before):
        sink = LanceDataSink(tmp_path / "tbl", _schema(), "append", checkpoint=checkpoint)

    # First attempt: open the pre-commit dataset, create an Append operation,
    # and make LanceDataset.commit return dataset_after. mark_committed above
    # then raises to simulate a crash after the Lance commit succeeded.
    with (
        patch("daft_lance.lance_data_sink.lance.dataset", return_value=dataset_before),
        patch("daft_lance.lance_data_sink.lance.LanceOperation.Append", return_value="append-op"),
        patch("daft_lance.lance_data_sink.lance.LanceDataset.commit", return_value=dataset_after) as commit,
    ):
        with pytest.raises(RuntimeError, match="simulated crash"):
            sink.finalize([])

    # Retry attempt: open the post-commit dataset. Its transaction history
    # contains the idempotence key, so finalize should call mark_committed()
    # without calling LanceDataset.commit again.
    with (
        patch("daft_lance.lance_data_sink.lance.dataset", return_value=dataset_after),
        patch("daft_lance.lance_data_sink.lance.LanceDataset.commit") as retry_commit,
    ):
        result = sink.finalize([])

    assert result.to_pydict()["version"] == [5]
    # The first finalize made the Lance commit before mark_committed failed.
    # The retry should see the idempotence key and avoid a second append.
    commit.assert_called_once()
    retry_commit.assert_not_called()
    # mark_committed is attempted once in the failed run and once in the retry.
    assert checkpoint.store.mark_committed.call_count == 2


def test_checkpoint_finalize_requires_staged_write_results(tmp_path):
    """Current write_results are not enough in checkpoint mode.

    If the current run produced fragments but the checkpoint store has no
    sealed write_results, committing would not be recoverable after a crash.
    Finalize should fail with a clear error.
    """
    current_write_result = WriteResult(result=["fragment"], bytes_written=1, rows_written=1)
    checkpoint = SimpleNamespace(
        store=FakeCheckpointStore(
            files=[],
            checkpoints=[SimpleNamespace(id="input-0-checkpoint-a", status=SimpleNamespace(name="Checkpointed"))],
        ),
        idempotence_key="run-1",
    )
    dataset = _existing_dataset(version=4)
    dataset.get_transactions = Mock(return_value=[])

    # This checkpoint store has no staged files. Even if the current run has
    # a write_result in memory, finalize must reject it because crash recovery
    # depends on the store copy.
    with patch("daft_lance.lance_data_sink.lance.dataset", return_value=dataset):
        sink = LanceDataSink(tmp_path / "tbl", _schema(), "append", checkpoint=checkpoint)

    with patch("daft_lance.lance_data_sink.lance.dataset", return_value=dataset):
        with pytest.raises(RuntimeError, match="did not stage any Lance write_results"):
            sink.finalize([current_write_result])


def test_checkpoint_finalize_marks_empty_fragment_results_committed(tmp_path):
    """A sealed checkpoint can have write_results but no fragments to append.

    This covers the branch where the store is wired correctly, but the sink did
    not produce any Lance fragments. There is no Lance commit to make, but the
    pending checkpoint ids should still be marked committed.
    """
    checkpointed = WriteResult(result=[], bytes_written=0, rows_written=0)
    dataset = _existing_dataset(version=4)
    dataset.get_transactions = Mock(return_value=[])
    checkpoint = SimpleNamespace(
        store=FakeCheckpointStore(
            files=[_checkpoint_file([checkpointed])],
            checkpoints=[SimpleNamespace(id="input-0-checkpoint-a", status=SimpleNamespace(name="Checkpointed"))],
        ),
        idempotence_key="run-1",
    )

    # The store is wired correctly, but the staged write_results contain an
    # empty fragment list. Patch commit so the test can prove no Lance append
    # is attempted for that no-op payload.
    with patch("daft_lance.lance_data_sink.lance.dataset", return_value=dataset):
        sink = LanceDataSink(tmp_path / "tbl", _schema(), "append", checkpoint=checkpoint)

    with (
        patch("daft_lance.lance_data_sink.lance.dataset", return_value=dataset),
        patch("daft_lance.lance_data_sink.lance.LanceDataset.commit") as commit,
    ):
        result = sink.finalize([])

    assert result.to_pydict()["version"] == [4]
    # Empty fragment results mean there is no Lance append to make. Finalize
    # still marks the pending checkpoint entry committed so it is not retried
    # forever.
    checkpoint.store.mark_committed.assert_called_once_with(["input-0-checkpoint-a"])
    commit.assert_not_called()


@pytest.mark.skipif(os.environ.get("DAFT_RUNNER") != "ray", reason="checkpoint source filtering requires Ray")
def test_write_lance_checkpoint_append_retry_e2e(tmp_path):
    """A normal append retry with the same idempotence key is a no-op.

    First run writes rows and marks checkpoint entries committed. The second
    run reuses the same store and key, so Lance should not receive duplicate
    rows.
    """
    import lance

    input_dir = tmp_path / "input"
    table_uri = tmp_path / "table.lance"
    checkpoint_uri = f"file://{tmp_path / 'ckpt'}"
    input_dir.mkdir()

    pq.write_table(pa.table({"file_id": ["a", "b", "c"], "x": [1, 2, 3]}), input_dir / "part.parquet")
    lance.write_dataset(pa.table({"file_id": ["seed"], "x": [0]}), table_uri)

    store = daft.CheckpointStore(checkpoint_uri)
    commit = daft.IdempotentCommit(store=store, idempotence_key="run-1")

    daft.read_parquet(str(input_dir), checkpoint=daft.CheckpointConfig(store=store, on="file_id")).write_lance(
        table_uri,
        mode="append",
        checkpoint=commit,
    )

    rows = lance.dataset(table_uri).to_table().to_pydict()
    assert sorted(rows["file_id"]) == ["a", "b", "c", "seed"]

    daft.read_parquet(str(input_dir), checkpoint=daft.CheckpointConfig(store=store, on="file_id")).write_lance(
        table_uri,
        mode="append",
        checkpoint=commit,
    )

    rows_after_retry = lance.dataset(table_uri).to_table().to_pydict()
    assert sorted(rows_after_retry["file_id"]) == ["a", "b", "c", "seed"]


@pytest.mark.skipif(os.environ.get("DAFT_RUNNER") != "ray", reason="checkpoint source filtering requires Ray")
def test_write_lance_checkpoint_requires_source_checkpoint_e2e(tmp_path):
    """A sink checkpoint without a source checkpoint is not recoverable.

    The sink can produce current write_results, but there is no
    StageCheckpointKeys operator and no shared checkpoint_id to seal those
    write_results into the store. Finalize must fail instead of committing an
    append that could not be recovered after a crash.
    """
    import lance

    input_dir = tmp_path / "input"
    table_uri = tmp_path / "table.lance"
    checkpoint_uri = f"file://{tmp_path / 'ckpt'}"
    input_dir.mkdir()

    pq.write_table(pa.table({"file_id": ["a", "b", "c"], "x": [1, 2, 3]}), input_dir / "part.parquet")
    lance.write_dataset(pa.table({"file_id": ["seed"], "x": [0]}), table_uri)

    store = daft.CheckpointStore(checkpoint_uri)
    commit = daft.IdempotentCommit(store=store, idempotence_key="run-missing-source-checkpoint")

    with pytest.raises(RuntimeError, match="did not stage any Lance write_results"):
        daft.read_parquet(str(input_dir)).write_lance(
            table_uri,
            mode="append",
            checkpoint=commit,
        )

    rows = lance.dataset(table_uri).to_table().to_pydict()
    assert sorted(rows["file_id"]) == ["seed"]


@pytest.mark.skipif(os.environ.get("DAFT_RUNNER") != "ray", reason="checkpoint source filtering requires Ray")
def test_write_lance_checkpoint_recovers_after_commit_before_mark_committed(tmp_path):
    """End-to-end recovery for commit succeeded, mark_committed failed.

    The first run appends rows to Lance and then crashes during
    mark_committed(). The retry uses a marker UDF that would fail if source rows were
    recomputed, proving the checkpoint filter skips already sealed keys.
    """
    import lance

    input_dir = tmp_path / "input"
    table_uri = tmp_path / "table.lance"
    checkpoint_uri = f"file://{tmp_path / 'ckpt'}"
    input_dir.mkdir()

    pq.write_table(pa.table({"file_id": ["a", "b", "c"], "x": [1, 2, 3]}), input_dir / "part.parquet")
    lance.write_dataset(pa.table({"file_id": ["seed"], "x": [0]}), table_uri)

    store = daft.CheckpointStore(checkpoint_uri)
    commit = daft.IdempotentCommit(store=store, idempotence_key="run-commit-before-mark")

    retry_marker = tmp_path / "fail-on-recompute"

    # The marker is created only before retry. If checkpoint filtering fails
    # and these source rows run again, this UDF fails the test.
    def fail_if_recomputed(value: int) -> int:
        if retry_marker.exists():
            raise AssertionError("retry should not recompute checkpointed rows")
        return value

    df = daft.read_parquet(str(input_dir), checkpoint=daft.CheckpointConfig(store=store, on="file_id"))
    df = df.with_column(
        "x",
        df["x"].apply(fail_if_recomputed, daft.DataType.int64()),
    )
    with patch.object(store, "mark_committed", side_effect=RuntimeError("simulated crash")):
        with pytest.raises(RuntimeError, match="simulated crash"):
            df.write_lance(table_uri, mode="append", checkpoint=commit)

    assert _pending_checkpoints(store)
    rows_after_crash = lance.dataset(table_uri).to_table().to_pydict()
    assert sorted(rows_after_crash["file_id"]) == ["a", "b", "c", "seed"]

    retry_marker.touch()
    retry_df = daft.read_parquet(str(input_dir), checkpoint=daft.CheckpointConfig(store=store, on="file_id"))
    retry_df.with_column(
        "x",
        retry_df["x"].apply(fail_if_recomputed, daft.DataType.int64()),
    ).write_lance(
        table_uri,
        mode="append",
        checkpoint=commit,
    )

    assert not _pending_checkpoints(store)
    rows_after_retry = lance.dataset(table_uri).to_table().to_pydict()
    assert sorted(rows_after_retry["file_id"]) == ["a", "b", "c", "seed"]


@pytest.mark.skipif(os.environ.get("DAFT_RUNNER") != "ray", reason="checkpoint source filtering requires Ray")
def test_write_lance_checkpoint_recovers_after_stage_before_commit(tmp_path):
    """End-to-end recovery for stage/seal succeeded, Lance commit failed.

    After the crash, Lance still has only the seed row, but the checkpoint
    store has pending write_results. The retry must commit those stored
    fragments without recomputing the source rows.
    """
    import lance

    input_dir = tmp_path / "input"
    table_uri = tmp_path / "table.lance"
    checkpoint_uri = f"file://{tmp_path / 'ckpt'}"
    input_dir.mkdir()

    pq.write_table(pa.table({"file_id": ["a", "b", "c"], "x": [1, 2, 3]}), input_dir / "part.parquet")
    lance.write_dataset(pa.table({"file_id": ["seed"], "x": [0]}), table_uri)

    store = daft.CheckpointStore(checkpoint_uri)
    commit = daft.IdempotentCommit(store=store, idempotence_key="run-stage-before-commit")

    retry_marker = tmp_path / "fail-on-recompute"

    # The marker is created only before retry. If checkpoint filtering fails
    # and these source rows run again, this UDF fails the test.
    def fail_if_recomputed(value: int) -> int:
        if retry_marker.exists():
            raise AssertionError("retry should not recompute checkpointed rows")
        return value

    df = daft.read_parquet(str(input_dir), checkpoint=daft.CheckpointConfig(store=store, on="file_id"))
    df = df.with_column(
        "x",
        df["x"].apply(fail_if_recomputed, daft.DataType.int64()),
    )
    with patch("daft_lance.lance_data_sink.lance.LanceDataset.commit", side_effect=RuntimeError("simulated crash")):
        with pytest.raises(RuntimeError, match="simulated crash"):
            df.write_lance(table_uri, mode="append", checkpoint=commit)

    assert _pending_checkpoints(store)
    rows_after_crash = lance.dataset(table_uri).to_table().to_pydict()
    assert sorted(rows_after_crash["file_id"]) == ["seed"]

    retry_marker.touch()
    retry_df = daft.read_parquet(str(input_dir), checkpoint=daft.CheckpointConfig(store=store, on="file_id"))
    retry_df.with_column(
        "x",
        retry_df["x"].apply(fail_if_recomputed, daft.DataType.int64()),
    ).write_lance(
        table_uri,
        mode="append",
        checkpoint=commit,
    )

    assert not _pending_checkpoints(store)
    rows_after_retry = lance.dataset(table_uri).to_table().to_pydict()
    assert sorted(rows_after_retry["file_id"]) == ["a", "b", "c", "seed"]
