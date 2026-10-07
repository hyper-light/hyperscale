import asyncio
import os

import pytest

from hyperscale.core.runtime.real_filesystem import RealFilesystem
from hyperscale.logging.config.durability_mode import DurabilityMode
from hyperscale.logging.models import Entry, LogLevel
from hyperscale.logging.exceptions import WALWriteError
from hyperscale.logging.streams.logger_stream import LoggerStream

from .conftest import create_mock_stream_writer


class TestBatchFsyncScheduling:
    @pytest.mark.asyncio
    async def test_batch_lock_created_on_first_log(
        self,
        batch_fsync_logger_stream: LoggerStream,
        sample_entry: Entry,
    ):
        assert batch_fsync_logger_stream._batch_lock is None

        await batch_fsync_logger_stream.log(sample_entry)

        assert batch_fsync_logger_stream._batch_lock is not None

    @pytest.mark.asyncio
    async def test_timer_handle_created_on_first_log(
        self,
        batch_fsync_logger_stream: LoggerStream,
        sample_entry: Entry,
    ):
        await batch_fsync_logger_stream.log(sample_entry)

        assert (
            batch_fsync_logger_stream._batch_timer_handle is not None
            or batch_fsync_logger_stream._batch_flush_task is not None
            or len(batch_fsync_logger_stream._pending_batch) == 0
        )


class TestBatchFsyncTimeout:
    @pytest.mark.asyncio
    async def test_batch_flushes_after_timeout(
        self,
        temp_log_directory: str,
    ):
        stream = LoggerStream(
            name="test_timeout",
            filename="timeout_test.wal",
            directory=temp_log_directory,
            durability=DurabilityMode.FSYNC_BATCH,
            log_format="binary",
            enable_lsn=True,
            instance_id=1,
            batch_timeout_ms=50,
        )
        await stream.initialize(
            stdout_writer=create_mock_stream_writer(),
            stderr_writer=create_mock_stream_writer(),
        )

        entry = Entry(message="timeout test", level=LogLevel.INFO)
        await stream.log(entry)

        await asyncio.sleep(0.1)

        assert len(stream._pending_batch) == 0

        await stream.close()

    @pytest.mark.asyncio
    async def test_configured_batch_timeout_bounds_a_lone_entry_durability(
        self,
        temp_log_directory: str,
    ):
        """A lone FSYNC_BATCH entry is durable only once the configured
        batch timeout fires: log() returns no sooner than it, and the
        default (10 ms) would return far sooner."""
        configured_timeout_seconds = 0.3
        stream = LoggerStream(
            name="test_configured_timeout",
            filename="configured_timeout_test.wal",
            directory=temp_log_directory,
            durability=DurabilityMode.FSYNC_BATCH,
            log_format="binary",
            enable_lsn=True,
            instance_id=1,
            batch_timeout_ms=configured_timeout_seconds * 1000.0,
        )
        await stream.initialize(
            stdout_writer=create_mock_stream_writer(),
            stderr_writer=create_mock_stream_writer(),
        )
        event_loop = asyncio.get_running_loop()

        started_at = event_loop.time()
        await stream.log(Entry(message="lone entry", level=LogLevel.INFO))
        durable_after_seconds = event_loop.time() - started_at

        assert durable_after_seconds >= configured_timeout_seconds * 0.9
        assert len(stream._pending_batch) == 0

        await stream.close()


class TestBatchFsyncMaxSize:
    @pytest.mark.asyncio
    async def test_batch_flushes_at_max_size(
        self,
        temp_log_directory: str,
        sample_entry_factory,
    ):
        stream = LoggerStream(
            name="test_max_size",
            filename="max_size_test.wal",
            directory=temp_log_directory,
            durability=DurabilityMode.FSYNC_BATCH,
            log_format="binary",
            enable_lsn=True,
            instance_id=1,
        )
        stream._batch_max_size = 10
        stream._batch_timeout_ms = 60000
        await stream.initialize(
            stdout_writer=create_mock_stream_writer(),
            stderr_writer=create_mock_stream_writer(),
        )

        # Group commit batches concurrent writers: the tenth fills the
        # batch and syncs it, long before the timer would.
        await asyncio.wait_for(
            asyncio.gather(
                *(stream.log(sample_entry_factory(message=f"batch message {idx}")) for idx in range(10))
            ),
            timeout=stream._batch_timeout_ms / 1000.0 / 2,
        )

        assert len(stream._pending_batch) == 0

        await stream.close()

    @pytest.mark.asyncio
    async def test_batch_size_resets_after_flush(
        self,
        temp_log_directory: str,
        sample_entry_factory,
    ):
        stream = LoggerStream(
            name="test_reset",
            filename="reset_test.wal",
            directory=temp_log_directory,
            durability=DurabilityMode.FSYNC_BATCH,
            log_format="binary",
            enable_lsn=True,
            instance_id=1,
        )
        stream._batch_max_size = 5
        stream._batch_timeout_ms = 60000
        await stream.initialize(
            stdout_writer=create_mock_stream_writer(),
            stderr_writer=create_mock_stream_writer(),
        )

        await asyncio.gather(
            *(stream.log(sample_entry_factory(message=f"first batch {idx}")) for idx in range(5))
        )
        assert len(stream._pending_batch) == 0

        second_batch = asyncio.gather(
            *(stream.log(sample_entry_factory(message=f"second batch {idx}")) for idx in range(3))
        )
        while len(stream._pending_batch) < 3:
            await asyncio.sleep(0)
        # The second batch starts empty and holds only its own writers,
        # which wait on its sync -- here, the one closing performs.
        assert len(stream._pending_batch) == 3 and not second_batch.done()

        await stream.close()
        await second_batch


class TestBatchFsyncWithOtherModes:
    @pytest.mark.asyncio
    async def test_no_batching_with_fsync_mode(
        self,
        fsync_logger_stream: LoggerStream,
        sample_entry: Entry,
    ):
        await fsync_logger_stream.log(sample_entry)

        assert len(fsync_logger_stream._pending_batch) == 0

    @pytest.mark.asyncio
    async def test_no_batching_with_flush_mode(
        self,
        json_logger_stream: LoggerStream,
        sample_entry: Entry,
    ):
        await json_logger_stream.log(sample_entry)

        assert len(json_logger_stream._pending_batch) == 0

    @pytest.mark.asyncio
    async def test_no_batching_with_none_mode(
        self,
        temp_log_directory: str,
    ):
        stream = LoggerStream(
            name="test_none",
            filename="none_test.json",
            directory=temp_log_directory,
            durability=DurabilityMode.NONE,
            log_format="json",
        )
        await stream.initialize(
            stdout_writer=create_mock_stream_writer(),
            stderr_writer=create_mock_stream_writer(),
        )

        entry = Entry(message="no batching", level=LogLevel.INFO)
        await stream.log(entry)

        assert len(stream._pending_batch) == 0

        await stream.close()


class TestBatchFsyncDataIntegrity:
    @pytest.mark.asyncio
    async def test_all_entries_written_with_batch_fsync(
        self,
        temp_log_directory: str,
        sample_entry_factory,
    ):
        stream = LoggerStream(
            name="test_integrity",
            filename="integrity_test.wal",
            directory=temp_log_directory,
            durability=DurabilityMode.FSYNC_BATCH,
            log_format="binary",
            enable_lsn=True,
            instance_id=1,
        )
        stream._batch_max_size = 5
        await stream.initialize(
            stdout_writer=create_mock_stream_writer(),
            stderr_writer=create_mock_stream_writer(),
        )

        written_lsns = []
        for idx in range(12):
            entry = sample_entry_factory(message=f"integrity message {idx}")
            lsn = await stream.log(entry)
            written_lsns.append(lsn)

        await asyncio.sleep(0.05)
        await stream.close()

        read_stream = LoggerStream(
            name="test_read",
            filename="integrity_test.wal",
            directory=temp_log_directory,
            durability=DurabilityMode.FLUSH,
            log_format="binary",
            enable_lsn=True,
            instance_id=1,
        )
        await read_stream.initialize(
            stdout_writer=create_mock_stream_writer(),
            stderr_writer=create_mock_stream_writer(),
        )

        log_path = os.path.join(temp_log_directory, "integrity_test.wal")
        read_lsns = []
        async for offset, log, lsn in read_stream.read_entries(log_path):
            read_lsns.append(lsn)

        assert len(read_lsns) == 12
        assert read_lsns == written_lsns

        await read_stream.close()


class RecordingSyncFilesystem(RealFilesystem):
    """A real filesystem that records each handle it syncs."""

    __slots__ = ("synced_handles",)

    def __init__(self) -> None:
        super().__init__()
        self.synced_handles: list[object] = []

    async def fsync(self, handle) -> None:
        self.synced_handles.append(handle)
        await super().fsync(handle)


class FailingSyncFilesystem(RealFilesystem):
    """A real filesystem whose syncs fail as a dying disk's do."""

    __slots__ = ()

    async def fsync(self, handle) -> None:
        raise OSError(5, "Input/output error")


class TestBatchFsyncDurability:
    @pytest.mark.asyncio
    async def test_a_write_returns_only_once_its_batch_has_synced(
        self,
        temp_log_directory: str,
        sample_entry_factory,
    ):
        filesystem = RecordingSyncFilesystem()
        stream = LoggerStream(
            name="test_durable_return",
            filename="durable_return.wal",
            directory=temp_log_directory,
            durability=DurabilityMode.FSYNC_BATCH,
            log_format="binary",
            enable_lsn=True,
            instance_id=1,
            filesystem=filesystem,
        )
        stream._batch_timeout_ms = 60000
        await stream.initialize(
            stdout_writer=create_mock_stream_writer(),
            stderr_writer=create_mock_stream_writer(),
        )
        write = asyncio.ensure_future(stream.log(sample_entry_factory(message="durable")))
        # Long enough for an unsynced write to return many times over, far
        # inside the batch timer: a durable write is still waiting.
        await asyncio.wait({write}, timeout=stream._batch_timeout_ms / 1000.0 / 1000)
        assert stream._pending_batch
        assert not write.done() and filesystem.synced_handles == []

        await stream._flush_batch(stream._pending_batch[0][0])
        await write

        assert len(filesystem.synced_handles) == 1
        await stream.close()
        filesystem.shutdown()

    @pytest.mark.asyncio
    async def test_a_failed_sync_fails_the_writes_waiting_on_it(
        self,
        temp_log_directory: str,
        sample_entry_factory,
    ):
        filesystem = FailingSyncFilesystem()
        stream = LoggerStream(
            name="test_failed_sync",
            filename="failed_sync.wal",
            directory=temp_log_directory,
            durability=DurabilityMode.FSYNC_BATCH,
            log_format="binary",
            enable_lsn=True,
            instance_id=1,
            filesystem=filesystem,
        )
        stream._batch_max_size = 3
        await stream.initialize(
            stdout_writer=create_mock_stream_writer(),
            stderr_writer=create_mock_stream_writer(),
        )

        outcomes = await asyncio.wait_for(
            asyncio.gather(
                *(stream.log(sample_entry_factory(message=f"doomed {idx}")) for idx in range(3)),
                return_exceptions=True,
            ),
            timeout=stream._batch_timeout_ms / 1000.0 * 10,
        )

        assert all(isinstance(outcome, WALWriteError) for outcome in outcomes), outcomes
        assert stream._pending_batch == []
        await stream.close()
        filesystem.shutdown()
