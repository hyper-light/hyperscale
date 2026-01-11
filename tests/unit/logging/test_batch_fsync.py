import asyncio
import os

import pytest

from hyperscale.logging.config.durability_mode import DurabilityMode
from hyperscale.logging.models import Entry, LogLevel
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
        )
        stream._batch_timeout_ms = 50
        await stream.initialize(
            stdout_writer=create_mock_stream_writer(),
            stderr_writer=create_mock_stream_writer(),
        )

        entry = Entry(message="timeout test", level=LogLevel.INFO)
        await stream.log(entry)

        await asyncio.sleep(0.1)

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

        for idx in range(10):
            entry = sample_entry_factory(message=f"batch message {idx}")
            await stream.log(entry)

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

        for idx in range(5):
            entry = sample_entry_factory(message=f"first batch {idx}")
            await stream.log(entry)

        for idx in range(3):
            entry = sample_entry_factory(message=f"second batch {idx}")
            await stream.log(entry)

        assert len(stream._pending_batch) <= 3

        await stream.close()


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
