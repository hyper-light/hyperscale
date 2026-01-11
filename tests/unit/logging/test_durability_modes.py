import os
import tempfile

import pytest

from hyperscale.logging.config.durability_mode import DurabilityMode
from hyperscale.logging.models import Entry, LogLevel
from hyperscale.logging.streams.logger_stream import LoggerStream


class TestDurabilityModeEnum:
    def test_durability_mode_values(self):
        assert DurabilityMode.NONE == 0
        assert DurabilityMode.FLUSH == 1
        assert DurabilityMode.FSYNC == 2
        assert DurabilityMode.FSYNC_BATCH == 3

    def test_durability_mode_ordering(self):
        assert DurabilityMode.NONE < DurabilityMode.FLUSH
        assert DurabilityMode.FLUSH < DurabilityMode.FSYNC
        assert DurabilityMode.FSYNC < DurabilityMode.FSYNC_BATCH

    def test_durability_mode_is_intenum(self):
        assert isinstance(DurabilityMode.FLUSH, int)
        assert DurabilityMode.FLUSH + 1 == DurabilityMode.FSYNC


class TestDurabilityModeDefaults:
    @pytest.mark.asyncio
    async def test_default_durability_is_flush(self, temp_log_directory: str):
        stream = LoggerStream(
            name="test_default",
            filename="test.json",
            directory=temp_log_directory,
        )
        assert stream._durability == DurabilityMode.FLUSH

    @pytest.mark.asyncio
    async def test_default_log_format_is_json(self, temp_log_directory: str):
        stream = LoggerStream(
            name="test_format",
            filename="test.json",
            directory=temp_log_directory,
        )
        assert stream._log_format == "json"

    @pytest.mark.asyncio
    async def test_default_lsn_disabled(self, temp_log_directory: str):
        stream = LoggerStream(
            name="test_lsn",
            filename="test.json",
            directory=temp_log_directory,
        )
        assert stream._enable_lsn is False
        assert stream._sequence_generator is None


class TestDurabilityModeNone:
    @pytest.mark.asyncio
    async def test_durability_none_no_sync(self, temp_log_directory: str):
        stream = LoggerStream(
            name="test_none",
            filename="test.json",
            directory=temp_log_directory,
            durability=DurabilityMode.NONE,
            log_format="json",
        )
        await stream.initialize()

        entry = Entry(message="test message", level=LogLevel.INFO)
        await stream.log(entry)

        log_path = os.path.join(temp_log_directory, "test.json")
        assert os.path.exists(log_path)

        await stream.close()


class TestDurabilityModeFlush:
    @pytest.mark.asyncio
    async def test_durability_flush_writes_immediately(
        self,
        json_logger_stream: LoggerStream,
        sample_entry: Entry,
        temp_log_directory: str,
    ):
        await json_logger_stream.log(sample_entry)

        log_path = os.path.join(temp_log_directory, "test.json")
        with open(log_path, "rb") as log_file:
            content = log_file.read()

        assert len(content) > 0
        assert b"Test log message" in content


class TestDurabilityModeFsync:
    @pytest.mark.asyncio
    async def test_durability_fsync_writes_to_disk(
        self,
        fsync_logger_stream: LoggerStream,
        sample_entry: Entry,
        temp_log_directory: str,
    ):
        await fsync_logger_stream.log(sample_entry)

        log_path = os.path.join(temp_log_directory, "test_fsync.wal")
        assert os.path.exists(log_path)

        with open(log_path, "rb") as log_file:
            content = log_file.read()
        assert len(content) > 0


class TestDurabilityModeFsyncBatch:
    @pytest.mark.asyncio
    async def test_durability_fsync_batch_creates_pending_batch(
        self,
        batch_fsync_logger_stream: LoggerStream,
        sample_entry: Entry,
    ):
        await batch_fsync_logger_stream.log(sample_entry)
        assert batch_fsync_logger_stream._batch_lock is not None
