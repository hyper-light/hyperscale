import os
import time

import pytest

from hyperscale.logging.config.durability_mode import DurabilityMode
from hyperscale.logging.models import Entry, LogLevel
from hyperscale.logging.streams.logger_stream import LoggerStream

from .conftest import create_mock_stream_writer


class TestGetLastLsnBasic:
    @pytest.mark.asyncio
    async def test_get_last_lsn_returns_none_for_empty_file(
        self,
        json_logger_stream: LoggerStream,
        temp_log_directory: str,
    ):
        log_path = os.path.join(temp_log_directory, "empty.json")
        with open(log_path, "w") as empty_file:
            pass

        last_lsn = await json_logger_stream.get_last_lsn(log_path)
        assert last_lsn is None

    @pytest.mark.asyncio
    async def test_get_last_lsn_returns_none_for_nonexistent_file(
        self,
        json_logger_stream: LoggerStream,
        temp_log_directory: str,
    ):
        log_path = os.path.join(temp_log_directory, "nonexistent.json")
        last_lsn = await json_logger_stream.get_last_lsn(log_path)
        assert last_lsn is None

    @pytest.mark.asyncio
    async def test_get_last_lsn_single_entry_json(
        self,
        json_logger_stream: LoggerStream,
        sample_entry: Entry,
        temp_log_directory: str,
    ):
        written_lsn = await json_logger_stream.log(sample_entry)

        log_path = os.path.join(temp_log_directory, "test.json")
        last_lsn = await json_logger_stream.get_last_lsn(log_path)

        assert last_lsn == written_lsn

    @pytest.mark.asyncio
    async def test_get_last_lsn_single_entry_binary(
        self,
        binary_logger_stream: LoggerStream,
        sample_entry: Entry,
        temp_log_directory: str,
    ):
        written_lsn = await binary_logger_stream.log(sample_entry)

        log_path = os.path.join(temp_log_directory, "test.wal")
        last_lsn = await binary_logger_stream.get_last_lsn(log_path)

        assert last_lsn == written_lsn


class TestGetLastLsnMultipleEntries:
    @pytest.mark.asyncio
    async def test_get_last_lsn_multiple_entries_json(
        self,
        json_logger_stream: LoggerStream,
        sample_entry_factory,
        temp_log_directory: str,
    ):
        written_lsns = []
        for idx in range(5):
            entry = sample_entry_factory(message=f"message {idx}")
            lsn = await json_logger_stream.log(entry)
            written_lsns.append(lsn)
            time.sleep(0.001)

        log_path = os.path.join(temp_log_directory, "test.json")
        last_lsn = await json_logger_stream.get_last_lsn(log_path)

        assert last_lsn == written_lsns[-1]

    @pytest.mark.asyncio
    async def test_get_last_lsn_multiple_entries_binary(
        self,
        binary_logger_stream: LoggerStream,
        sample_entry_factory,
        temp_log_directory: str,
    ):
        written_lsns = []
        for idx in range(5):
            entry = sample_entry_factory(message=f"message {idx}")
            lsn = await binary_logger_stream.log(entry)
            written_lsns.append(lsn)
            time.sleep(0.001)

        log_path = os.path.join(temp_log_directory, "test.wal")
        last_lsn = await binary_logger_stream.get_last_lsn(log_path)

        assert last_lsn == written_lsns[-1]


class TestGetLastLsnRecovery:
    @pytest.mark.asyncio
    async def test_recovery_after_crash_simulation(
        self,
        temp_log_directory: str,
        sample_entry_factory,
    ):
        stream1 = LoggerStream(
            name="original",
            filename="recovery.wal",
            directory=temp_log_directory,
            durability=DurabilityMode.FSYNC,
            log_format="binary",
            enable_lsn=True,
            instance_id=1,
        )
        await stream1.initialize(
            stdout_writer=create_mock_stream_writer(),
            stderr_writer=create_mock_stream_writer(),
        )

        written_lsns = []
        for idx in range(10):
            entry = sample_entry_factory(message=f"pre-crash message {idx}")
            lsn = await stream1.log(entry)
            written_lsns.append(lsn)

        await stream1.close()

        stream2 = LoggerStream(
            name="recovery",
            filename="recovery.wal",
            directory=temp_log_directory,
            durability=DurabilityMode.FSYNC,
            log_format="binary",
            enable_lsn=True,
            instance_id=1,
        )
        await stream2.initialize(
            stdout_writer=create_mock_stream_writer(),
            stderr_writer=create_mock_stream_writer(),
        )

        log_path = os.path.join(temp_log_directory, "recovery.wal")
        last_lsn = await stream2.get_last_lsn(log_path)

        assert last_lsn == written_lsns[-1]
        await stream2.close()

    @pytest.mark.asyncio
    async def test_continue_from_last_lsn(
        self,
        temp_log_directory: str,
        sample_entry_factory,
    ):
        stream1 = LoggerStream(
            name="original",
            filename="continue.json",
            directory=temp_log_directory,
            durability=DurabilityMode.FLUSH,
            log_format="json",
            enable_lsn=True,
            instance_id=1,
        )
        await stream1.initialize(
            stdout_writer=create_mock_stream_writer(),
            stderr_writer=create_mock_stream_writer(),
        )

        for idx in range(5):
            entry = sample_entry_factory(message=f"first batch {idx}")
            await stream1.log(entry)
            time.sleep(0.001)

        await stream1.close()

        log_path = os.path.join(temp_log_directory, "continue.json")

        stream2 = LoggerStream(
            name="continuation",
            filename="continue.json",
            directory=temp_log_directory,
            durability=DurabilityMode.FLUSH,
            log_format="json",
            enable_lsn=True,
            instance_id=1,
        )
        await stream2.initialize(
            stdout_writer=create_mock_stream_writer(),
            stderr_writer=create_mock_stream_writer(),
        )

        last_lsn_before = await stream2.get_last_lsn(log_path)

        for idx in range(5):
            entry = sample_entry_factory(message=f"second batch {idx}")
            await stream2.log(entry)
            time.sleep(0.001)

        last_lsn_after = await stream2.get_last_lsn(log_path)

        assert last_lsn_after is not None
        assert last_lsn_before is not None
        assert last_lsn_after > last_lsn_before
        await stream2.close()


class TestGetLastLsnWithoutLsnEnabled:
    @pytest.mark.asyncio
    async def test_get_last_lsn_returns_none_when_lsn_disabled(
        self,
        no_lsn_logger_stream: LoggerStream,
        sample_entry: Entry,
        temp_log_directory: str,
    ):
        await no_lsn_logger_stream.log(sample_entry)

        log_path = os.path.join(temp_log_directory, "test_no_lsn.json")
        last_lsn = await no_lsn_logger_stream.get_last_lsn(log_path)

        assert last_lsn is None
