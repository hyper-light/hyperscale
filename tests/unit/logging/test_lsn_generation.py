import os
import time

import pytest

from hyperscale.logging.config.durability_mode import DurabilityMode
from hyperscale.logging.models import Entry, LogLevel
from hyperscale.logging.snowflake import SnowflakeGenerator
from hyperscale.logging.streams.logger_stream import LoggerStream

from .conftest import create_mock_stream_writer


class TestSnowflakeGeneratorIntegration:
    def test_snowflake_generator_created_when_lsn_enabled(
        self,
        temp_log_directory: str,
    ):
        stream = LoggerStream(
            name="test_lsn",
            filename="test.json",
            directory=temp_log_directory,
            enable_lsn=True,
            instance_id=5,
        )
        assert stream._sequence_generator is not None
        assert isinstance(stream._sequence_generator, SnowflakeGenerator)

    def test_snowflake_generator_not_created_when_lsn_disabled(
        self,
        temp_log_directory: str,
    ):
        stream = LoggerStream(
            name="test_no_lsn",
            filename="test.json",
            directory=temp_log_directory,
            enable_lsn=False,
        )
        assert stream._sequence_generator is None

    def test_snowflake_generator_uses_instance_id(
        self,
        temp_log_directory: str,
    ):
        instance_id = 42
        stream = LoggerStream(
            name="test_instance",
            filename="test.json",
            directory=temp_log_directory,
            enable_lsn=True,
            instance_id=instance_id,
        )

        assert stream._sequence_generator is not None
        lsn = stream._sequence_generator.generate()
        assert lsn is not None

        extracted_instance = (lsn >> 12) & 0x3FF
        assert extracted_instance == instance_id


class TestLSNGeneration:
    @pytest.mark.asyncio
    async def test_log_returns_lsn_when_enabled(
        self,
        json_logger_stream: LoggerStream,
        sample_entry: Entry,
    ):
        lsn = await json_logger_stream.log(sample_entry)
        assert lsn is not None
        assert isinstance(lsn, int)
        assert lsn > 0

    @pytest.mark.asyncio
    async def test_log_returns_none_when_lsn_disabled(
        self,
        no_lsn_logger_stream: LoggerStream,
        sample_entry: Entry,
    ):
        lsn = await no_lsn_logger_stream.log(sample_entry)
        assert lsn is None

    @pytest.mark.asyncio
    async def test_log_returns_none_for_stdout_logging(
        self,
        temp_log_directory: str,
    ):
        stream = LoggerStream(
            name="test_stdout",
            enable_lsn=True,
            instance_id=1,
        )
        await stream.initialize(
            stdout_writer=create_mock_stream_writer(),
            stderr_writer=create_mock_stream_writer(),
        )

        entry = Entry(message="stdout test", level=LogLevel.INFO)
        lsn = await stream.log(entry)

        assert lsn is None
        await stream.close()


class TestLSNMonotonicity:
    @pytest.mark.asyncio
    async def test_lsn_is_monotonically_increasing(
        self,
        json_logger_stream: LoggerStream,
        sample_entry_factory,
    ):
        lsns = []
        for idx in range(10):
            entry = sample_entry_factory(message=f"message {idx}")
            lsn = await json_logger_stream.log(entry)
            lsns.append(lsn)
            time.sleep(0.001)

        for idx in range(1, len(lsns)):
            assert lsns[idx] > lsns[idx - 1], f"LSN at {idx} not greater than previous"

    @pytest.mark.asyncio
    async def test_lsns_are_unique(
        self,
        json_logger_stream: LoggerStream,
        sample_entry_factory,
    ):
        lsns = set()
        for idx in range(100):
            entry = sample_entry_factory(message=f"message {idx}")
            lsn = await json_logger_stream.log(entry)
            assert lsn not in lsns, f"Duplicate LSN: {lsn}"
            lsns.add(lsn)

    @pytest.mark.asyncio
    async def test_lsn_stored_in_log_entry(
        self,
        binary_logger_stream: LoggerStream,
        sample_entry: Entry,
        temp_log_directory: str,
    ):
        lsn = await binary_logger_stream.log(sample_entry)

        log_path = os.path.join(temp_log_directory, "test.wal")
        entries = []
        async for offset, log, entry_lsn in binary_logger_stream.read_entries(log_path):
            entries.append((log, entry_lsn))

        assert len(entries) == 1
        assert entries[0][1] == lsn


class TestLSNWithDifferentInstanceIds:
    @pytest.mark.asyncio
    async def test_different_instances_generate_different_lsns(
        self,
        temp_log_directory: str,
    ):
        stream1 = LoggerStream(
            name="instance1",
            filename="test1.json",
            directory=temp_log_directory,
            enable_lsn=True,
            instance_id=1,
        )
        stream2 = LoggerStream(
            name="instance2",
            filename="test2.json",
            directory=temp_log_directory,
            enable_lsn=True,
            instance_id=2,
        )

        await stream1.initialize(
            stdout_writer=create_mock_stream_writer(),
            stderr_writer=create_mock_stream_writer(),
        )
        await stream2.initialize(
            stdout_writer=create_mock_stream_writer(),
            stderr_writer=create_mock_stream_writer(),
        )

        entry = Entry(message="test", level=LogLevel.INFO)

        lsn1 = await stream1.log(entry)
        lsn2 = await stream2.log(entry)

        assert lsn1 is not None
        assert lsn2 is not None
        assert lsn1 != lsn2

        instance1_from_lsn = (lsn1 >> 12) & 0x3FF
        instance2_from_lsn = (lsn2 >> 12) & 0x3FF

        assert instance1_from_lsn == 1
        assert instance2_from_lsn == 2

        await stream1.close()
        await stream2.close()
