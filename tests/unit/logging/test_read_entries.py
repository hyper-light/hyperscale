import os
import time

import msgspec
import pytest

from hyperscale.logging.config.durability_mode import DurabilityMode
from hyperscale.logging.models import Entry, Log, LogLevel
from hyperscale.logging.streams.logger_stream import LoggerStream


class TestReadEntriesJson:
    @pytest.mark.asyncio
    async def test_read_single_json_entry(
        self,
        json_logger_stream: LoggerStream,
        sample_entry: Entry,
        temp_log_directory: str,
    ):
        await json_logger_stream.log(sample_entry)

        log_path = os.path.join(temp_log_directory, "test.json")
        entries = []
        async for offset, log, lsn in json_logger_stream.read_entries(log_path):
            entries.append((offset, log, lsn))

        assert len(entries) == 1
        assert entries[0][0] == 0
        assert entries[0][1].entry.message == "Test log message"

    @pytest.mark.asyncio
    async def test_read_multiple_json_entries(
        self,
        json_logger_stream: LoggerStream,
        sample_entry_factory,
        temp_log_directory: str,
    ):
        messages = ["first", "second", "third"]
        for message in messages:
            entry = sample_entry_factory(message=message)
            await json_logger_stream.log(entry)
            time.sleep(0.001)

        log_path = os.path.join(temp_log_directory, "test.json")
        entries = []
        async for offset, log, lsn in json_logger_stream.read_entries(log_path):
            entries.append(log)

        assert len(entries) == 3
        assert entries[0].entry.message == "first"
        assert entries[1].entry.message == "second"
        assert entries[2].entry.message == "third"

    @pytest.mark.asyncio
    async def test_read_json_entries_with_offset(
        self,
        json_logger_stream: LoggerStream,
        sample_entry_factory,
        temp_log_directory: str,
    ):
        for idx in range(5):
            entry = sample_entry_factory(message=f"message {idx}")
            await json_logger_stream.log(entry)

        log_path = os.path.join(temp_log_directory, "test.json")

        all_entries = []
        async for offset, log, lsn in json_logger_stream.read_entries(log_path):
            all_entries.append((offset, log))

        second_entry_offset = all_entries[1][0]

        from_offset_entries = []
        async for offset, log, lsn in json_logger_stream.read_entries(
            log_path, from_offset=second_entry_offset
        ):
            from_offset_entries.append(log)

        assert len(from_offset_entries) == 4
        assert from_offset_entries[0].entry.message == "message 1"


class TestReadEntriesBinary:
    @pytest.mark.asyncio
    async def test_read_single_binary_entry(
        self,
        binary_logger_stream: LoggerStream,
        sample_entry: Entry,
        temp_log_directory: str,
    ):
        await binary_logger_stream.log(sample_entry)

        log_path = os.path.join(temp_log_directory, "test.wal")
        entries = []
        async for offset, log, lsn in binary_logger_stream.read_entries(log_path):
            entries.append((offset, log, lsn))

        assert len(entries) == 1
        assert entries[0][0] == 0
        assert entries[0][1].entry.message == "Test log message"
        assert entries[0][2] is not None

    @pytest.mark.asyncio
    async def test_read_multiple_binary_entries(
        self,
        binary_logger_stream: LoggerStream,
        sample_entry_factory,
        temp_log_directory: str,
    ):
        messages = ["alpha", "beta", "gamma"]
        expected_lsns = []
        for message in messages:
            entry = sample_entry_factory(message=message)
            lsn = await binary_logger_stream.log(entry)
            expected_lsns.append(lsn)
            time.sleep(0.001)

        log_path = os.path.join(temp_log_directory, "test.wal")
        entries = []
        async for offset, log, lsn in binary_logger_stream.read_entries(log_path):
            entries.append((log, lsn))

        assert len(entries) == 3
        for idx, (log, lsn) in enumerate(entries):
            assert log.entry.message == messages[idx]
            assert lsn == expected_lsns[idx]

    @pytest.mark.asyncio
    async def test_read_binary_entries_with_offset(
        self,
        binary_logger_stream: LoggerStream,
        sample_entry_factory,
        temp_log_directory: str,
    ):
        for idx in range(5):
            entry = sample_entry_factory(message=f"binary message {idx}")
            await binary_logger_stream.log(entry)

        log_path = os.path.join(temp_log_directory, "test.wal")

        all_entries = []
        async for offset, log, lsn in binary_logger_stream.read_entries(log_path):
            all_entries.append((offset, log))

        third_entry_offset = all_entries[2][0]

        from_offset_entries = []
        async for offset, log, lsn in binary_logger_stream.read_entries(
            log_path, from_offset=third_entry_offset
        ):
            from_offset_entries.append(log)

        assert len(from_offset_entries) == 3
        assert from_offset_entries[0].entry.message == "binary message 2"


class TestReadEntriesOffsets:
    @pytest.mark.asyncio
    async def test_json_offsets_are_monotonically_increasing(
        self,
        json_logger_stream: LoggerStream,
        sample_entry_factory,
        temp_log_directory: str,
    ):
        for idx in range(10):
            entry = sample_entry_factory(message=f"message {idx}")
            await json_logger_stream.log(entry)

        log_path = os.path.join(temp_log_directory, "test.json")

        offsets = []
        async for offset, log, lsn in json_logger_stream.read_entries(log_path):
            offsets.append(offset)

        for idx in range(1, len(offsets)):
            assert offsets[idx] > offsets[idx - 1]

    @pytest.mark.asyncio
    async def test_binary_offsets_are_monotonically_increasing(
        self,
        binary_logger_stream: LoggerStream,
        sample_entry_factory,
        temp_log_directory: str,
    ):
        for idx in range(10):
            entry = sample_entry_factory(message=f"message {idx}")
            await binary_logger_stream.log(entry)

        log_path = os.path.join(temp_log_directory, "test.wal")

        offsets = []
        async for offset, log, lsn in binary_logger_stream.read_entries(log_path):
            offsets.append(offset)

        for idx in range(1, len(offsets)):
            assert offsets[idx] > offsets[idx - 1]


class TestReadEntriesEmptyFile:
    @pytest.mark.asyncio
    async def test_read_empty_json_file(
        self,
        json_logger_stream: LoggerStream,
        temp_log_directory: str,
    ):
        log_path = os.path.join(temp_log_directory, "empty.json")
        with open(log_path, "w") as empty_file:
            pass

        entries = []
        async for offset, log, lsn in json_logger_stream.read_entries(log_path):
            entries.append(log)

        assert len(entries) == 0

    @pytest.mark.asyncio
    async def test_read_empty_binary_file(
        self,
        binary_logger_stream: LoggerStream,
        temp_log_directory: str,
    ):
        log_path = os.path.join(temp_log_directory, "empty.wal")
        with open(log_path, "wb") as empty_file:
            pass

        entries = []
        async for offset, log, lsn in binary_logger_stream.read_entries(log_path):
            entries.append(log)

        assert len(entries) == 0


class TestReadEntriesLsnExtraction:
    @pytest.mark.asyncio
    async def test_json_lsn_extraction(
        self,
        json_logger_stream: LoggerStream,
        sample_entry: Entry,
        temp_log_directory: str,
    ):
        written_lsn = await json_logger_stream.log(sample_entry)

        log_path = os.path.join(temp_log_directory, "test.json")

        async for offset, log, lsn in json_logger_stream.read_entries(log_path):
            assert lsn == log.lsn
            assert lsn == written_lsn

    @pytest.mark.asyncio
    async def test_binary_lsn_extraction(
        self,
        binary_logger_stream: LoggerStream,
        sample_entry: Entry,
        temp_log_directory: str,
    ):
        written_lsn = await binary_logger_stream.log(sample_entry)

        log_path = os.path.join(temp_log_directory, "test.wal")

        async for offset, log, lsn in binary_logger_stream.read_entries(log_path):
            assert lsn == written_lsn
