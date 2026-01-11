import os
import struct

import pytest

from hyperscale.logging.config.durability_mode import DurabilityMode
from hyperscale.logging.models import Entry, Log, LogLevel
from hyperscale.logging.streams.logger_stream import BINARY_HEADER_SIZE, LoggerStream


class TestEmptyFiles:
    @pytest.mark.asyncio
    async def test_read_entries_empty_json(
        self,
        json_logger_stream: LoggerStream,
        temp_log_directory: str,
    ):
        log_path = os.path.join(temp_log_directory, "empty.json")
        with open(log_path, "w"):
            pass

        entries = []
        async for offset, log, lsn in json_logger_stream.read_entries(log_path):
            entries.append(log)

        assert len(entries) == 0

    @pytest.mark.asyncio
    async def test_read_entries_empty_binary(
        self,
        binary_logger_stream: LoggerStream,
        temp_log_directory: str,
    ):
        log_path = os.path.join(temp_log_directory, "empty.wal")
        with open(log_path, "wb"):
            pass

        entries = []
        async for offset, log, lsn in binary_logger_stream.read_entries(log_path):
            entries.append(log)

        assert len(entries) == 0


class TestTruncatedEntries:
    @pytest.mark.asyncio
    async def test_truncated_header_raises_error(
        self,
        binary_logger_stream: LoggerStream,
        temp_log_directory: str,
    ):
        log_path = os.path.join(temp_log_directory, "truncated_header.wal")
        with open(log_path, "wb") as log_file:
            log_file.write(b"\x00" * 8)

        with pytest.raises(ValueError, match="Truncated header"):
            async for offset, log, lsn in binary_logger_stream.read_entries(log_path):
                pass

    @pytest.mark.asyncio
    async def test_truncated_payload_raises_error(
        self,
        binary_logger_stream: LoggerStream,
        sample_entry: Entry,
        temp_log_directory: str,
    ):
        await binary_logger_stream.log(sample_entry)

        log_path = os.path.join(temp_log_directory, "test.wal")
        with open(log_path, "rb") as log_file:
            data = log_file.read()

        truncated_path = os.path.join(temp_log_directory, "truncated_payload.wal")
        with open(truncated_path, "wb") as log_file:
            log_file.write(data[:-20])

        with pytest.raises(ValueError, match="Truncated payload"):
            async for offset, log, lsn in binary_logger_stream.read_entries(
                truncated_path
            ):
                pass


class TestFilenameExtensions:
    @pytest.mark.asyncio
    async def test_valid_json_extension(self, temp_log_directory: str):
        stream = LoggerStream(
            name="test",
            filename="test.json",
            directory=temp_log_directory,
        )
        assert stream._default_logfile == "test.json"

    @pytest.mark.asyncio
    async def test_valid_wal_extension(self, temp_log_directory: str):
        stream = LoggerStream(
            name="test",
            filename="test.wal",
            directory=temp_log_directory,
        )
        assert stream._default_logfile == "test.wal"

    @pytest.mark.asyncio
    async def test_valid_log_extension(self, temp_log_directory: str):
        stream = LoggerStream(
            name="test",
            filename="test.log",
            directory=temp_log_directory,
        )
        assert stream._default_logfile == "test.log"

    @pytest.mark.asyncio
    async def test_valid_bin_extension(self, temp_log_directory: str):
        stream = LoggerStream(
            name="test",
            filename="test.bin",
            directory=temp_log_directory,
        )
        assert stream._default_logfile == "test.bin"

    @pytest.mark.asyncio
    async def test_invalid_extension_raises_error(self, temp_log_directory: str):
        stream = LoggerStream(
            name="test",
            filename="test.txt",
            directory=temp_log_directory,
        )
        await stream.initialize()

        with pytest.raises(ValueError, match="Invalid log file extension"):
            stream._to_logfile_path("test.txt")

        await stream.close()


class TestLargeMessages:
    @pytest.mark.asyncio
    async def test_large_message_json(
        self,
        json_logger_stream: LoggerStream,
        temp_log_directory: str,
    ):
        large_message = "x" * 100000
        entry = Entry(message=large_message, level=LogLevel.INFO)

        await json_logger_stream.log(entry)

        log_path = os.path.join(temp_log_directory, "test.json")
        async for offset, log, lsn in json_logger_stream.read_entries(log_path):
            assert log.entry.message == large_message

    @pytest.mark.asyncio
    async def test_large_message_binary(
        self,
        binary_logger_stream: LoggerStream,
        temp_log_directory: str,
    ):
        large_message = "y" * 100000
        entry = Entry(message=large_message, level=LogLevel.INFO)

        await binary_logger_stream.log(entry)

        log_path = os.path.join(temp_log_directory, "test.wal")
        async for offset, log, lsn in binary_logger_stream.read_entries(log_path):
            assert log.entry.message == large_message


class TestSpecialCharacters:
    @pytest.mark.asyncio
    async def test_unicode_message_json(
        self,
        json_logger_stream: LoggerStream,
        temp_log_directory: str,
    ):
        unicode_message = "Hello 世界 🌍 مرحبا שלום"
        entry = Entry(message=unicode_message, level=LogLevel.INFO)

        await json_logger_stream.log(entry)

        log_path = os.path.join(temp_log_directory, "test.json")
        async for offset, log, lsn in json_logger_stream.read_entries(log_path):
            assert log.entry.message == unicode_message

    @pytest.mark.asyncio
    async def test_unicode_message_binary(
        self,
        binary_logger_stream: LoggerStream,
        temp_log_directory: str,
    ):
        unicode_message = "日本語テスト 中文测试 한국어 テスト"
        entry = Entry(message=unicode_message, level=LogLevel.INFO)

        await binary_logger_stream.log(entry)

        log_path = os.path.join(temp_log_directory, "test.wal")
        async for offset, log, lsn in binary_logger_stream.read_entries(log_path):
            assert log.entry.message == unicode_message

    @pytest.mark.asyncio
    async def test_newlines_in_message_json(
        self,
        json_logger_stream: LoggerStream,
        temp_log_directory: str,
    ):
        multiline_message = "Line 1\nLine 2\nLine 3"
        entry = Entry(message=multiline_message, level=LogLevel.INFO)

        await json_logger_stream.log(entry)

        log_path = os.path.join(temp_log_directory, "test.json")
        async for offset, log, lsn in json_logger_stream.read_entries(log_path):
            assert log.entry.message == multiline_message


class TestBoundaryConditions:
    @pytest.mark.asyncio
    async def test_zero_lsn_in_header(
        self,
        binary_logger_stream: LoggerStream,
    ):
        entry = Entry(message="test", level=LogLevel.INFO)
        log = Log(
            entry=entry,
            filename="test.py",
            function_name="test",
            line_number=1,
        )

        encoded = binary_logger_stream._encode_binary(log, lsn=None)

        lsn_stored = struct.unpack("<Q", encoded[8:16])[0]
        assert lsn_stored == 0

    @pytest.mark.asyncio
    async def test_max_lsn_value(
        self,
        binary_logger_stream: LoggerStream,
    ):
        entry = Entry(message="test", level=LogLevel.INFO)
        log = Log(
            entry=entry,
            filename="test.py",
            function_name="test",
            line_number=1,
        )

        max_lsn = (1 << 63) - 1
        encoded = binary_logger_stream._encode_binary(log, lsn=max_lsn)
        decoded_log, decoded_lsn = binary_logger_stream._decode_binary(encoded)

        assert decoded_lsn == max_lsn
