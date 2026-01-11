import os
import struct
import zlib

import pytest

from hyperscale.logging.models import Entry, Log, LogLevel
from hyperscale.logging.streams.logger_stream import BINARY_HEADER_SIZE, LoggerStream


class TestCRCMismatch:
    @pytest.mark.asyncio
    async def test_corrupted_crc_field_raises_error(
        self,
        binary_logger_stream: LoggerStream,
        sample_entry: Entry,
        temp_log_directory: str,
    ):
        await binary_logger_stream.log(sample_entry)

        log_path = os.path.join(temp_log_directory, "test.wal")
        with open(log_path, "rb") as log_file:
            data = log_file.read()

        corrupted = b"\x00\x00\x00\x00" + data[4:]

        with open(log_path, "wb") as log_file:
            log_file.write(corrupted)

        with pytest.raises(ValueError, match="CRC mismatch"):
            async for offset, log, lsn in binary_logger_stream.read_entries(log_path):
                pass

    @pytest.mark.asyncio
    async def test_flipped_bit_in_payload_raises_error(
        self,
        binary_logger_stream: LoggerStream,
        sample_entry: Entry,
        temp_log_directory: str,
    ):
        await binary_logger_stream.log(sample_entry)

        log_path = os.path.join(temp_log_directory, "test.wal")
        with open(log_path, "rb") as log_file:
            data = bytearray(log_file.read())

        if len(data) > 20:
            data[20] ^= 0xFF

        with open(log_path, "wb") as log_file:
            log_file.write(bytes(data))

        with pytest.raises(ValueError, match="CRC mismatch"):
            async for offset, log, lsn in binary_logger_stream.read_entries(log_path):
                pass


class TestTruncatedData:
    @pytest.mark.asyncio
    async def test_header_only_raises_error(
        self,
        binary_logger_stream: LoggerStream,
        temp_log_directory: str,
    ):
        log_path = os.path.join(temp_log_directory, "header_only.wal")

        header = struct.pack("<I", 0)
        header += struct.pack("<IQ", 100, 12345)

        with open(log_path, "wb") as log_file:
            log_file.write(header)

        with pytest.raises(ValueError, match="Truncated payload"):
            async for offset, log, lsn in binary_logger_stream.read_entries(log_path):
                pass

    @pytest.mark.asyncio
    async def test_partial_header_raises_error(
        self,
        binary_logger_stream: LoggerStream,
        temp_log_directory: str,
    ):
        log_path = os.path.join(temp_log_directory, "partial_header.wal")

        with open(log_path, "wb") as log_file:
            log_file.write(b"\x00" * 10)

        with pytest.raises(ValueError, match="Truncated header"):
            async for offset, log, lsn in binary_logger_stream.read_entries(log_path):
                pass


class TestMalformedPayload:
    @pytest.mark.asyncio
    async def test_invalid_json_payload(
        self,
        binary_logger_stream: LoggerStream,
        temp_log_directory: str,
    ):
        log_path = os.path.join(temp_log_directory, "malformed.wal")

        payload = b"not valid json {{{{"
        lsn = 12345

        header = struct.pack("<IQ", len(payload), lsn)
        crc = zlib.crc32(header + payload) & 0xFFFFFFFF
        data = struct.pack("<I", crc) + header + payload

        with open(log_path, "wb") as log_file:
            log_file.write(data)

        with pytest.raises(Exception):
            async for offset, log, lsn in binary_logger_stream.read_entries(log_path):
                pass


class TestDecodeErrors:
    @pytest.mark.asyncio
    async def test_decode_empty_data_raises_error(
        self,
        binary_logger_stream: LoggerStream,
    ):
        with pytest.raises(ValueError, match="Entry too short"):
            binary_logger_stream._decode_binary(b"")

    @pytest.mark.asyncio
    async def test_decode_short_data_raises_error(
        self,
        binary_logger_stream: LoggerStream,
    ):
        with pytest.raises(ValueError, match="Entry too short"):
            binary_logger_stream._decode_binary(b"\x00" * 10)

    @pytest.mark.asyncio
    async def test_decode_exact_header_size_with_zero_length_raises_error(
        self,
        binary_logger_stream: LoggerStream,
    ):
        header = struct.pack("<I", 0)
        header += struct.pack("<IQ", 10, 0)

        with pytest.raises(ValueError, match="Truncated entry"):
            binary_logger_stream._decode_binary(header)


class TestRecoveryFromPartialWrite:
    @pytest.mark.asyncio
    async def test_valid_entries_before_corruption(
        self,
        binary_logger_stream: LoggerStream,
        sample_entry_factory,
        temp_log_directory: str,
    ):
        for idx in range(3):
            entry = sample_entry_factory(message=f"valid message {idx}")
            await binary_logger_stream.log(entry)

        log_path = os.path.join(temp_log_directory, "test.wal")
        with open(log_path, "ab") as log_file:
            log_file.write(b"\x00\x00\x00\x00\x00")

        valid_entries = []
        try:
            async for offset, log, lsn in binary_logger_stream.read_entries(log_path):
                valid_entries.append(log)
        except ValueError:
            pass

        assert len(valid_entries) == 3


class TestFileNotFound:
    @pytest.mark.asyncio
    async def test_read_nonexistent_file_raises_error(
        self,
        binary_logger_stream: LoggerStream,
        temp_log_directory: str,
    ):
        nonexistent_path = os.path.join(temp_log_directory, "does_not_exist.wal")

        with pytest.raises(FileNotFoundError):
            async for offset, log, lsn in binary_logger_stream.read_entries(
                nonexistent_path
            ):
                pass

    @pytest.mark.asyncio
    async def test_get_last_lsn_nonexistent_file_returns_none(
        self,
        binary_logger_stream: LoggerStream,
        temp_log_directory: str,
    ):
        nonexistent_path = os.path.join(temp_log_directory, "does_not_exist.wal")

        result = await binary_logger_stream.get_last_lsn(nonexistent_path)
        assert result is None
