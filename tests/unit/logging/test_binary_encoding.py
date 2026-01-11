import hashlib
import struct

import pytest

from hyperscale.logging.models import Entry, Log, LogLevel
from hyperscale.logging.streams.logger_stream import BINARY_HEADER_SIZE, LoggerStream


class TestBinaryEncode:
    @pytest.mark.asyncio
    async def test_encode_binary_returns_bytes(
        self,
        binary_logger_stream: LoggerStream,
    ):
        entry = Entry(message="test", level=LogLevel.INFO)
        log = Log(
            entry=entry,
            filename="test.py",
            function_name="test_func",
            line_number=42,
        )

        encoded = binary_logger_stream._encode_binary(log, lsn=12345)
        assert isinstance(encoded, bytes)

    @pytest.mark.asyncio
    async def test_encode_binary_header_structure(
        self,
        binary_logger_stream: LoggerStream,
    ):
        entry = Entry(message="test", level=LogLevel.INFO)
        log = Log(
            entry=entry,
            filename="test.py",
            function_name="test_func",
            line_number=42,
        )
        lsn = 12345

        encoded = binary_logger_stream._encode_binary(log, lsn=lsn)

        assert len(encoded) >= BINARY_HEADER_SIZE

        crc_stored = struct.unpack("<I", encoded[:4])[0]
        length, lsn_stored = struct.unpack("<IQ", encoded[4:16])

        assert lsn_stored == lsn
        assert length == len(encoded) - BINARY_HEADER_SIZE

    @pytest.mark.asyncio
    async def test_encode_binary_crc_is_valid(
        self,
        binary_logger_stream: LoggerStream,
    ):
        entry = Entry(message="test message", level=LogLevel.INFO)
        log = Log(
            entry=entry,
            filename="test.py",
            function_name="test_func",
            line_number=42,
        )

        encoded = binary_logger_stream._encode_binary(log, lsn=12345)

        crc_stored = struct.unpack("<I", encoded[:4])[0]

        length = struct.unpack("<I", encoded[4:8])[0]
        crc_computed = hashlib.crc32(encoded[4 : 16 + length]) & 0xFFFFFFFF

        assert crc_stored == crc_computed

    @pytest.mark.asyncio
    async def test_encode_binary_with_none_lsn(
        self,
        binary_logger_stream: LoggerStream,
    ):
        entry = Entry(message="test", level=LogLevel.INFO)
        log = Log(
            entry=entry,
            filename="test.py",
            function_name="test_func",
            line_number=42,
        )

        encoded = binary_logger_stream._encode_binary(log, lsn=None)

        lsn_stored = struct.unpack("<Q", encoded[8:16])[0]
        assert lsn_stored == 0


class TestBinaryDecode:
    @pytest.mark.asyncio
    async def test_decode_binary_returns_log_and_lsn(
        self,
        binary_logger_stream: LoggerStream,
    ):
        entry = Entry(message="test", level=LogLevel.INFO)
        log = Log(
            entry=entry,
            filename="test.py",
            function_name="test_func",
            line_number=42,
        )
        lsn = 12345

        encoded = binary_logger_stream._encode_binary(log, lsn=lsn)
        decoded_log, decoded_lsn = binary_logger_stream._decode_binary(encoded)

        assert isinstance(decoded_log, Log)
        assert decoded_lsn == lsn

    @pytest.mark.asyncio
    async def test_decode_binary_preserves_message(
        self,
        binary_logger_stream: LoggerStream,
    ):
        original_message = "This is a test message with special chars: åäö 日本語"
        entry = Entry(message=original_message, level=LogLevel.INFO)
        log = Log(
            entry=entry,
            filename="test.py",
            function_name="test_func",
            line_number=42,
        )

        encoded = binary_logger_stream._encode_binary(log, lsn=12345)
        decoded_log, _ = binary_logger_stream._decode_binary(encoded)

        assert decoded_log.entry.message == original_message


class TestBinaryRoundtrip:
    @pytest.mark.asyncio
    async def test_encode_decode_roundtrip(
        self,
        binary_logger_stream: LoggerStream,
    ):
        entry = Entry(message="roundtrip test", level=LogLevel.ERROR)
        log = Log(
            entry=entry,
            filename="roundtrip.py",
            function_name="test_roundtrip",
            line_number=100,
        )
        lsn = 9876543210

        encoded = binary_logger_stream._encode_binary(log, lsn=lsn)
        decoded_log, decoded_lsn = binary_logger_stream._decode_binary(encoded)

        assert decoded_log.entry.message == "roundtrip test"
        assert decoded_log.entry.level == LogLevel.ERROR
        assert decoded_log.filename == "roundtrip.py"
        assert decoded_log.function_name == "test_roundtrip"
        assert decoded_log.line_number == 100
        assert decoded_lsn == lsn

    @pytest.mark.asyncio
    async def test_multiple_encode_decode_roundtrips(
        self,
        binary_logger_stream: LoggerStream,
    ):
        messages = [
            ("message 1", LogLevel.INFO, 1),
            ("message 2", LogLevel.WARNING, 2),
            ("message 3", LogLevel.ERROR, 3),
        ]

        for message, level, lsn in messages:
            entry = Entry(message=message, level=level)
            log = Log(
                entry=entry,
                filename="test.py",
                function_name="test",
                line_number=1,
            )

            encoded = binary_logger_stream._encode_binary(log, lsn=lsn)
            decoded_log, decoded_lsn = binary_logger_stream._decode_binary(encoded)

            assert decoded_log.entry.message == message
            assert decoded_log.entry.level == level
            assert decoded_lsn == lsn


class TestBinaryDecodeErrors:
    @pytest.mark.asyncio
    async def test_decode_too_short_raises_value_error(
        self,
        binary_logger_stream: LoggerStream,
    ):
        short_data = b"\x00" * 10

        with pytest.raises(ValueError, match="Entry too short"):
            binary_logger_stream._decode_binary(short_data)

    @pytest.mark.asyncio
    async def test_decode_truncated_payload_raises_value_error(
        self,
        binary_logger_stream: LoggerStream,
    ):
        entry = Entry(message="test", level=LogLevel.INFO)
        log = Log(
            entry=entry,
            filename="test.py",
            function_name="test_func",
            line_number=42,
        )

        encoded = binary_logger_stream._encode_binary(log, lsn=12345)
        truncated = encoded[: len(encoded) - 10]

        with pytest.raises(ValueError, match="Truncated entry"):
            binary_logger_stream._decode_binary(truncated)

    @pytest.mark.asyncio
    async def test_decode_corrupted_crc_raises_value_error(
        self,
        binary_logger_stream: LoggerStream,
    ):
        entry = Entry(message="test", level=LogLevel.INFO)
        log = Log(
            entry=entry,
            filename="test.py",
            function_name="test_func",
            line_number=42,
        )

        encoded = binary_logger_stream._encode_binary(log, lsn=12345)

        corrupted = b"\xff\xff\xff\xff" + encoded[4:]

        with pytest.raises(ValueError, match="CRC mismatch"):
            binary_logger_stream._decode_binary(corrupted)

    @pytest.mark.asyncio
    async def test_decode_corrupted_payload_raises_value_error(
        self,
        binary_logger_stream: LoggerStream,
    ):
        entry = Entry(message="test", level=LogLevel.INFO)
        log = Log(
            entry=entry,
            filename="test.py",
            function_name="test_func",
            line_number=42,
        )

        encoded = binary_logger_stream._encode_binary(log, lsn=12345)

        corrupted = bytearray(encoded)
        corrupted[20] = (corrupted[20] + 1) % 256
        corrupted = bytes(corrupted)

        with pytest.raises(ValueError, match="CRC mismatch"):
            binary_logger_stream._decode_binary(corrupted)
