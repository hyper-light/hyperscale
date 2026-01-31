"""
Unit tests for Raft WAL adapter.

Tests binary entry serialization, CRC32 integrity, and recovery.
"""

import struct
import zlib

import pytest

from hyperscale.distributed.raft.models import RaftLogEntry
from hyperscale.distributed.raft.raft_wal import RaftWALEntry


class TestRaftWALEntry:
    """Tests for RaftWALEntry binary serialization."""

    def test_pack_unpack_roundtrip(self) -> None:
        """Pack and unpack produce identical entry."""
        entry = RaftWALEntry(term=5, index=42, payload=b"hello world")
        packed = entry.pack()
        recovered = RaftWALEntry.unpack(packed)
        assert recovered is not None
        assert recovered.term == 5
        assert recovered.index == 42
        assert recovered.payload == b"hello world"

    def test_pack_format(self) -> None:
        """Packed data has correct binary layout."""
        entry = RaftWALEntry(term=1, index=2, payload=b"abc")
        packed = entry.pack()

        # Header: crc32(4) + total_length(4) + term(8) + index(8) + payload(3)
        assert len(packed) == 4 + 4 + 8 + 8 + 3

        # total_length covers term + index + payload = 8 + 8 + 3 = 19
        _, total_length = struct.unpack_from(">I I", packed, 0)
        assert total_length == 19

    def test_crc_validates_body(self) -> None:
        """CRC32 covers the body after the crc+length header."""
        entry = RaftWALEntry(term=3, index=7, payload=b"data")
        packed = entry.pack()

        # Corrupt a byte in the payload section
        corrupted = bytearray(packed)
        corrupted[-1] ^= 0xFF
        result = RaftWALEntry.unpack(bytes(corrupted))
        assert result is None

    def test_unpack_truncated_header(self) -> None:
        """Truncated header returns None."""
        assert RaftWALEntry.unpack(b"\x00" * 7) is None

    def test_unpack_truncated_body(self) -> None:
        """Truncated body returns None."""
        entry = RaftWALEntry(term=1, index=1, payload=b"x" * 100)
        packed = entry.pack()
        truncated = packed[:20]
        assert RaftWALEntry.unpack(truncated) is None

    def test_empty_payload(self) -> None:
        """Empty payload roundtrips correctly."""
        entry = RaftWALEntry(term=0, index=0, payload=b"")
        packed = entry.pack()
        recovered = RaftWALEntry.unpack(packed)
        assert recovered is not None
        assert recovered.payload == b""

    def test_large_payload(self) -> None:
        """Large payload roundtrips correctly."""
        payload = b"x" * 1_000_000
        entry = RaftWALEntry(term=99, index=500, payload=payload)
        packed = entry.pack()
        recovered = RaftWALEntry.unpack(packed)
        assert recovered is not None
        assert recovered.payload == payload
        assert recovered.term == 99
        assert recovered.index == 500

    def test_total_size_property(self) -> None:
        """total_size matches actual packed size."""
        entry = RaftWALEntry(term=1, index=1, payload=b"test")
        assert entry.total_size == len(entry.pack())

    def test_multiple_entries_sequential(self) -> None:
        """Multiple entries packed sequentially can be recovered."""
        entries = [
            RaftWALEntry(term=1, index=i, payload=f"entry-{i}".encode())
            for i in range(1, 6)
        ]

        buffer = b"".join(entry.pack() for entry in entries)

        recovered = []
        offset = 0
        while offset < len(buffer):
            _, total_length = struct.unpack_from(">I I", buffer, offset)
            entry_size = 8 + total_length  # crc + length + body
            entry_data = buffer[offset : offset + entry_size]
            entry = RaftWALEntry.unpack(entry_data)
            assert entry is not None
            recovered.append(entry)
            offset += entry_size

        assert len(recovered) == 5
        for idx, entry in enumerate(recovered, start=1):
            assert entry.index == idx
            assert entry.payload == f"entry-{idx}".encode()
