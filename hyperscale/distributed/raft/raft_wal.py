"""
Raft WAL adapter for persisting log entries to disk.

Wraps the production WALWriter to provide Raft-specific
binary format, group commit, backpressure, and recovery.

Binary entry format:
    [4:crc32][4:total_length][8:term][8:index][N:payload]

Total length includes the 16-byte term+index header plus payload.
CRC32 covers everything after the CRC field itself.
"""

import asyncio
import struct
import zlib
from pathlib import Path
from typing import TYPE_CHECKING

import cloudpickle

from .models import RaftLogEntry

if TYPE_CHECKING:
    from hyperscale.distributed.ledger.wal import WALWriter
    from hyperscale.logging import Logger


# Fixed-width header: crc32 (4) + total_length (4) + term (8) + index (8)
_HEADER_FORMAT = ">I I Q Q"
_HEADER_SIZE = struct.calcsize(_HEADER_FORMAT)  # 24 bytes
_CRC_SIZE = 4
_LENGTH_OFFSET = 4
_BODY_OFFSET = 8  # After crc32 and total_length


class RaftWALEntry:
    """
    Binary-serializable Raft WAL entry with CRC32 integrity.

    Provides pack/unpack for disk persistence. The CRC32
    covers the body (term + index + payload) for corruption
    detection during recovery.
    """

    __slots__ = ("term", "index", "payload")

    def __init__(self, term: int, index: int, payload: bytes) -> None:
        self.term = term
        self.index = index
        self.payload = payload

    def pack(self) -> bytes:
        """Serialize to binary format with CRC32."""
        body = struct.pack(">Q Q", self.term, self.index) + self.payload
        total_length = len(body)
        crc = zlib.crc32(body) & 0xFFFFFFFF
        header = struct.pack(">I I", crc, total_length)
        return header + body

    @classmethod
    def unpack(cls, data: bytes) -> "RaftWALEntry | None":
        """
        Deserialize from binary format. Returns None on corruption.

        Validates CRC32 and length before constructing the entry.
        """
        if len(data) < _HEADER_SIZE:
            return None

        stored_crc, total_length = struct.unpack_from(">I I", data, 0)

        expected_body_size = total_length
        actual_body = data[_BODY_OFFSET : _BODY_OFFSET + expected_body_size]

        if len(actual_body) < expected_body_size:
            return None

        computed_crc = zlib.crc32(actual_body) & 0xFFFFFFFF
        if computed_crc != stored_crc:
            return None

        term, index = struct.unpack_from(">Q Q", actual_body, 0)
        payload = actual_body[16:]
        return cls(term=term, index=index, payload=payload)

    @property
    def total_size(self) -> int:
        """Total on-disk size including header."""
        return _BODY_OFFSET + 16 + len(self.payload)


class RaftWAL:
    """
    Raft-specific WAL adapter wrapping WALWriter.

    Provides:
    - Append with group commit and backpressure from WALWriter
    - Recovery: sequential replay reconstructs RaftLogEntry list
    - Close with graceful drain

    Each RaftLogEntry is serialized via cloudpickle for the payload,
    wrapped in a binary envelope with CRC32 integrity checking.
    """

    __slots__ = (
        "_path",
        "_writer",
        "_logger",
        "_closed",
        "_loop",
    )

    def __init__(
        self,
        path: Path,
        logger: "Logger",
    ) -> None:
        self._path = path
        self._logger = logger
        self._writer: "WALWriter | None" = None
        self._closed = False
        self._loop: asyncio.AbstractEventLoop | None = None

    async def open(self) -> None:
        """Open the WAL file and start the writer."""
        from hyperscale.distributed.ledger.wal.wal_writer import (
            WALWriter,
            WALWriterConfig,
        )

        self._loop = asyncio.get_running_loop()
        self._path.parent.mkdir(parents=True, exist_ok=True)

        config = WALWriterConfig(
            batch_timeout_microseconds=500,
            batch_max_entries=500,
            batch_max_bytes=4 * 1024 * 1024,
            queue_max_size=5000,
        )

        self._writer = WALWriter(
            path=self._path,
            config=config,
            logger=self._logger,
        )
        await self._writer.start()
        self._closed = False

    async def close(self) -> None:
        """Close the WAL writer, draining pending writes."""
        if self._closed:
            return
        self._closed = True
        if self._writer is not None:
            await self._writer.stop()

    async def append(self, entry: RaftLogEntry) -> bool:
        """
        Append a Raft log entry to the WAL.

        Serializes the entry, wraps in binary envelope with CRC32,
        and submits to the WALWriter for group commit.

        Returns True on success, False if closed or backpressure rejects.
        """
        if self._closed or self._writer is None:
            return False

        from hyperscale.distributed.ledger.wal.wal_writer import WriteRequest

        payload = cloudpickle.dumps(entry)
        wal_entry = RaftWALEntry(
            term=entry.term,
            index=entry.index,
            payload=payload,
        )
        data = wal_entry.pack()

        future: asyncio.Future[None] = self._loop.create_future()
        request = WriteRequest(data=data, future=future)

        try:
            self._writer.submit(request)
            await future
            return True
        except Exception:
            return False

    async def recover(self) -> list[RaftLogEntry]:
        """
        Recover Raft log entries from the WAL file.

        Reads the file sequentially, validates CRC32 for each entry,
        and deserializes the payload back to RaftLogEntry. Stops at
        the first corrupt entry (truncation-safe).

        Returns entries sorted by index.
        """
        if not self._path.exists():
            return []

        entries = await asyncio.get_running_loop().run_in_executor(
            None, self._recover_sync
        )
        entries.sort(key=lambda entry: entry.index)
        return entries

    def _recover_sync(self) -> list[RaftLogEntry]:
        """Synchronous recovery: read and validate all WAL entries."""
        recovered: list[RaftLogEntry] = []
        file_data = self._path.read_bytes()
        offset = 0
        file_size = len(file_data)

        while offset + _BODY_OFFSET <= file_size:
            # Read header to get total_length
            _, total_length = struct.unpack_from(">I I", file_data, offset)

            entry_total_size = _BODY_OFFSET + total_length
            if offset + entry_total_size > file_size:
                break  # Truncated entry, stop recovery

            entry_data = file_data[offset : offset + entry_total_size]
            wal_entry = RaftWALEntry.unpack(entry_data)
            if wal_entry is None:
                break  # CRC mismatch or corruption, stop

            try:
                raft_entry: RaftLogEntry = cloudpickle.loads(wal_entry.payload)
                recovered.append(raft_entry)
            except Exception:
                break  # Deserialization failure, stop

            offset += entry_total_size

        return recovered

    @property
    def is_closed(self) -> bool:
        """Whether the WAL has been closed."""
        return self._closed
