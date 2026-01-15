from __future__ import annotations

import struct
import zlib
from typing import TYPE_CHECKING

from hyperscale.logging.lsn import LSN

from ..events.event_type import JobEventType
from .entry_state import WALEntryState

if TYPE_CHECKING:
    from ..events.job_event import JobEventUnion

HEADER_SIZE = 34
HEADER_FORMAT = ">I I Q 16s B B"


class WALEntry:
    """
    Binary WAL entry with CRC32 checksum.

    Wire format (34 bytes header + variable payload):
    +----------+----------+----------+----------+----------+----------+
    | CRC32    | Length   | LSN      | HLC      | State    | Type     |
    | (4 bytes)| (4 bytes)| (8 bytes)|(16 bytes)| (1 byte) | (1 byte) |
    +----------+----------+----------+----------+----------+----------+
    |                        Payload (variable)                        |
    +------------------------------------------------------------------+
    """

    __slots__ = (
        "_lsn",
        "_hlc",
        "_state",
        "_event_type",
        "_payload",
        "_crc",
    )

    def __init__(
        self,
        lsn: int,
        hlc: LSN,
        state: WALEntryState,
        event_type: JobEventType,
        payload: bytes,
    ) -> None:
        self._lsn = lsn
        self._hlc = hlc
        self._state = state
        self._event_type = event_type
        self._payload = payload
        self._crc: int | None = None

    @property
    def lsn(self) -> int:
        return self._lsn

    @property
    def hlc(self) -> LSN:
        return self._hlc

    @property
    def state(self) -> WALEntryState:
        return self._state

    @property
    def event_type(self) -> JobEventType:
        return self._event_type

    @property
    def payload(self) -> bytes:
        return self._payload

    @property
    def crc(self) -> int | None:
        return self._crc

    def to_bytes(self) -> bytes:
        hlc_bytes = self._hlc.to_bytes()
        total_length = HEADER_SIZE + len(self._payload)

        header_without_crc = struct.pack(
            ">I Q 16s B B",
            total_length,
            self._lsn,
            hlc_bytes,
            self._state.value,
            self._event_type.value,
        )

        body = header_without_crc + self._payload
        crc = zlib.crc32(body) & 0xFFFFFFFF
        self._crc = crc

        return struct.pack(">I", crc) + body

    @classmethod
    def from_bytes(cls, data: bytes) -> WALEntry:
        if len(data) < HEADER_SIZE:
            raise ValueError(f"WAL entry too short: {len(data)} < {HEADER_SIZE}")

        stored_crc = struct.unpack(">I", data[:4])[0]
        body = data[4:]

        computed_crc = zlib.crc32(body) & 0xFFFFFFFF
        if stored_crc != computed_crc:
            raise ValueError(
                f"CRC mismatch: stored={stored_crc:08x}, computed={computed_crc:08x}"
            )

        total_length, lsn, hlc_bytes, state_val, type_val = struct.unpack(
            ">I Q 16s B B",
            body[:30],
        )

        hlc = LSN.from_bytes(hlc_bytes)
        state = WALEntryState(state_val)
        event_type = JobEventType(type_val)
        payload = body[30:]

        entry = cls(
            lsn=lsn,
            hlc=hlc,
            state=state,
            event_type=event_type,
            payload=payload,
        )
        entry._crc = stored_crc
        return entry

    def with_state(self, new_state: WALEntryState) -> WALEntry:
        return WALEntry(
            lsn=self._lsn,
            hlc=self._hlc,
            state=new_state,
            event_type=self._event_type,
            payload=self._payload,
        )

    def __repr__(self) -> str:
        return (
            f"WALEntry(lsn={self._lsn}, hlc={self._hlc}, "
            f"state={self._state.name}, type={self._event_type.name})"
        )
