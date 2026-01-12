from __future__ import annotations

import struct
from typing import NamedTuple


class LSN(NamedTuple):
    """
    128-bit globally unique, globally orderable Log Sequence Number.

    Structure (128 bits total):
    - logical_time (48 bits): Lamport timestamp for global ordering
    - node_id (16 bits): Unique node identifier (0-65535)
    - sequence (24 bits): Per-millisecond sequence (0-16777215)
    - wall_clock (40 bits): Unix milliseconds for debugging (~34 years)

    Ordering uses (logical_time, node_id, sequence) - wall_clock is NOT
    used for ordering, only for human debugging.

    Properties:
    - Globally unique: node_id + sequence guarantees no collisions
    - Globally orderable: Lamport logical time provides total order
    - High throughput: 16M LSNs/ms/node (24-bit sequence)
    - Debuggable: Wall clock embedded for approximate timestamps
    """

    logical_time: int
    node_id: int
    sequence: int
    wall_clock: int

    LOGICAL_TIME_BITS = 48
    NODE_ID_BITS = 16
    SEQUENCE_BITS = 24
    WALL_CLOCK_BITS = 40

    MAX_LOGICAL_TIME = (1 << LOGICAL_TIME_BITS) - 1
    MAX_NODE_ID = (1 << NODE_ID_BITS) - 1
    MAX_SEQUENCE = (1 << SEQUENCE_BITS) - 1
    MAX_WALL_CLOCK = (1 << WALL_CLOCK_BITS) - 1

    def __lt__(self, other: object) -> bool:
        """
        Compare LSNs using Lamport ordering.

        Primary: logical_time
        Tiebreaker 1: node_id
        Tiebreaker 2: sequence

        wall_clock is NOT used for ordering.
        """
        if not isinstance(other, LSN):
            return NotImplemented

        if self.logical_time != other.logical_time:
            return self.logical_time < other.logical_time

        if self.node_id != other.node_id:
            return self.node_id < other.node_id

        return self.sequence < other.sequence

    def __le__(self, other: object) -> bool:
        if not isinstance(other, LSN):
            return NotImplemented
        return self == other or self < other

    def __gt__(self, other: object) -> bool:
        if not isinstance(other, LSN):
            return NotImplemented
        return other < self

    def __ge__(self, other: object) -> bool:
        if not isinstance(other, LSN):
            return NotImplemented
        return self == other or self > other

    def to_bytes(self) -> bytes:
        """
        Encode LSN to 16 bytes (128 bits).

        Layout:
        - bytes 0-7: (logical_time << 16) | node_id
        - bytes 8-15: (sequence << 40) | wall_clock
        """
        high = (self.logical_time << 16) | self.node_id
        low = (self.sequence << 40) | self.wall_clock
        return struct.pack(">QQ", high, low)

    @classmethod
    def from_bytes(cls, data: bytes) -> LSN:
        """Decode LSN from 16 bytes."""
        if len(data) != 16:
            raise ValueError(f"LSN requires 16 bytes, got {len(data)}")

        high, low = struct.unpack(">QQ", data)

        logical_time = high >> 16
        node_id = high & 0xFFFF
        sequence = low >> 40
        wall_clock = low & 0xFFFFFFFFFF

        return cls(
            logical_time=logical_time,
            node_id=node_id,
            sequence=sequence,
            wall_clock=wall_clock,
        )

    def to_int(self) -> int:
        """
        Convert to 128-bit integer for storage or transmission.

        Layout: logical_time(48) | node_id(16) | sequence(24) | wall_clock(40)
        """
        return (
            (self.logical_time << 80)
            | (self.node_id << 64)
            | (self.sequence << 40)
            | self.wall_clock
        )

    @classmethod
    def from_int(cls, value: int) -> LSN:
        """Reconstruct LSN from 128-bit integer."""
        logical_time = (value >> 80) & cls.MAX_LOGICAL_TIME
        node_id = (value >> 64) & cls.MAX_NODE_ID
        sequence = (value >> 40) & cls.MAX_SEQUENCE
        wall_clock = value & cls.MAX_WALL_CLOCK

        return cls(
            logical_time=logical_time,
            node_id=node_id,
            sequence=sequence,
            wall_clock=wall_clock,
        )

    def __str__(self) -> str:
        """Human-readable format for debugging."""
        return (
            f"LSN({self.logical_time}:{self.node_id}:{self.sequence}@{self.wall_clock})"
        )

    def __repr__(self) -> str:
        return (
            f"LSN(logical_time={self.logical_time}, node_id={self.node_id}, "
            f"sequence={self.sequence}, wall_clock={self.wall_clock})"
        )
