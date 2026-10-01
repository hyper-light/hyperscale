from __future__ import annotations

from typing import NamedTuple

WALL_MS_BITS = 48
LOGICAL_BITS = 16
NODE_ID_BITS = 64
MAX_WALL_MS = (1 << WALL_MS_BITS) - 1
MAX_LOGICAL = (1 << LOGICAL_BITS) - 1
MAX_NODE_ID = (1 << NODE_ID_BITS) - 1
ENCODED_SIZE = 16


class HLCTimestamp(NamedTuple):
    """A hybrid logical clock timestamp (AD-39).

    ``wall_ms`` is the physical component -- milliseconds since the Unix
    epoch, never behind the issuing node's clock and never more than the
    configured maximum offset ahead of any correct node's -- ``logical``
    orders events that share a ``wall_ms``, and ``node_id`` makes
    timestamps from different nodes distinct. Ordering is the tuple order:
    physical time first, so timestamps are both causally consistent and
    close to real time.
    """

    wall_ms: int
    logical: int
    node_id: int

    def to_bytes(self) -> bytes:
        """16 bytes: wall_ms(48) | logical(16), then node_id(64)."""
        return ((self.wall_ms << LOGICAL_BITS) | self.logical).to_bytes(8, "big") + self.node_id.to_bytes(8, "big")

    @classmethod
    def from_bytes(cls, data: bytes) -> HLCTimestamp:
        if len(data) != ENCODED_SIZE:
            raise ValueError(f"HLC timestamp must be {ENCODED_SIZE} bytes, got {len(data)}")
        physical_and_logical = int.from_bytes(data[:8], "big")
        return cls(
            wall_ms=physical_and_logical >> LOGICAL_BITS,
            logical=physical_and_logical & MAX_LOGICAL,
            node_id=int.from_bytes(data[8:], "big"),
        )

    @classmethod
    def zero(cls) -> HLCTimestamp:
        return cls(0, 0, 0)

    @property
    def seconds(self) -> float:
        """The physical component as Unix seconds."""
        return self.wall_ms / 1000.0
