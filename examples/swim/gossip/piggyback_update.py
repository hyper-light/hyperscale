"""
Piggyback update for SWIM gossip dissemination.
"""

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ..core.types import UpdateType

if TYPE_CHECKING:
    from typing import Self


@dataclass(slots=True)
class PiggybackUpdate:
    """
    A membership update to be piggybacked on probe messages.
    
    In SWIM, membership updates are disseminated by "piggybacking" them
    onto the protocol messages (probes, acks). This achieves O(log n)
    dissemination without additional message overhead.
    
    Uses __slots__ for memory efficiency since many instances are created.
    """
    update_type: UpdateType
    node: tuple[str, int]
    incarnation: int
    timestamp: float
    # Number of times this update has been piggybacked
    broadcast_count: int = 0
    # Maximum number of times to piggyback (lambda * log(n))
    max_broadcasts: int = 10
    
    def should_broadcast(self) -> bool:
        """Check if this update should still be piggybacked."""
        return self.broadcast_count < self.max_broadcasts
    
    def mark_broadcast(self) -> None:
        """Mark that this update was piggybacked."""
        self.broadcast_count += 1
    
    def to_bytes(self) -> bytes:
        """Serialize update for transmission."""
        # Format: type:incarnation:host:port
        return (
            self.update_type.encode() + b':' +
            str(self.incarnation).encode() + b':' +
            self.node[0].encode() + b':' +
            str(self.node[1]).encode()
        )
    
    @classmethod
    def from_bytes(cls, data: bytes) -> 'PiggybackUpdate | None':
        """Deserialize an update from bytes."""
        try:
            parts = data.decode().split(':')
            if len(parts) < 4:
                return None
            update_type = parts[0]
            incarnation = int(parts[1])
            host = parts[2]
            port = int(parts[3])
            return cls(
                update_type=update_type,
                node=(host, port),
                incarnation=incarnation,
                timestamp=time.monotonic(),
            )
        except (ValueError, UnicodeDecodeError):
            return None
    
    def __hash__(self) -> int:
        return hash((self.update_type, self.node, self.incarnation))
    
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, PiggybackUpdate):
            return False
        return (
            self.update_type == other.update_type and
            self.node == other.node and
            self.incarnation == other.incarnation
        )

