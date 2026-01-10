"""
Piggyback update for SWIM gossip dissemination.
"""

import sys
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ..core.constants import DELIM_COLON, encode_int
from ..core.types import UpdateType

if TYPE_CHECKING:
    from typing import Self

# Pre-encode update type bytes for fast lookup (module-level cache)
_UPDATE_TYPE_CACHE: dict[str, bytes] = {
    'alive': b'alive',
    'suspect': b'suspect',
    'dead': b'dead',
    'join': b'join',
    'leave': b'leave',
}

# Module-level cache for host encoding (shared across all instances)
_HOST_BYTES_CACHE: dict[str, bytes] = {}
_MAX_HOST_CACHE_SIZE = 1000


@dataclass(slots=True)
class PiggybackUpdate:
    """
    A membership update to be piggybacked on probe messages.

    In SWIM, membership updates are disseminated by "piggybacking" them
    onto the protocol messages (probes, acks). This achieves O(log n)
    dissemination without additional message overhead.

    Uses __slots__ for memory efficiency since many instances are created.

    AD-35 Task 12.4.3: Extended with optional role field for role-aware failure detection.
    """
    update_type: UpdateType
    node: tuple[str, int]
    incarnation: int
    timestamp: float
    # Number of times this update has been piggybacked
    broadcast_count: int = 0
    # Maximum number of times to piggyback (lambda * log(n))
    max_broadcasts: int = 10
    # AD-35 Task 12.4.3: Optional node role (gate/manager/worker)
    role: str | None = None
    
    def should_broadcast(self) -> bool:
        """Check if this update should still be piggybacked."""
        return self.broadcast_count < self.max_broadcasts
    
    def mark_broadcast(self) -> None:
        """Mark that this update was piggybacked."""
        self.broadcast_count += 1
    
    def to_bytes(self) -> bytes:
        """
        Serialize update for transmission.
        
        Uses pre-allocated constants and caching for performance.
        Format: type:incarnation:host:port
        """
        # Use cached update type bytes
        type_bytes = _UPDATE_TYPE_CACHE.get(self.update_type)
        if type_bytes is None:
            type_bytes = self.update_type.encode()
        
        # Use cached host encoding (module-level shared cache)
        host = self.node[0]
        host_bytes = _HOST_BYTES_CACHE.get(host)
        if host_bytes is None:
            host_bytes = host.encode()
            # Limit cache size
            if len(_HOST_BYTES_CACHE) < _MAX_HOST_CACHE_SIZE:
                _HOST_BYTES_CACHE[host] = host_bytes
        
        # Use pre-allocated delimiter and integer encoding
        return (
            type_bytes + DELIM_COLON +
            encode_int(self.incarnation) + DELIM_COLON +
            host_bytes + DELIM_COLON +
            encode_int(self.node[1])
        )
    
    @classmethod
    def from_bytes(cls, data: bytes) -> 'PiggybackUpdate | None':
        """
        Deserialize an update from bytes.
        
        Uses string interning for hosts to reduce memory when
        the same hosts appear in many updates.
        """
        try:
            # Use maxsplit for efficiency - we only need 4 parts
            parts = data.decode().split(':', maxsplit=3)
            if len(parts) < 4:
                return None
            update_type = parts[0]
            incarnation = int(parts[1])
            # Intern host string to share memory across updates
            host = sys.intern(parts[2])
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

