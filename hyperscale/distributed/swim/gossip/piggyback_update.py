"""
Piggyback update for SWIM gossip dissemination.
"""

import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING

from hyperscale.distributed.swim.core.constants import DELIM_COLON, encode_int
from hyperscale.distributed.swim.core.types import UpdateType

from hyperscale.distributed.runtime import Clock, RealClock


_DEFAULT_CLOCK: Clock = RealClock()

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

# Lifeguard accuser annotation (wire format, see ``PiggybackUpdate.accuser``):
# a suspect entry is followed by its own entry
# ``by:<incarnation>:<accuser_host>:<accuser_port>``. An older node decodes it
# as an update of an unknown type and drops it on its forward-compatible
# path (``gossip_unknown_updates_suppressed``), so it never disturbs the
# suspect entry the older node does act on; a newer node attaches it to the
# entry before it. Appending a trailing ``:accuser`` field to the suspect
# entry itself, as role and node_id were added, is not compatible: an older
# parser's last field (node_id) takes the rest of the entry.
ACCUSER_UPDATE_TYPE = "by"
ACCUSER_ENTRY_PREFIX = b"|by:"

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
    # Stable identity bound to ``node`` when known. Address reuse means
    # address+incarnation alone cannot fence stale predecessor gossip.
    node_id: str | None = None
    # The member whose own failed probes raised a SUSPECT (the Lifeguard
    # suspicion's ``From``), carried unchanged through relays so a
    # receiver counts it as ONE independent confirmation however many
    # relayers pass it on. None when unknown (an older sender): never a
    # confirmation.
    accuser: tuple[str, int] | None = None
    
    def should_broadcast(self) -> bool:
        """Check if this update should still be piggybacked."""
        return self.broadcast_count < self.max_broadcasts
    
    def to_bytes(self) -> bytes:
        """
        Serialize update for transmission.

        Uses pre-allocated constants and caching for performance.
        Format: type:incarnation:host:port[:role[:node_id]]
        Role and node_id are optional. If node_id is present without
        role, the role field is emitted empty to preserve field order.
        A known accuser follows as its own ``by:`` entry
        (``ACCUSER_UPDATE_TYPE``).
        """
        # Every UpdateType is cached; only those are ever queued or sent.
        type_bytes = _UPDATE_TYPE_CACHE[self.update_type]

        # Use cached host encoding (module-level shared cache)
        host = self.node[0]
        host_bytes = _HOST_BYTES_CACHE.get(host)
        if host_bytes is None:
            host_bytes = host.encode()
            # Limit cache size
            if len(_HOST_BYTES_CACHE) < _MAX_HOST_CACHE_SIZE:
                _HOST_BYTES_CACHE[host] = host_bytes

        # Use pre-allocated delimiter and integer encoding
        result = (
            type_bytes + DELIM_COLON +
            encode_int(self.incarnation) + DELIM_COLON +
            host_bytes + DELIM_COLON +
            encode_int(self.node[1])
        )

        # AD-35 Task 12.4.3: Append role if present (backward compatible).
        # Node identity is a trailing optional field so older 5-field
        # role-bearing gossip remains parseable.
        if self.role or self.node_id:
            result += DELIM_COLON + (self.role.encode() if self.role else b"")
        if self.node_id:
            result += DELIM_COLON + self.node_id.encode()

        return result + (
            ACCUSER_ENTRY_PREFIX
            + encode_int(self.incarnation) + DELIM_COLON
            + self.accuser[0].encode() + DELIM_COLON
            + encode_int(self.accuser[1])
            if self.accuser
            else b""
        )
    
    @classmethod
    def from_bytes(cls, data: bytes) -> 'PiggybackUpdate | None':
        """
        Deserialize an update from bytes.

        Uses string interning for hosts to reduce memory when
        the same hosts appear in many updates.

        AD-35 Task 12.4.3: Parses optional 5th field (role) if present.
        A 6th field carries stable node identity for address-reuse
        fencing. Backward compatible - defaults optional fields to None.
        """
        try:
            # Split into parts - maxsplit=5 to get up to 6 parts:
            # type:inc:host:port:role:node_id.
            parts = data.decode().split(':', maxsplit=5)
            if len(parts) < 4:
                return None
            update_type = parts[0]
            incarnation = int(parts[1])
            # Intern host string to share memory across updates
            host = sys.intern(parts[2])
            port = int(parts[3])
            # AD-35 Task 12.4.3: Parse role if present (backward compatible)
            role = parts[4] if len(parts) >= 5 and parts[4] else None
            node_id = parts[5] if len(parts) >= 6 and parts[5] else None
            return cls(
                update_type=update_type,
                node=(host, port),
                incarnation=incarnation,
                timestamp=_DEFAULT_CLOCK.monotonic(),
                role=role,
                node_id=node_id,
            )
        except (ValueError, UnicodeDecodeError):
            return None
    
    def __hash__(self) -> int:
        return hash((self.update_type, self.node, self.incarnation, self.node_id))
    
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, PiggybackUpdate):
            return False
        return (
            self.update_type == other.update_type and
            self.node == other.node and
            self.incarnation == other.incarnation and
            self.node_id == other.node_id
        )
