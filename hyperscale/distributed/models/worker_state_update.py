"""Wire model ``WorkerStateUpdate`` -- pickled under the wire namespace
``hyperscale.distributed.models.worker_state`` (see that module)."""

import sys
from dataclasses import dataclass

from .message import Message
from .worker_state_constants import _DELIM

# Pre-encode state bytes for fast lookup
_STATE_BYTES_CACHE: dict[str, bytes] = {
    "registered": b"registered",
    "draining": b"draining",
    "dead": b"dead",
    "evicted": b"evicted",
    "left": b"left",
}

# Module-level cache for host encoding
_HOST_BYTES_CACHE: dict[str, bytes] = {}

_MAX_HOST_CACHE_SIZE = 1000


@dataclass(slots=True, kw_only=True)
class WorkerStateUpdate(Message):
    """
    Worker state update for cross-manager dissemination.

    Sent via TCP on critical events (registration, death, eviction)
    and piggybacked on UDP gossip for steady-state convergence.

    Incarnation numbers prevent stale updates:
    - Incremented by owner manager on each state change
    - Receivers reject updates with lower incarnation
    """

    worker_id: str
    owner_manager_id: str
    host: str
    tcp_port: int
    udp_port: int

    # State info
    state: str  # "registered", "draining", "dead", "evicted", "left"
    incarnation: int  # Monotonic, reject lower incarnation

    # Capacity (for scheduling decisions)
    total_cores: int
    available_cores: int

    # Metadata
    timestamp: float  # time.monotonic() on owner manager
    datacenter: str = ""

    def to_bytes(self) -> bytes:
        """
        Serialize for piggyback transmission.

        Format:
            worker_id:owner_manager_id:host:tcp_port:udp_port:state:
            incarnation:total_cores:available_cores:timestamp:datacenter

        Uses caching for frequently-encoded values.
        """
        # Use cached state bytes
        state_bytes = _STATE_BYTES_CACHE.get(self.state)
        if state_bytes is None:
            state_bytes = self.state.encode()

        # Use cached host encoding
        host_bytes = _HOST_BYTES_CACHE.get(self.host)
        if host_bytes is None:
            host_bytes = self.host.encode()
            if len(_HOST_BYTES_CACHE) < _MAX_HOST_CACHE_SIZE:
                _HOST_BYTES_CACHE[self.host] = host_bytes

        # Build serialized form
        parts = [
            self.worker_id.encode(),
            self.owner_manager_id.encode(),
            host_bytes,
            str(self.tcp_port).encode(),
            str(self.udp_port).encode(),
            state_bytes,
            str(self.incarnation).encode(),
            str(self.total_cores).encode(),
            str(self.available_cores).encode(),
            f"{self.timestamp:.6f}".encode(),
            self.datacenter.encode(),
        ]

        return _DELIM.join(parts)

    @classmethod
    def from_bytes(cls, data: bytes) -> "WorkerStateUpdate | None":
        """
        Deserialize from piggyback.

        Uses string interning for IDs to reduce memory.
        """
        try:
            decoded = data.decode()
            parts = decoded.split(":", maxsplit=10)

            if len(parts) < 11:
                return None

            return cls(
                worker_id=sys.intern(parts[0]),
                owner_manager_id=sys.intern(parts[1]),
                host=sys.intern(parts[2]),
                tcp_port=int(parts[3]),
                udp_port=int(parts[4]),
                state=parts[5],
                incarnation=int(parts[6]),
                total_cores=int(parts[7]),
                available_cores=int(parts[8]),
                timestamp=float(parts[9]),
                datacenter=parts[10] if parts[10] else "",
            )
        except (ValueError, UnicodeDecodeError, IndexError):
            return None

    def is_alive_state(self) -> bool:
        """Check if this update represents a live worker."""
        return self.state in ("registered", "draining")

    def is_dead_state(self) -> bool:
        """Check if this update represents a dead/removed worker."""
        return self.state in ("dead", "evicted", "left")
