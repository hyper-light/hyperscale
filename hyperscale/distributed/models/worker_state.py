"""
Worker state update models for cross-manager dissemination (AD-48).

These models support worker visibility across managers using:
- TCP broadcast for critical events (registration, death)
- UDP gossip piggyback for steady-state convergence

Each worker has ONE owner manager that is authoritative; other managers
track workers as "remote" with reduced trust.
"""

import sys
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from .message import Message

if TYPE_CHECKING:
    from typing import Self

# Pre-encode state bytes for fast lookup
_STATE_BYTES_CACHE: dict[str, bytes] = {
    "registered": b"registered",
    "dead": b"dead",
    "evicted": b"evicted",
    "left": b"left",
}

# Module-level cache for host encoding
_HOST_BYTES_CACHE: dict[str, bytes] = {}
_MAX_HOST_CACHE_SIZE = 1000

# Field delimiter for serialization
_DELIM = b":"


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
    state: str  # "registered", "dead", "evicted", "left"
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

        Format: worker_id:owner_manager_id:host:tcp_port:udp_port:state:incarnation:total_cores:available_cores:timestamp:datacenter

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
        return self.state == "registered"

    def is_dead_state(self) -> bool:
        """Check if this update represents a dead/removed worker."""
        return self.state in ("dead", "evicted", "left")


@dataclass(slots=True, kw_only=True)
class WorkerStatePiggybackUpdate:
    """
    A worker state update to be piggybacked on SWIM messages.

    Similar to PiggybackUpdate but for worker state dissemination.
    Uses __slots__ for memory efficiency since many instances are created.
    """

    update: WorkerStateUpdate
    timestamp: float
    broadcast_count: int = 0
    max_broadcasts: int = 10

    def should_broadcast(self) -> bool:
        """Check if this update should still be piggybacked."""
        return self.broadcast_count < self.max_broadcasts

    def mark_broadcast(self) -> None:
        """Mark that this update was piggybacked."""
        self.broadcast_count += 1


@dataclass(slots=True, kw_only=True)
class WorkerListResponse(Message):
    """
    Response to list_workers request containing all locally-owned workers.

    Sent when a new manager joins the cluster and requests the worker
    list from peer managers to bootstrap its knowledge.
    """

    manager_id: str  # Responding manager's ID
    workers: list[WorkerStateUpdate] = field(default_factory=list)

    def to_bytes(self) -> bytes:
        """Serialize for transmission."""
        # Format: manager_id|worker1_bytes|worker2_bytes|...
        parts = [self.manager_id.encode()]
        parts.extend(worker.to_bytes() for worker in self.workers)
        return b"|".join(parts)

    @classmethod
    def from_bytes(cls, data: bytes) -> "WorkerListResponse | None":
        """Deserialize from transmission."""
        try:
            parts = data.split(b"|")
            if not parts:
                return None

            manager_id = parts[0].decode()
            workers = []

            for worker_bytes in parts[1:]:
                if worker_bytes:
                    worker_update = WorkerStateUpdate.from_bytes(worker_bytes)
                    if worker_update:
                        workers.append(worker_update)

            return cls(manager_id=manager_id, workers=workers)
        except (ValueError, UnicodeDecodeError):
            return None


@dataclass(slots=True, kw_only=True)
class WorkerListRequest(Message):
    """
    Request for worker list from peer managers.

    Sent when a manager joins the cluster to bootstrap knowledge
    of workers registered with other managers.
    """

    requester_id: str  # Requesting manager's ID
    requester_datacenter: str = ""  # Requester's datacenter
