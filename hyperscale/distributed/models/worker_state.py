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


# Pre-encode reason bytes for workflow reassignment
_REASSIGNMENT_REASON_BYTES_CACHE: dict[str, bytes] = {
    "worker_dead": b"worker_dead",
    "worker_evicted": b"worker_evicted",
    "worker_overloaded": b"worker_overloaded",
    "rebalance": b"rebalance",
}


@dataclass(slots=True, kw_only=True)
class WorkflowReassignmentNotification(Message):
    """
    Notification of workflow reassignment after worker failure.

    Sent via TCP to peer managers when workflows are requeued
    from a failed worker. Enables peers to:
    - Update their tracking of workflow locations
    - Avoid sending results to stale worker assignments
    - Maintain consistent view of workflow state

    This is informational (not authoritative) - the job leader
    remains the source of truth for workflow state.
    """

    job_id: str
    workflow_id: str
    sub_workflow_token: str
    failed_worker_id: str
    reason: str  # "worker_dead", "worker_evicted", "worker_overloaded", "rebalance"
    originating_manager_id: str
    timestamp: float
    datacenter: str = ""

    def to_bytes(self) -> bytes:
        """Serialize for TCP transmission."""
        reason_bytes = _REASSIGNMENT_REASON_BYTES_CACHE.get(self.reason)
        if reason_bytes is None:
            reason_bytes = self.reason.encode()

        parts = [
            self.job_id.encode(),
            self.workflow_id.encode(),
            self.sub_workflow_token.encode(),
            self.failed_worker_id.encode(),
            reason_bytes,
            self.originating_manager_id.encode(),
            f"{self.timestamp:.6f}".encode(),
            self.datacenter.encode(),
        ]

        return _DELIM.join(parts)

    @classmethod
    def from_bytes(cls, data: bytes) -> "WorkflowReassignmentNotification | None":
        """Deserialize from TCP transmission."""
        try:
            decoded = data.decode()
            parts = decoded.split(":", maxsplit=7)

            if len(parts) < 8:
                return None

            return cls(
                job_id=sys.intern(parts[0]),
                workflow_id=sys.intern(parts[1]),
                sub_workflow_token=sys.intern(parts[2]),
                failed_worker_id=sys.intern(parts[3]),
                reason=parts[4],
                originating_manager_id=sys.intern(parts[5]),
                timestamp=float(parts[6]),
                datacenter=parts[7] if parts[7] else "",
            )
        except (ValueError, UnicodeDecodeError, IndexError):
            return None


@dataclass(slots=True, kw_only=True)
class WorkflowReassignmentBatch(Message):
    """
    Batch of workflow reassignment notifications.

    Used when multiple workflows need reassignment (e.g., worker death
    affecting multiple running workflows). Reduces TCP overhead.
    """

    originating_manager_id: str
    failed_worker_id: str
    reason: str
    timestamp: float
    datacenter: str
    reassignments: list[
        tuple[str, str, str]
    ]  # (job_id, workflow_id, sub_workflow_token)

    def to_bytes(self) -> bytes:
        """Serialize for TCP transmission."""
        reason_bytes = _REASSIGNMENT_REASON_BYTES_CACHE.get(self.reason)
        if reason_bytes is None:
            reason_bytes = self.reason.encode()

        # Header: manager_id|worker_id|reason|timestamp|datacenter|count
        header_parts = [
            self.originating_manager_id.encode(),
            self.failed_worker_id.encode(),
            reason_bytes,
            f"{self.timestamp:.6f}".encode(),
            self.datacenter.encode(),
            str(len(self.reassignments)).encode(),
        ]
        header = b"|".join(header_parts)

        # Each reassignment: job_id:workflow_id:sub_token
        reassignment_parts = [
            f"{job_id}:{workflow_id}:{sub_token}".encode()
            for job_id, workflow_id, sub_token in self.reassignments
        ]

        # Combine: header||reassignment1||reassignment2||...
        all_parts = [header] + reassignment_parts
        return b"||".join(all_parts)

    @classmethod
    def from_bytes(cls, data: bytes) -> "WorkflowReassignmentBatch | None":
        """Deserialize from TCP transmission."""
        try:
            parts = data.split(b"||")
            if not parts:
                return None

            # Parse header
            header = parts[0].split(b"|")
            if len(header) < 6:
                return None

            originating_manager_id = sys.intern(header[0].decode())
            failed_worker_id = sys.intern(header[1].decode())
            reason = header[2].decode()
            timestamp = float(header[3].decode())
            datacenter = header[4].decode()
            count = int(header[5].decode())

            # Parse reassignments
            reassignments: list[tuple[str, str, str]] = []
            for reassignment_bytes in parts[1 : count + 1]:
                reassignment_parts = reassignment_bytes.decode().split(":", maxsplit=2)
                if len(reassignment_parts) == 3:
                    reassignments.append(
                        (
                            sys.intern(reassignment_parts[0]),
                            sys.intern(reassignment_parts[1]),
                            sys.intern(reassignment_parts[2]),
                        )
                    )

            return cls(
                originating_manager_id=originating_manager_id,
                failed_worker_id=failed_worker_id,
                reason=reason,
                timestamp=timestamp,
                datacenter=datacenter,
                reassignments=reassignments,
            )
        except (ValueError, UnicodeDecodeError, IndexError):
            return None
