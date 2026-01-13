"""
Gate runtime state for GateServer.

Manages all mutable state including peer tracking, job management,
datacenter health, leases, and metrics.
"""

import asyncio
from collections import defaultdict
from typing import Callable

from hyperscale.distributed.models import (
    GateHeartbeat,
    GateInfo,
    GateState as GateStateEnum,
    ManagerHeartbeat,
    DatacenterRegistrationState,
    DatacenterLease,
    JobSubmission,
    WorkflowResultPush,
    NegotiatedCapabilities,
)
from hyperscale.distributed.health import (
    ManagerHealthState,
    GateHealthState,
)
from hyperscale.distributed.reliability import BackpressureLevel


class GateRuntimeState:
    """
    Runtime state for GateServer.

    Centralizes all mutable dictionaries and tracking structures.
    Provides clean separation between configuration (immutable) and
    runtime state (mutable).
    """

    def __init__(self) -> None:
        """Initialize empty state containers."""
        # Counter protection lock (for race-free increments)
        self._counter_lock: asyncio.Lock | None = None

        # Lock creation lock (protects creation of per-resource locks)
        self._lock_creation_lock: asyncio.Lock | None = None

        # Manager state lock (protects manager status dictionaries)
        self._manager_state_lock: asyncio.Lock | None = None

        # Gate peer state
        self._gate_udp_to_tcp: dict[tuple[str, int], tuple[str, int]] = {}
        self._active_gate_peers: set[tuple[str, int]] = set()
        self._peer_state_locks: dict[tuple[str, int], asyncio.Lock] = {}
        self._peer_state_epoch: dict[tuple[str, int], int] = {}
        self._gate_peer_info: dict[tuple[str, int], GateHeartbeat] = {}
        self._known_gates: dict[str, GateInfo] = {}
        self._gate_peer_health: dict[str, GateHealthState] = {}

        # Datacenter/manager state
        self._dc_registration_states: dict[str, DatacenterRegistrationState] = {}
        self._datacenter_manager_status: dict[
            str, dict[tuple[str, int], ManagerHeartbeat]
        ] = {}
        self._manager_last_status: dict[tuple[str, int], float] = {}
        self._manager_health: dict[tuple[str, tuple[str, int]], ManagerHealthState] = {}

        # Backpressure state (AD-37)
        self._manager_backpressure: dict[tuple[str, int], BackpressureLevel] = {}
        self._backpressure_delay_ms: int = 0
        self._dc_backpressure: dict[str, BackpressureLevel] = {}

        # Protocol negotiation
        self._manager_negotiated_caps: dict[
            tuple[str, int], NegotiatedCapabilities
        ] = {}

        # Job state (handled by GateJobManager, but some local tracking)
        self._workflow_dc_results: dict[
            str, dict[str, dict[str, WorkflowResultPush]]
        ] = {}
        self._job_workflow_ids: dict[str, set[str]] = {}
        self._job_dc_managers: dict[str, dict[str, tuple[str, int]]] = {}
        self._job_submissions: dict[str, JobSubmission] = {}
        self._job_reporter_tasks: dict[str, dict[str, asyncio.Task]] = {}

        # Cancellation state
        self._cancellation_completion_events: dict[str, asyncio.Event] = {}
        self._cancellation_errors: dict[str, list[str]] = defaultdict(list)

        # Progress callbacks
        self._progress_callbacks: dict[str, tuple[str, int]] = {}

        # Lease state (legacy)
        self._leases: dict[str, DatacenterLease] = {}
        self._fence_token: int = 0

        # Leadership/orphan tracking
        self._dead_job_leaders: set[tuple[str, int]] = set()
        self._orphaned_jobs: dict[str, float] = {}

        # Gate state
        self._gate_state: GateStateEnum = GateStateEnum.SYNCING
        self._state_version: int = 0

        self._gate_peer_unhealthy_since: dict[tuple[str, int], float] = {}
        self._dead_gate_peers: set[tuple[str, int]] = set()
        self._dead_gate_timestamps: dict[tuple[str, int], float] = {}

        # Throughput tracking (AD-19)
        self._forward_throughput_count: int = 0
        self._forward_throughput_interval_start: float = 0.0
        self._forward_throughput_last_value: float = 0.0

    def initialize_locks(self) -> None:
        self._counter_lock = asyncio.Lock()
        self._lock_creation_lock = asyncio.Lock()
        self._manager_state_lock = asyncio.Lock()

    def _get_counter_lock(self) -> asyncio.Lock:
        if self._counter_lock is None:
            self._counter_lock = asyncio.Lock()
        return self._counter_lock

    def _get_lock_creation_lock(self) -> asyncio.Lock:
        if self._lock_creation_lock is None:
            self._lock_creation_lock = asyncio.Lock()
        return self._lock_creation_lock

    def _get_manager_state_lock(self) -> asyncio.Lock:
        if self._manager_state_lock is None:
            self._manager_state_lock = asyncio.Lock()
        return self._manager_state_lock

    async def get_or_create_peer_lock(self, peer_addr: tuple[str, int]) -> asyncio.Lock:
        async with self._get_lock_creation_lock():
            if peer_addr not in self._peer_state_locks:
                self._peer_state_locks[peer_addr] = asyncio.Lock()
            return self._peer_state_locks[peer_addr]

    async def increment_peer_epoch(self, peer_addr: tuple[str, int]) -> int:
        async with self._get_counter_lock():
            current_epoch = self._peer_state_epoch.get(peer_addr, 0)
            new_epoch = current_epoch + 1
            self._peer_state_epoch[peer_addr] = new_epoch
            return new_epoch

    async def get_peer_epoch(self, peer_addr: tuple[str, int]) -> int:
        async with self._get_counter_lock():
            return self._peer_state_epoch.get(peer_addr, 0)

    async def add_active_peer(self, peer_addr: tuple[str, int]) -> None:
        async with self._get_counter_lock():
            self._active_gate_peers.add(peer_addr)

    async def remove_active_peer(self, peer_addr: tuple[str, int]) -> None:
        async with self._get_counter_lock():
            self._active_gate_peers.discard(peer_addr)

    def remove_peer_lock(self, peer_addr: tuple[str, int]) -> None:
        """Remove lock and epoch when peer disconnects to prevent memory leak."""
        self._peer_state_locks.pop(peer_addr, None)
        self._peer_state_epoch.pop(peer_addr, None)

    def is_peer_active(self, peer_addr: tuple[str, int]) -> bool:
        """Check if a peer is in the active set."""
        return peer_addr in self._active_gate_peers

    def get_active_peer_count(self) -> int:
        """Get the number of active peers."""
        return len(self._active_gate_peers)

    async def update_manager_status(
        self,
        datacenter_id: str,
        manager_addr: tuple[str, int],
        heartbeat: ManagerHeartbeat,
        timestamp: float,
    ) -> None:
        async with self._get_manager_state_lock():
            if datacenter_id not in self._datacenter_manager_status:
                self._datacenter_manager_status[datacenter_id] = {}
            self._datacenter_manager_status[datacenter_id][manager_addr] = heartbeat
            self._manager_last_status[manager_addr] = timestamp

    def get_manager_status(
        self, datacenter_id: str, manager_addr: tuple[str, int]
    ) -> ManagerHeartbeat | None:
        """Get the latest heartbeat for a manager."""
        dc_status = self._datacenter_manager_status.get(datacenter_id, {})
        return dc_status.get(manager_addr)

    def get_dc_backpressure_level(self, datacenter_id: str) -> BackpressureLevel:
        """Get the backpressure level for a datacenter."""
        return self._dc_backpressure.get(datacenter_id, BackpressureLevel.NONE)

    def get_max_backpressure_level(self) -> BackpressureLevel:
        """Get the maximum backpressure level across all DCs."""
        if not self._dc_backpressure:
            return BackpressureLevel.NONE
        return max(self._dc_backpressure.values(), key=lambda x: x.value)

    # Lease methods
    def get_lease_key(self, job_id: str, datacenter_id: str) -> str:
        """Get the lease key for a job-DC pair."""
        return f"{job_id}:{datacenter_id}"

    def get_lease(self, job_id: str, datacenter_id: str) -> DatacenterLease | None:
        """Get the lease for a job-DC pair."""
        key = self.get_lease_key(job_id, datacenter_id)
        return self._leases.get(key)

    def set_lease(
        self, job_id: str, datacenter_id: str, lease: DatacenterLease
    ) -> None:
        """Set the lease for a job-DC pair."""
        key = self.get_lease_key(job_id, datacenter_id)
        self._leases[key] = lease

    def remove_lease(self, job_id: str, datacenter_id: str) -> None:
        """Remove the lease for a job-DC pair."""
        key = self.get_lease_key(job_id, datacenter_id)
        self._leases.pop(key, None)

    async def next_fence_token(self) -> int:
        async with self._get_counter_lock():
            self._fence_token += 1
            return self._fence_token

    # Orphan/leadership methods
    def mark_leader_dead(self, leader_addr: tuple[str, int]) -> None:
        """Mark a job leader as dead."""
        self._dead_job_leaders.add(leader_addr)

    def clear_dead_leader(self, leader_addr: tuple[str, int]) -> None:
        """Clear a dead leader."""
        self._dead_job_leaders.discard(leader_addr)

    def is_leader_dead(self, leader_addr: tuple[str, int]) -> bool:
        """Check if a leader is marked as dead."""
        return leader_addr in self._dead_job_leaders

    def mark_job_orphaned(self, job_id: str, timestamp: float) -> None:
        """Mark a job as orphaned."""
        self._orphaned_jobs[job_id] = timestamp

    def clear_orphaned_job(self, job_id: str) -> None:
        """Clear orphaned status for a job."""
        self._orphaned_jobs.pop(job_id, None)

    def is_job_orphaned(self, job_id: str) -> bool:
        """Check if a job is orphaned."""
        return job_id in self._orphaned_jobs

    def get_orphaned_jobs(self) -> dict[str, float]:
        """Get all orphaned jobs with their timestamps."""
        return dict(self._orphaned_jobs)

    # Cancellation methods
    def initialize_cancellation(self, job_id: str) -> asyncio.Event:
        """Initialize cancellation tracking for a job."""
        self._cancellation_completion_events[job_id] = asyncio.Event()
        return self._cancellation_completion_events[job_id]

    def get_cancellation_event(self, job_id: str) -> asyncio.Event | None:
        """Get the cancellation event for a job."""
        return self._cancellation_completion_events.get(job_id)

    def add_cancellation_error(self, job_id: str, error: str) -> None:
        """Add a cancellation error for a job."""
        self._cancellation_errors[job_id].append(error)

    def get_cancellation_errors(self, job_id: str) -> list[str]:
        """Get all cancellation errors for a job."""
        return list(self._cancellation_errors.get(job_id, []))

    def cleanup_cancellation(self, job_id: str) -> None:
        """Clean up cancellation state for a job."""
        self._cancellation_completion_events.pop(job_id, None)
        self._cancellation_errors.pop(job_id, None)

    async def record_forward(self) -> None:
        async with self._get_counter_lock():
            self._forward_throughput_count += 1

    def calculate_throughput(self, now: float, interval_seconds: float) -> float:
        """Calculate and reset throughput for the current interval."""
        elapsed = now - self._forward_throughput_interval_start
        if elapsed >= interval_seconds:
            throughput = (
                self._forward_throughput_count / elapsed if elapsed > 0 else 0.0
            )
            self._forward_throughput_last_value = throughput
            self._forward_throughput_count = 0
            self._forward_throughput_interval_start = now
        return self._forward_throughput_last_value

    async def increment_state_version(self) -> int:
        async with self._get_counter_lock():
            self._state_version += 1
            return self._state_version

    def get_state_version(self) -> int:
        return self._state_version

    # Gate state methods
    def set_gate_state(self, state: GateStateEnum) -> None:
        """Set the gate state."""
        self._gate_state = state

    def get_gate_state(self) -> GateStateEnum:
        """Get the current gate state."""
        return self._gate_state

    def is_active(self) -> bool:
        """Check if the gate is in ACTIVE state."""
        return self._gate_state == GateStateEnum.ACTIVE

    def mark_peer_unhealthy(self, peer_addr: tuple[str, int], timestamp: float) -> None:
        self._gate_peer_unhealthy_since[peer_addr] = timestamp

    def mark_peer_healthy(self, peer_addr: tuple[str, int]) -> None:
        self._gate_peer_unhealthy_since.pop(peer_addr, None)

    def mark_peer_dead(self, peer_addr: tuple[str, int], timestamp: float) -> None:
        self._dead_gate_peers.add(peer_addr)
        self._dead_gate_timestamps[peer_addr] = timestamp

    def cleanup_dead_peer(self, peer_addr: tuple[str, int]) -> None:
        self._dead_gate_peers.discard(peer_addr)
        self._dead_gate_timestamps.pop(peer_addr, None)
        self._gate_peer_unhealthy_since.pop(peer_addr, None)
        self.remove_peer_lock(peer_addr)

    def is_peer_dead(self, peer_addr: tuple[str, int]) -> bool:
        return peer_addr in self._dead_gate_peers

    def get_unhealthy_peers(self) -> dict[tuple[str, int], float]:
        return dict(self._gate_peer_unhealthy_since)

    def get_dead_peer_timestamps(self) -> dict[tuple[str, int], float]:
        return dict(self._dead_gate_timestamps)
