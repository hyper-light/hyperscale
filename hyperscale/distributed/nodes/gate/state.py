"""
Gate runtime state for GateServer.

Manages all mutable state including peer tracking, job management,
datacenter health, leases, and metrics.
"""

import asyncio
import time
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
        self._backpressure_lock: asyncio.Lock | None = None

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
        self._job_lease_renewal_tokens: dict[str, str] = {}

        # Cancellation state
        self._cancellation_completion_events: dict[str, asyncio.Event] = {}
        self._cancellation_errors: dict[str, list[str]] = defaultdict(list)

        # Progress callbacks
        self._progress_callbacks: dict[str, tuple[str, int]] = {}

        self._client_update_history_limit: int = 200
        self._job_update_sequences: dict[str, int] = {}
        self._job_update_history: dict[str, list[tuple[int, str, bytes, float]]] = {}
        self._job_client_update_positions: dict[str, dict[tuple[str, int], int]] = {}

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
        self._backpressure_lock = asyncio.Lock()

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

    def cleanup_peer_tcp_tracking(self, peer_addr: tuple[str, int]) -> None:
        """Remove TCP-address-keyed tracking data for a peer."""
        self._gate_peer_unhealthy_since.pop(peer_addr, None)
        self._dead_gate_peers.discard(peer_addr)
        self._dead_gate_timestamps.pop(peer_addr, None)
        self._dead_job_leaders.discard(peer_addr)
        self._active_gate_peers.discard(peer_addr)
        self.remove_peer_lock(peer_addr)

    def cleanup_peer_udp_tracking(self, peer_addr: tuple[str, int]) -> set[str]:
        """Remove UDP-address-keyed tracking data for a peer."""
        udp_addrs_to_remove = {
            udp_addr
            for udp_addr, tcp_addr in list(self._gate_udp_to_tcp.items())
            if tcp_addr == peer_addr
        }
        gate_ids_to_remove: set[str] = set()

        for udp_addr, heartbeat in list(self._gate_peer_info.items()):
            if udp_addr in udp_addrs_to_remove:
                continue

            peer_tcp_host = heartbeat.tcp_host or udp_addr[0]
            peer_tcp_port = heartbeat.tcp_port or udp_addr[1]
            peer_tcp_addr = (peer_tcp_host, peer_tcp_port)
            if peer_tcp_addr == peer_addr:
                udp_addrs_to_remove.add(udp_addr)

        for udp_addr in udp_addrs_to_remove:
            heartbeat = self._gate_peer_info.get(udp_addr)
            if heartbeat and heartbeat.node_id:
                gate_ids_to_remove.add(heartbeat.node_id)

            self._gate_udp_to_tcp.pop(udp_addr, None)
            self._gate_peer_info.pop(udp_addr, None)

        return gate_ids_to_remove

    def cleanup_peer_tracking(self, peer_addr: tuple[str, int]) -> set[str]:
        """Remove TCP and UDP tracking data for a peer address."""
        gate_ids_to_remove = self.cleanup_peer_udp_tracking(peer_addr)
        self.cleanup_peer_tcp_tracking(peer_addr)
        return gate_ids_to_remove

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

    def _get_backpressure_lock(self) -> asyncio.Lock:
        if self._backpressure_lock is None:
            self._backpressure_lock = asyncio.Lock()
        return self._backpressure_lock

    def _update_dc_backpressure_locked(
        self, datacenter_id: str, datacenter_managers: dict[str, list[tuple[str, int]]]
    ) -> None:
        manager_addrs = datacenter_managers.get(datacenter_id, [])
        if not manager_addrs:
            return

        max_level = BackpressureLevel.NONE
        for manager_addr in manager_addrs:
            level = self._manager_backpressure.get(manager_addr, BackpressureLevel.NONE)
            if level > max_level:
                max_level = level

        self._dc_backpressure[datacenter_id] = max_level

    async def update_backpressure(
        self,
        manager_addr: tuple[str, int],
        datacenter_id: str,
        level: BackpressureLevel,
        suggested_delay_ms: int,
        datacenter_managers: dict[str, list[tuple[str, int]]],
    ) -> None:
        async with self._get_backpressure_lock():
            self._manager_backpressure[manager_addr] = level
            self._backpressure_delay_ms = max(
                self._backpressure_delay_ms, suggested_delay_ms
            )
            self._update_dc_backpressure_locked(datacenter_id, datacenter_managers)

    async def clear_manager_backpressure(
        self,
        manager_addr: tuple[str, int],
        datacenter_id: str,
        datacenter_managers: dict[str, list[tuple[str, int]]],
    ) -> None:
        async with self._get_backpressure_lock():
            self._manager_backpressure[manager_addr] = BackpressureLevel.NONE
            self._update_dc_backpressure_locked(datacenter_id, datacenter_managers)

    async def remove_manager_backpressure(self, manager_addr: tuple[str, int]) -> None:
        async with self._get_backpressure_lock():
            self._manager_backpressure.pop(manager_addr, None)

    async def recalculate_dc_backpressure(
        self, datacenter_id: str, datacenter_managers: dict[str, list[tuple[str, int]]]
    ) -> None:
        async with self._get_backpressure_lock():
            self._update_dc_backpressure_locked(datacenter_id, datacenter_managers)

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

    def set_job_reporter_task(
        self, job_id: str, reporter_type: str, task: asyncio.Task
    ) -> None:
        self._job_reporter_tasks.setdefault(job_id, {})[reporter_type] = task

    def remove_job_reporter_task(self, job_id: str, reporter_type: str) -> None:
        job_tasks = self._job_reporter_tasks.get(job_id)
        if not job_tasks:
            return
        job_tasks.pop(reporter_type, None)
        if not job_tasks:
            self._job_reporter_tasks.pop(job_id, None)

    def pop_job_reporter_tasks(self, job_id: str) -> dict[str, asyncio.Task] | None:
        return self._job_reporter_tasks.pop(job_id, None)

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

    def set_client_update_history_limit(self, limit: int) -> None:
        self._client_update_history_limit = max(1, limit)

    async def record_client_update(
        self,
        job_id: str,
        message_type: str,
        payload: bytes,
    ) -> int:
        async with self._get_counter_lock():
            sequence = self._job_update_sequences.get(job_id, 0) + 1
            self._job_update_sequences[job_id] = sequence
            history = self._job_update_history.setdefault(job_id, [])
            history.append((sequence, message_type, payload, time.monotonic()))
            if self._client_update_history_limit > 0:
                excess = len(history) - self._client_update_history_limit
                if excess > 0:
                    del history[:excess]
            return sequence

    async def set_client_update_position(
        self,
        job_id: str,
        callback: tuple[str, int],
        sequence: int,
    ) -> None:
        async with self._get_counter_lock():
            positions = self._job_client_update_positions.setdefault(job_id, {})
            positions[callback] = sequence

    async def get_client_update_position(
        self,
        job_id: str,
        callback: tuple[str, int],
    ) -> int:
        async with self._get_counter_lock():
            return self._job_client_update_positions.get(job_id, {}).get(callback, 0)

    async def get_latest_update_sequence(self, job_id: str) -> int:
        async with self._get_counter_lock():
            return self._job_update_sequences.get(job_id, 0)

    async def get_client_updates_since(
        self,
        job_id: str,
        last_sequence: int,
    ) -> tuple[list[tuple[int, str, bytes, float]], int, int]:
        async with self._get_counter_lock():
            history = list(self._job_update_history.get(job_id, []))
        if not history:
            return [], 0, 0
        oldest_sequence = history[0][0]
        latest_sequence = history[-1][0]
        updates = [entry for entry in history if entry[0] > last_sequence]
        return updates, oldest_sequence, latest_sequence

    async def cleanup_job_update_state(self, job_id: str) -> None:
        async with self._get_counter_lock():
            self._job_update_sequences.pop(job_id, None)
            self._job_update_history.pop(job_id, None)
            self._job_client_update_positions.pop(job_id, None)

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
        self._gate_peer_unhealthy_since.pop(peer_addr, None)

    def cleanup_dead_peer(self, peer_addr: tuple[str, int]) -> set[str]:
        """
        Fully clean up a dead peer from all tracking structures.

        This method removes both TCP-address-keyed and UDP-address-keyed
        data structures to prevent memory leaks from peer churn.

        Args:
            peer_addr: TCP address of the dead peer

        Returns:
            Set of gate IDs cleaned up from peer metadata.
        """
        gate_ids_to_remove = self.cleanup_peer_tracking(peer_addr)

        # Clean up gate_id-keyed structures
        for gate_id in gate_ids_to_remove:
            self._gate_peer_health.pop(gate_id, None)
            self._known_gates.pop(gate_id, None)

        return gate_ids_to_remove

    def is_peer_dead(self, peer_addr: tuple[str, int]) -> bool:
        return peer_addr in self._dead_gate_peers

    def get_unhealthy_peers(self) -> dict[tuple[str, int], float]:
        return dict(self._gate_peer_unhealthy_since)

    def get_dead_peer_timestamps(self) -> dict[tuple[str, int], float]:
        return dict(self._dead_gate_timestamps)

    # Gate UDP/TCP mapping methods
    def set_udp_to_tcp_mapping(
        self, udp_addr: tuple[str, int], tcp_addr: tuple[str, int]
    ) -> None:
        """Set UDP to TCP address mapping for a gate peer."""
        self._gate_udp_to_tcp[udp_addr] = tcp_addr

    def get_tcp_addr_for_udp(self, udp_addr: tuple[str, int]) -> tuple[str, int] | None:
        """Get TCP address for a UDP address."""
        return self._gate_udp_to_tcp.get(udp_addr)

    def get_all_udp_to_tcp_mappings(self) -> dict[tuple[str, int], tuple[str, int]]:
        """Get all UDP to TCP mappings."""
        return dict(self._gate_udp_to_tcp)

    def iter_udp_to_tcp_mappings(self):
        """Iterate over UDP to TCP mappings."""
        return self._gate_udp_to_tcp.items()

    # Active peer methods (additional)
    def get_active_peers(self) -> set[tuple[str, int]]:
        """Get the set of active peers (reference, not copy)."""
        return self._active_gate_peers

    def get_active_peers_list(self) -> list[tuple[str, int]]:
        """Get list of active peers."""
        return list(self._active_gate_peers)

    def has_active_peers(self) -> bool:
        """Check if there are any active peers."""
        return len(self._active_gate_peers) > 0

    def iter_active_peers(self):
        """Iterate over active peers."""
        return iter(self._active_gate_peers)

    # Peer lock methods (synchronous alternative for setdefault pattern)
    def get_or_create_peer_lock_sync(self, peer_addr: tuple[str, int]) -> asyncio.Lock:
        """Get or create peer lock synchronously (for use in sync contexts)."""
        return self._peer_state_locks.setdefault(peer_addr, asyncio.Lock())

    # Gate peer info methods
    def set_gate_peer_heartbeat(
        self, udp_addr: tuple[str, int], heartbeat: GateHeartbeat
    ) -> None:
        """Store heartbeat from a gate peer."""
        self._gate_peer_info[udp_addr] = heartbeat

    def get_gate_peer_heartbeat(
        self, udp_addr: tuple[str, int]
    ) -> GateHeartbeat | None:
        """Get the last heartbeat from a gate peer."""
        return self._gate_peer_info.get(udp_addr)

    # Known gates methods
    def add_known_gate(self, gate_id: str, gate_info: GateInfo) -> None:
        """Add or update a known gate."""
        self._known_gates[gate_id] = gate_info

    def remove_known_gate(self, gate_id: str) -> GateInfo | None:
        """Remove a known gate."""
        return self._known_gates.pop(gate_id, None)

    def get_known_gate(self, gate_id: str) -> GateInfo | None:
        """Get info for a known gate."""
        return self._known_gates.get(gate_id)

    def get_all_known_gates(self) -> list[GateInfo]:
        """Get all known gates."""
        return list(self._known_gates.values())

    def iter_known_gates(self):
        """Iterate over known gates as (gate_id, gate_info) pairs."""
        return self._known_gates.items()
