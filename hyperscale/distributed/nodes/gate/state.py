"""
Gate runtime state for GateServer.

Manages all mutable state including peer tracking, job management,
datacenter health, and metrics.
"""

import asyncio
from operator import attrgetter
from types import MappingProxyType
from typing import Callable, Iterable, Mapping

from hyperscale.distributed.slo.latency_observation import LatencyObservation
from hyperscale.distributed.models import (
    GateHeartbeat,
    GateInfo,
    GateState as GateStateEnum,
    ManagerHeartbeat,
    DatacenterRegistrationState,
    JobSubmission,
    WorkflowResultPush,
    NegotiatedCapabilities,
)
from hyperscale.distributed.health import (
    ManagerHealthState,
)
from hyperscale.distributed.reliability import BackpressureLevel





class GateRuntimeState:
    """
    Runtime state for GateServer.

    Centralizes all mutable dictionaries and tracking structures.
    Provides clean separation between configuration (immutable) and
    runtime state (mutable).

    Lock ordering (acquire in this order to avoid deadlock):
        1. _lock_creation_lock — outermost; held only briefly while creating
           per-resource locks (e.g. _peer_state_locks entries). Never held
           across an await on any other lock here.
        2. _manager_state_lock — guards _datacenter_manager_status and
           _manager_last_status. Acquired only after _lock_creation_lock has
           been released.
        3. _backpressure_lock — guards _manager_backpressure / _dc_backpressure.
           Independent of the manager state lock; never nest the two.
        4. _job_progress_lock — guards _job_progress_sequences and
           _job_progress_seen. Independent of the locks above.
        5. _counter_lock — innermost; pure atomic-increment guard.
           Never held across awaits on any other lock above.

    Per-resource locks (_peer_state_locks[addr]) are leaf locks: acquired
    after the lookup-time critical section in _lock_creation_lock has been
    released, and never held while acquiring any of the locks above.
    """

    def __init__(self, forward_throughput_interval_start: float) -> None:
        """
        Initialize empty state containers.

        Args:
            forward_throughput_interval_start: Monotonic time, on the gate's
                clock, at which the first job-forwarding throughput interval
                (AD-19) begins.
        """
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
        self._job_lease_renewal_tokens: dict[str, str] = {}

        # JobProgress sequence tracking for ordering/dedup (Task 31)
        # Key: (job_id, datacenter_id) -> last_seen_sequence
        self._job_progress_sequences: dict[tuple[str, str], int] = {}
        # Key: (job_id, datacenter_id) -> set of seen (fence_token, timestamp) pairs for dedup
        self._job_progress_seen: dict[tuple[str, str], set[tuple[int, float]]] = {}
        self._job_progress_lock: asyncio.Lock | None = None

        # Cancellation state

        # Progress callbacks
        self._progress_callbacks: dict[str, tuple[str, int]] = {}

        self._client_update_history_limit: int = 200
        self._job_update_sequences: dict[str, int] = {}
        self._job_update_history: dict[str, list[tuple[int, str, bytes, float]]] = {}
        self._job_client_update_positions: dict[str, dict[tuple[str, int], int]] = {}
        # Updates a callback received past a gap in its position -- ones
        # sent after an earlier update that has not reached it.
        self._job_client_updates_delivered_ahead: dict[
            str, dict[tuple[str, int], set[int]]
        ] = {}

        # Leadership/orphan tracking
        self._dead_job_leaders: set[tuple[str, int]] = set()
        self._orphaned_jobs: dict[str, float] = {}
        # Each orphan's leader gate (TCP), when known, and the heartbeats
        # it had sent when the job was orphaned: a leader heard from since
        # may yet renew, so the orphan grace extends for it (AD-26).
        self._orphan_leader_addrs: dict[str, tuple[str, int]] = {}
        self._orphan_heartbeat_baselines: dict[str, int] = {}
        # Heartbeats received from each peer gate (TCP), for as long as it
        # is a peer.
        self._gate_peer_heartbeats_received: dict[tuple[str, int], int] = {}
        # Heartbeats received from every peer gate together: a tier still
        # heard from may yet elect the leader a due orphan waits on.
        self._gate_peer_heartbeats_total: int = 0
        # The longest a due orphan waited for its takeover (by this gate's
        # tier leader or a peer's announcement): the takeover window learns
        # from it.
        self._longest_orphan_takeover_wait_seconds: float = 0.0
        # The longest an orphan waited before its leadership was resolved
        # by a peer: the orphan grace learns from it.
        self._longest_orphan_rescue_seconds: float = 0.0

        # Gate state
        self._gate_state: GateStateEnum = GateStateEnum.SYNCING
        self._state_version: int = 0

        self._gate_peer_unhealthy_since: dict[tuple[str, int], float] = {}
        self._dead_gate_peers: set[tuple[str, int]] = set()
        self._dead_gate_timestamps: dict[tuple[str, int], float] = {}

        # Throughput tracking (AD-19): dispatches a datacenter accepted, and
        # dispatches attempted -- the demand the accepted ones are judged
        # against -- over the same intervals.
        self._forward_throughput_count: int = 0
        self._forward_throughput_interval_start: float = (
            forward_throughput_interval_start
        )
        self._forward_throughput_last_value: float = 0.0
        self._forward_attempt_count: int = 0
        self._forward_attempt_last_value: float = 0.0

    def initialize_locks(self) -> None:
        self._counter_lock = asyncio.Lock()
        self._lock_creation_lock = asyncio.Lock()
        self._manager_state_lock = asyncio.Lock()
        self._backpressure_lock = asyncio.Lock()
        self._job_progress_lock = asyncio.Lock()

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
        self._gate_peer_heartbeats_received.pop(peer_addr, None)
        self.remove_peer_lock(peer_addr)

    def cleanup_peer_udp_tracking(self, peer_addr: tuple[str, int]) -> set[str]:
        """Remove UDP-address-keyed tracking data for a peer."""
        udp_addrs_to_remove = {
            udp_addr
            for udp_addr, tcp_addr in list(self._gate_udp_to_tcp.items())
            if tcp_addr == peer_addr
        }
        udp_addrs_to_remove.update(
            self._udp_addrs_advertising_tcp_addr(peer_addr, udp_addrs_to_remove)
        )
        return self._forget_peer_udp_addrs(udp_addrs_to_remove)

    def _udp_addrs_advertising_tcp_addr(
        self,
        peer_addr: tuple[str, int],
        excluded_udp_addrs: set[tuple[str, int]],
    ) -> list[tuple[str, int]]:
        """Peer UDP addresses (not already excluded) whose heartbeat advertises ``peer_addr`` as TCP."""
        return [
            udp_addr
            for udp_addr, heartbeat in list(self._gate_peer_info.items())
            if self._is_unexcluded_peer_advertising(udp_addr, heartbeat, peer_addr, excluded_udp_addrs)
        ]

    def _is_unexcluded_peer_advertising(
        self,
        udp_addr: tuple[str, int],
        heartbeat: GateHeartbeat,
        peer_addr: tuple[str, int],
        excluded_udp_addrs: set[tuple[str, int]],
    ) -> bool:
        """Whether a not-yet-excluded peer heartbeat advertises ``peer_addr`` as its TCP address."""
        return (
            udp_addr not in excluded_udp_addrs
            and self._peer_heartbeat_tcp_addr(udp_addr, heartbeat) == peer_addr
        )

    @staticmethod
    def _peer_heartbeat_tcp_addr(
        udp_addr: tuple[str, int],
        heartbeat: GateHeartbeat,
    ) -> tuple[str, int]:
        """The TCP address a peer heartbeat advertises, defaulting each part to its UDP address."""
        peer_tcp_host = heartbeat.tcp_host or udp_addr[0]
        peer_tcp_port = heartbeat.tcp_port or udp_addr[1]
        return (peer_tcp_host, peer_tcp_port)

    def _forget_peer_udp_addrs(self, udp_addrs_to_remove: set[tuple[str, int]]) -> set[str]:
        """Drop the UDP-keyed tracking for each address, returning the gate ids they carried."""
        gate_ids_to_remove: set[str] = set()

        for udp_addr in udp_addrs_to_remove:
            if node_id := self._heartbeat_node_id(self._gate_peer_info.get(udp_addr)):
                gate_ids_to_remove.add(node_id)

            self._gate_udp_to_tcp.pop(udp_addr, None)
            self._gate_peer_info.pop(udp_addr, None)

        return gate_ids_to_remove

    @staticmethod
    def _heartbeat_node_id(heartbeat: GateHeartbeat | None) -> str | None:
        """The heartbeat's node id, or None when no heartbeat is held."""
        return heartbeat.node_id if heartbeat else None

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

    def get_stale_manager_addrs(self, stale_cutoff: float) -> list[tuple[str, int]]:
        """Managers whose last heartbeat is older than ``stale_cutoff``."""
        return [
            manager_addr
            for manager_addr, last_status in self._manager_last_status.items()
            if last_status < stale_cutoff
        ]

    async def remove_manager(self, manager_addr: tuple[str, int]) -> None:
        """Forget a departed manager: its heartbeats, health state and
        negotiated capabilities. A manager that returns is re-learned
        from its next heartbeat."""
        async with self._get_manager_state_lock():
            self._manager_last_status.pop(manager_addr, None)
            self._remove_manager_statuses_locked(manager_addr)
        for health_key in self._manager_health_keys(manager_addr):
            del self._manager_health[health_key]
        self._manager_negotiated_caps.pop(manager_addr, None)

    def _remove_manager_statuses_locked(self, manager_addr: tuple[str, int]) -> None:
        """Drop the manager's heartbeat from every datacenter, removing datacenters left empty."""
        for datacenter_id in list(self._datacenter_manager_status):
            datacenter_managers = self._datacenter_manager_status[datacenter_id]
            datacenter_managers.pop(manager_addr, None)
            if not datacenter_managers:
                del self._datacenter_manager_status[datacenter_id]

    def _manager_health_keys(self, manager_addr: tuple[str, int]) -> list[tuple[str, tuple[str, int]]]:
        """Every (datacenter, manager) health key held for the manager."""
        return [key for key in self._manager_health if key[1] == manager_addr]

    def set_job_dc_manager(
        self, job_id: str, datacenter_id: str, manager_addr: tuple[str, int]
    ) -> None:
        """Record the manager ``datacenter_id`` runs ``job_id`` on."""
        self._job_dc_managers.setdefault(job_id, {})[datacenter_id] = manager_addr

    def copy_job_dc_managers(self) -> dict[str, dict[str, tuple[str, int]]]:
        """Every job's DC managers, copied (for a state snapshot)."""
        return {
            job_id: dict(datacenter_managers)
            for job_id, datacenter_managers in self._job_dc_managers.items()
        }

    def clear_job(self, job_id: str) -> None:
        """Drop every per-job entry this state holds for ``job_id``: its
        submission, workflow ids, client callback and DC managers."""
        self._job_submissions.pop(job_id, None)
        self._job_workflow_ids.pop(job_id, None)
        self._progress_callbacks.pop(job_id, None)
        self._job_dc_managers.pop(job_id, None)

    def get_job_dc_managers(self, job_id: str) -> Mapping[str, tuple[str, int]]:
        """The manager each datacenter dispatched ``job_id`` to (read-only)."""
        return MappingProxyType(self._job_dc_managers.get(job_id, {}))

    def get_datacenter_manager_statuses(
        self, datacenter_id: str
    ) -> Mapping[tuple[str, int], ManagerHeartbeat]:
        """Latest heartbeat per manager in a datacenter (read-only view)."""
        return MappingProxyType(self._datacenter_manager_status.get(datacenter_id, {}))

    def get_manager_status(
        self, datacenter_id: str, manager_addr: tuple[str, int]
    ) -> ManagerHeartbeat | None:
        """Get the latest heartbeat for a manager."""
        dc_status = self._datacenter_manager_status.get(datacenter_id, {})
        return dc_status.get(manager_addr)

    def get_dc_slo_routing_factor(self, datacenter_id: str) -> float:
        """AD-42 Phase E5: return the freshest SLO routing factor
        for a DC.

        Picks the manager with the most-recent ``slo_updated_at``
        and returns its pre-computed routing factor. Defaults to
        1.0 (neutral) when:

        * no managers are registered for the DC, or
        * no manager has reported any latency observations yet
          (``slo_sample_count == 0``).

        Used by AD-36 routing scorer to deprioritize DCs that are
        violating their latency SLOs.
        """
        freshest = self._freshest_slo_heartbeat(
            self._datacenter_manager_status.get(datacenter_id, {}).values()
        )
        return 1.0 if freshest is None else freshest.slo_routing_factor

    @staticmethod
    def _freshest_slo_heartbeat(
        heartbeats: Iterable[ManagerHeartbeat],
    ) -> ManagerHeartbeat | None:
        """AD-42: the first heartbeat with the latest ``slo_updated_at`` among those
        reporting latency samples, or None when none has."""
        return max(
            (heartbeat for heartbeat in heartbeats if heartbeat.slo_sample_count > 0),
            key=attrgetter("slo_updated_at"),
            default=None,
        )

    def get_dc_latency_observation(self, datacenter_id: str) -> LatencyObservation | None:
        """AD-42: the datacenter's freshest latency percentiles -- from the
        manager that reported most recently -- or None when none of its
        managers has observed any workflow latency yet."""
        freshest: ManagerHeartbeat | None = None
        for heartbeat in self._datacenter_manager_status.get(datacenter_id, {}).values():
            if heartbeat.slo_sample_count > 0 and (
                freshest is None or heartbeat.slo_updated_at > freshest.slo_updated_at
            ):
                freshest = heartbeat
        if freshest is None:
            return None
        return LatencyObservation(
            target_id=datacenter_id,
            p50_ms=freshest.slo_p50_ms,
            p95_ms=freshest.slo_p95_ms,
            p99_ms=freshest.slo_p99_ms,
            sample_count=freshest.slo_sample_count,
            window_start=freshest.slo_updated_at,
            window_end=freshest.slo_updated_at,
        )

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

    # JobProgress sequence tracking methods (Task 31)
    def _get_job_progress_lock(self) -> asyncio.Lock:
        if self._job_progress_lock is None:
            self._job_progress_lock = asyncio.Lock()
        return self._job_progress_lock

    async def check_and_record_progress(
        self,
        job_id: str,
        datacenter_id: str,
        progress_sequence: int,
        timestamp: float,
    ) -> tuple[bool, str]:
        """
        Check if a JobProgress update should be accepted based on ordering/dedup.

        Uses progress_sequence (per-job per-DC monotonic counter) for ordering,
        NOT fence_token (which is for leadership safety only).

        Returns:
            (accepted, reason) - True if update should be processed, False if rejected
        """
        key = (job_id, datacenter_id)
        dedup_key = (progress_sequence, timestamp)

        async with self._get_job_progress_lock():
            seen_set = self._job_progress_seen.get(key)
            if seen_set is not None and dedup_key in seen_set:
                return (False, "duplicate")

            last_sequence = self._job_progress_sequences.get(key, 0)
            if progress_sequence < last_sequence:
                return (False, "out_of_order")

            if seen_set is None:
                seen_set = set()
                self._job_progress_seen[key] = seen_set

            seen_set.add(dedup_key)
            if len(seen_set) > 100:
                oldest = min(seen_set, key=lambda x: x[1])
                seen_set.discard(oldest)

            if progress_sequence > last_sequence:
                self._job_progress_sequences[key] = progress_sequence

            return (True, "accepted")

    def cleanup_job_progress_tracking(self, job_id: str) -> None:
        """Clean up progress tracking state for a completed job."""
        for key in self._job_progress_keys(job_id):
            self._job_progress_sequences.pop(key, None)
            self._job_progress_seen.pop(key, None)

    def _job_progress_keys(self, job_id: str) -> list[tuple[str, str]]:
        """Every (job, datacenter) progress-sequence key held for the job."""
        return [
            key for key in self._job_progress_sequences if key[0] == job_id
        ]

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

    def mark_job_orphaned(
        self,
        job_id: str,
        timestamp: float,
        leader_addr: tuple[str, int] | None,
    ) -> None:
        """Mark a job as orphaned, led by ``leader_addr`` (TCP) when known."""
        self._orphaned_jobs[job_id] = timestamp
        if leader_addr is None:
            return
        self._orphan_leader_addrs[job_id] = leader_addr
        self._orphan_heartbeat_baselines[job_id] = self._gate_peer_heartbeats_received.get(leader_addr, 0)

    def clear_orphaned_job(self, job_id: str) -> None:
        """Stop tracking an orphan (not a rescue: ended, failed, or taken
        over by this gate)."""
        self._orphaned_jobs.pop(job_id, None)
        self._orphan_leader_addrs.pop(job_id, None)
        self._orphan_heartbeat_baselines.pop(job_id, None)

    def rescue_orphaned_job(self, job_id: str, now: float) -> None:
        """A peer resolved an orphan's leadership: how long that took is a
        rescue the orphan grace learns from."""
        if (orphaned_at := self._orphaned_jobs.get(job_id)) is not None:
            self._longest_orphan_rescue_seconds = max(self._longest_orphan_rescue_seconds, now - orphaned_at)
        self.clear_orphaned_job(job_id)

    def record_gate_peer_heartbeat(self, peer_addr: tuple[str, int]) -> None:
        """A heartbeat from the peer gate at ``peer_addr`` (TCP) arrived."""
        self._gate_peer_heartbeats_received[peer_addr] = self._gate_peer_heartbeats_received.get(peer_addr, 0) + 1
        self._gate_peer_heartbeats_total += 1

    @property
    def gate_peer_heartbeats_total(self) -> int:
        return self._gate_peer_heartbeats_total

    def record_orphan_takeover_wait(self, waited_seconds: float) -> None:
        """A due orphan was taken over ``waited_seconds`` after it came due."""
        self._longest_orphan_takeover_wait_seconds = max(self._longest_orphan_takeover_wait_seconds, waited_seconds)

    @property
    def longest_orphan_takeover_wait_seconds(self) -> float:
        return self._longest_orphan_takeover_wait_seconds

    def orphan_leader_heartbeats(self, job_id: str) -> tuple[int, int] | None:
        """The heartbeats an orphan's leader has sent, and had sent when the
        job was orphaned -- None when its leader is unknown."""
        if (leader_addr := self._orphan_leader_addrs.get(job_id)) is None:
            return None
        return (
            self._gate_peer_heartbeats_received.get(leader_addr, 0),
            self._orphan_heartbeat_baselines[job_id],
        )

    @property
    def longest_orphan_rescue_seconds(self) -> float:
        return self._longest_orphan_rescue_seconds

    def is_job_orphaned(self, job_id: str) -> bool:
        """Check if a job is orphaned."""
        return job_id in self._orphaned_jobs

    def get_orphaned_jobs(self) -> dict[str, float]:
        """Get all orphaned jobs with their timestamps."""
        return dict(self._orphaned_jobs)

    async def record_forward(self) -> None:
        async with self._get_counter_lock():
            self._forward_throughput_count += 1

    async def record_forward_attempt(self) -> None:
        async with self._get_counter_lock():
            self._forward_attempt_count += 1

    def calculate_throughput(self, now: float, interval_seconds: float) -> float:
        """Calculate and reset throughput for the current interval (and the
        attempt rate over the same interval)."""
        elapsed = now - self._forward_throughput_interval_start
        if elapsed >= interval_seconds:
            throughput = (
                self._forward_throughput_count / elapsed if elapsed > 0 else 0.0
            )
            self._forward_throughput_last_value = throughput
            self._forward_attempt_last_value = (
                self._forward_attempt_count / elapsed if elapsed > 0 else 0.0
            )
            self._forward_throughput_count = 0
            self._forward_attempt_count = 0
            self._forward_throughput_interval_start = now
        return self._forward_throughput_last_value

    def get_forward_attempt_rate(self) -> float:
        """Dispatches attempted per second over the last full interval."""
        return self._forward_attempt_last_value

    async def increment_state_version(self) -> int:
        async with self._get_counter_lock():
            self._state_version += 1
            return self._state_version

    def advance_state_version(self) -> int:
        """Synchronous increment for callers that cannot await. It never
        yields, so it cannot interleave with increment_state_version."""
        self._state_version += 1
        return self._state_version

    def adopt_state_version(self, version: int) -> None:
        """Raise the version to a peer snapshot's (never lowers it)."""
        self._state_version = max(self._state_version, version)

    def get_state_version(self) -> int:
        return self._state_version

    def set_client_update_history_limit(self, limit: int) -> None:
        self._client_update_history_limit = max(1, limit)

    async def record_client_update(
        self,
        job_id: str,
        message_type: str,
        payload: bytes,
        recorded_at: float,
    ) -> int:
        async with self._get_counter_lock():
            sequence = self._job_update_sequences.get(job_id, 0) + 1
            self._job_update_sequences[job_id] = sequence
            history = self._job_update_history.setdefault(job_id, [])
            history.append((sequence, message_type, payload, recorded_at))
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
        """Record update ``sequence`` delivered to ``callback``.

        The position is the last sequence below which every update reached
        the callback -- what a replay resends from. An update landing ahead
        of an earlier one that failed (or is still in flight) is held aside
        and does not move it: overwritten with the latest delivery, the
        position skipped the failed update, which no replay then resent.
        A gap older than the retained history closes -- what fell out of
        it cannot be resent -- so the updates held aside never outnumber
        the history.
        """
        async with self._get_counter_lock():
            positions = self._job_client_update_positions.setdefault(job_id, {})
            position = positions.get(callback, 0)
            if sequence <= position:
                return

            positions[callback] = self._advance_client_update_position_locked(
                job_id,
                callback,
                sequence,
                position,
            )

    def _advance_client_update_position_locked(
        self,
        job_id: str,
        callback: tuple[str, int],
        sequence: int,
        position: int,
    ) -> int:
        """Hold ``sequence`` aside, close gaps older than the retained history,
        and return the position the contiguous deliveries now reach."""
        delivered_ahead = self._job_client_updates_delivered_ahead.setdefault(
            job_id, {}
        ).setdefault(callback, set())
        delivered_ahead.add(sequence)
        if history := self._job_update_history.get(job_id):
            position = max(position, history[0][0] - 1)
        return self._drain_contiguous_deliveries(delivered_ahead, position)

    def _drain_contiguous_deliveries(self, delivered_ahead: set[int], position: int) -> int:
        """Advance past every held delivery contiguous with ``position``, then
        drop held deliveries the position has passed."""
        while position + 1 in delivered_ahead:
            position += 1
            delivered_ahead.remove(position)
        self._discard_deliveries_through(delivered_ahead, position)
        return position

    @staticmethod
    def _discard_deliveries_through(delivered_ahead: set[int], position: int) -> None:
        """Drop held deliveries at or below ``position``: the gap they sat past has closed."""
        delivered_ahead.difference_update(
            [held for held in delivered_ahead if held <= position]
        )

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
        updates = self._updates_after(history, last_sequence)
        return updates, oldest_sequence, latest_sequence

    @staticmethod
    def _updates_after(
        history: list[tuple[int, str, bytes, float]],
        last_sequence: int,
    ) -> list[tuple[int, str, bytes, float]]:
        """The retained updates whose sequence is past ``last_sequence``."""
        return [entry for entry in history if entry[0] > last_sequence]

    async def cleanup_job_update_state(self, job_id: str) -> None:
        async with self._get_counter_lock():
            self._job_update_sequences.pop(job_id, None)
            self._job_update_history.pop(job_id, None)
            self._job_client_update_positions.pop(job_id, None)
            self._job_client_updates_delivered_ahead.pop(job_id, None)

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
        """Get list of active peers, in address order: an iteration order
        that does not follow string hashing (one schedule per seed)."""
        return sorted(self._active_gate_peers)

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

    def iter_gate_peer_heartbeats(self):
        """Iterate over (udp_addr, last heartbeat) for every gate peer."""
        return iter(list(self._gate_peer_info.items()))

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
        return list(self._known_gates.values())

    def get_known_gate_count(self) -> int:
        return len(self._known_gates)

    def iter_known_gates(self):
        return self._known_gates.items()
