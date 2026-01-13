"""
Manager runtime state for ManagerServer.

Manages all mutable state including worker tracking, peer management,
job leadership, cancellation tracking, and metrics.
"""

import asyncio
from collections import defaultdict, deque
from typing import TYPE_CHECKING

from hyperscale.distributed.models import (
    GateInfo,
    ManagerInfo,
    ManagerHeartbeat,
    WorkerRegistration,
    CancelledWorkflowInfo,
    JobSubmission,
    ProvisionRequest,
    ManagerState as ManagerStateEnum,
)
from hyperscale.distributed.server.events import VersionedStateClock
from hyperscale.distributed.swim.core import ErrorStats
from hyperscale.distributed.protocol.version import NegotiatedCapabilities
from hyperscale.distributed.slo import TimeWindowedTDigest

if TYPE_CHECKING:
    from hyperscale.core.state.context import Context
    from hyperscale.distributed.jobs.timeout_strategy import TimeoutStrategy
    from hyperscale.distributed.workflow import WorkflowStateMachine
    from hyperscale.reporting.common.results_types import WorkflowStats
    from hyperscale.distributed.slo import LatencyObservation


class ManagerState:
    """
    Runtime state for ManagerServer.

    Centralizes all mutable dictionaries and tracking structures.
    Provides clean separation between configuration (immutable) and
    runtime state (mutable).
    """

    def __init__(self) -> None:
        """Initialize empty state containers."""
        # Counter protection lock (for race-free increments)
        self._counter_lock: asyncio.Lock | None = None

        # Lock for creating per-resource locks and semaphores
        self._resource_creation_lock: asyncio.Lock | None = None

        # Gate tracking
        self._known_gates: dict[str, GateInfo] = {}
        self._healthy_gate_ids: set[str] = set()
        self._primary_gate_id: str | None = None
        self._gate_udp_to_tcp: dict[tuple[str, int], tuple[str, int]] = {}
        self._gate_state_locks: dict[str, asyncio.Lock] = {}
        self._gate_state_epoch: dict[str, int] = {}
        self._current_gate_leader_id: str | None = None
        self._current_gate_leader_addr: tuple[str, int] | None = None
        self._gate_negotiated_caps: dict[str, NegotiatedCapabilities] = {}
        self._gate_unhealthy_since: dict[str, float] = {}

        # Manager peer tracking
        self._known_manager_peers: dict[str, ManagerInfo] = {}
        self._manager_udp_to_tcp: dict[tuple[str, int], tuple[str, int]] = {}
        self._active_manager_peer_ids: set[str] = set()
        self._active_manager_peers: set[tuple[str, int]] = set()
        self._peer_state_locks: dict[tuple[str, int], asyncio.Lock] = {}
        self._peer_state_epoch: dict[tuple[str, int], int] = {}
        self._manager_peer_info: dict[tuple[str, int], ManagerHeartbeat] = {}
        self._registered_with_managers: set[str] = set()
        self._manager_peer_unhealthy_since: dict[str, float] = {}
        self._dead_managers: set[tuple[str, int]] = set()
        self._dead_manager_timestamps: dict[tuple[str, int], float] = {}
        self._peer_manager_health_states: dict[str, str] = {}
        self._dc_leader_manager_id: str | None = None
        self._recovery_verification_pending: dict[tuple[str, int], float] = {}
        self._last_leader_heartbeat_at: float = 0.0
        self._consecutive_quorum_failures: int = 0

        # Worker tracking
        self._workers: dict[str, WorkerRegistration] = {}
        self._worker_addr_to_id: dict[tuple[str, int], str] = {}
        self._worker_circuits: dict[str, ErrorStats] = {}
        self._worker_unhealthy_since: dict[str, float] = {}
        self._worker_deadlines: dict[str, float] = {}
        self._worker_job_last_progress: dict[tuple[str, str], float] = {}
        self._worker_health_states: dict[str, str] = {}
        self._dispatch_semaphores: dict[str, asyncio.Semaphore] = {}

        # Versioned state clock
        self._versioned_clock: VersionedStateClock = VersionedStateClock()

        # Quorum protocol state
        self._pending_provisions: dict[str, ProvisionRequest] = {}
        self._provision_confirmations: dict[str, set[str]] = {}

        # Job leader tracking (Context Consistency Protocol)
        self._job_leaders: dict[str, str] = {}
        self._job_leader_addrs: dict[str, tuple[str, int]] = {}
        self._job_fencing_tokens: dict[str, int] = {}
        self._job_layer_version: dict[str, int] = {}
        self._job_contexts: dict[str, "Context"] = {}
        self._context_lamport_clock: int = 0

        # Client callbacks
        self._job_callbacks: dict[str, tuple[str, int]] = {}
        self._client_callbacks: dict[str, tuple[str, int]] = {}
        self._job_origin_gates: dict[str, tuple[str, int]] = {}
        self._progress_callbacks: dict[str, tuple[str, int]] = {}

        # Cancellation tracking (AD-20)
        self._cancellation_pending_workflows: dict[str, set[str]] = defaultdict(set)
        self._cancellation_errors: dict[str, list[str]] = defaultdict(list)
        self._cancellation_completion_events: dict[str, asyncio.Event] = {}
        self._cancellation_initiated_at: dict[str, float] = {}
        self._cancelled_workflows: dict[str, CancelledWorkflowInfo] = {}
        self._workflow_cancellation_locks: dict[str, asyncio.Lock] = {}

        # Workflow lifecycle (AD-33)
        self._workflow_lifecycle_states: "WorkflowStateMachine | None" = None
        self._workflow_completion_events: dict[str, asyncio.Event] = {}

        # Job tracking
        self._job_submissions: dict[str, JobSubmission] = {}
        self._job_reporter_tasks: dict[str, dict[str, asyncio.Task]] = {}
        self._workflow_retries: dict[str, tuple[int, bytes, set[str]]] = {}
        self._job_timeout_strategies: dict[str, "TimeoutStrategy"] = {}
        self._job_aggregated_results: dict[str, list["WorkflowStats"]] = defaultdict(
            list
        )

        # Core allocation
        self._cores_available_event: asyncio.Event = asyncio.Event()
        self._core_allocation_lock: asyncio.Lock | None = None
        self._eager_dispatch_lock: asyncio.Lock | None = None

        # State versioning and manager state
        self._fence_token: int = 0
        self._state_version: int = 0
        self._external_incarnation: int = 0
        self._manager_state: ManagerStateEnum = ManagerStateEnum.SYNCING

        # Latency tracking (bounded deques to prevent memory leaks)
        self._max_latency_samples: int = 1000
        self._gate_latency_samples: deque[tuple[float, float]] = deque(
            maxlen=self._max_latency_samples
        )
        self._peer_manager_latency_samples: dict[str, deque[tuple[float, float]]] = {}
        self._worker_latency_samples: dict[str, deque[tuple[float, float]]] = {}

        # Throughput tracking (AD-19)
        self._dispatch_throughput_count: int = 0
        self._dispatch_throughput_interval_start: float = 0.0
        self._dispatch_throughput_last_value: float = 0.0
        self._dispatch_failure_count: int = 0

        self._workflow_latency_digest = TimeWindowedTDigest()

        # Background tasks
        self._dead_node_reap_task: asyncio.Task | None = None
        self._orphan_scan_task: asyncio.Task | None = None
        self._discovery_maintenance_task: asyncio.Task | None = None

    def initialize_locks(self) -> None:
        self._core_allocation_lock = asyncio.Lock()
        self._eager_dispatch_lock = asyncio.Lock()
        self._counter_lock = asyncio.Lock()
        self._resource_creation_lock = asyncio.Lock()

    def _get_counter_lock(self) -> asyncio.Lock:
        if self._counter_lock is None:
            self._counter_lock = asyncio.Lock()
        return self._counter_lock

    def _get_resource_creation_lock(self) -> asyncio.Lock:
        if self._resource_creation_lock is None:
            self._resource_creation_lock = asyncio.Lock()
        return self._resource_creation_lock

    async def get_peer_state_lock(self, peer_addr: tuple[str, int]) -> asyncio.Lock:
        async with self._get_resource_creation_lock():
            if peer_addr not in self._peer_state_locks:
                self._peer_state_locks[peer_addr] = asyncio.Lock()
            return self._peer_state_locks[peer_addr]

    async def get_gate_state_lock(self, gate_id: str) -> asyncio.Lock:
        async with self._get_resource_creation_lock():
            if gate_id not in self._gate_state_locks:
                self._gate_state_locks[gate_id] = asyncio.Lock()
            return self._gate_state_locks[gate_id]

    async def get_workflow_cancellation_lock(self, workflow_id: str) -> asyncio.Lock:
        async with self._get_resource_creation_lock():
            if workflow_id not in self._workflow_cancellation_locks:
                self._workflow_cancellation_locks[workflow_id] = asyncio.Lock()
            return self._workflow_cancellation_locks[workflow_id]

    async def get_dispatch_semaphore(
        self, worker_id: str, max_concurrent: int
    ) -> asyncio.Semaphore:
        async with self._get_resource_creation_lock():
            if worker_id not in self._dispatch_semaphores:
                self._dispatch_semaphores[worker_id] = asyncio.Semaphore(max_concurrent)
            return self._dispatch_semaphores[worker_id]

    async def get_peer_latency_samples(
        self, peer_id: str
    ) -> deque[tuple[float, float]]:
        async with self._get_resource_creation_lock():
            if peer_id not in self._peer_manager_latency_samples:
                self._peer_manager_latency_samples[peer_id] = deque(
                    maxlen=self._max_latency_samples
                )
            return self._peer_manager_latency_samples[peer_id]

    async def get_worker_latency_samples(
        self, worker_id: str
    ) -> deque[tuple[float, float]]:
        async with self._get_resource_creation_lock():
            if worker_id not in self._worker_latency_samples:
                self._worker_latency_samples[worker_id] = deque(
                    maxlen=self._max_latency_samples
                )
            return self._worker_latency_samples[worker_id]

    async def increment_fence_token(self) -> int:
        async with self._get_counter_lock():
            self._fence_token += 1
            return self._fence_token

    async def increment_state_version(self) -> int:
        async with self._get_counter_lock():
            self._state_version += 1
            return self._state_version

    async def increment_external_incarnation(self) -> int:
        async with self._get_counter_lock():
            self._external_incarnation += 1
            return self._external_incarnation

    async def increment_context_lamport_clock(self) -> int:
        async with self._get_counter_lock():
            self._context_lamport_clock += 1
            return self._context_lamport_clock

    def get_active_peer_count(self) -> int:
        """Get count of active manager peers (including self)."""
        return len(self._active_manager_peers) + 1

    async def is_peer_active(self, tcp_addr: tuple[str, int]) -> bool:
        async with self._get_counter_lock():
            return tcp_addr in self._active_manager_peers

    async def add_active_peer(self, tcp_addr: tuple[str, int], node_id: str) -> None:
        async with self._get_counter_lock():
            self._active_manager_peers.add(tcp_addr)
            self._active_manager_peer_ids.add(node_id)

    async def remove_active_peer(self, tcp_addr: tuple[str, int], node_id: str) -> None:
        async with self._get_counter_lock():
            self._active_manager_peers.discard(tcp_addr)
            self._active_manager_peer_ids.discard(node_id)

    def clear_cancellation_state(self, job_id: str) -> None:
        """Clear cancellation tracking state for a job."""
        self._cancellation_pending_workflows.pop(job_id, None)
        self._cancellation_errors.pop(job_id, None)
        self._cancellation_completion_events.pop(job_id, None)
        self._cancellation_initiated_at.pop(job_id, None)

    def clear_job_state(self, job_id: str) -> None:
        self._job_leaders.pop(job_id, None)
        self._job_leader_addrs.pop(job_id, None)
        self._job_fencing_tokens.pop(job_id, None)
        self._job_layer_version.pop(job_id, None)
        self._job_contexts.pop(job_id, None)
        self._job_callbacks.pop(job_id, None)
        self._client_callbacks.pop(job_id, None)
        self._job_origin_gates.pop(job_id, None)
        self._progress_callbacks.pop(job_id, None)
        self._job_submissions.pop(job_id, None)
        reporter_tasks = self._job_reporter_tasks.pop(job_id, None)
        if reporter_tasks:
            for task in reporter_tasks.values():
                if not task.done():
                    task.cancel()
        self._job_timeout_strategies.pop(job_id, None)
        self._job_aggregated_results.pop(job_id, None)
        self.clear_cancellation_state(job_id)
        self._workflow_cancellation_locks.pop(job_id, None)

    def remove_gate_lock(self, gate_id: str) -> None:
        """Remove lock when gate disconnects to prevent memory leak."""
        self._gate_state_locks.pop(gate_id, None)
        self._gate_state_epoch.pop(gate_id, None)

    def remove_peer_lock(self, peer_addr: tuple[str, int]) -> None:
        """Remove lock when manager peer disconnects to prevent memory leak."""
        self._peer_state_locks.pop(peer_addr, None)
        self._peer_state_epoch.pop(peer_addr, None)

    def remove_worker_state(self, worker_id: str) -> None:
        """Remove all state associated with a dead worker to prevent memory leaks."""
        self._worker_latency_samples.pop(worker_id, None)
        self._worker_circuits.pop(worker_id, None)
        self._worker_unhealthy_since.pop(worker_id, None)
        self._worker_deadlines.pop(worker_id, None)
        self._worker_health_states.pop(worker_id, None)
        self._dispatch_semaphores.pop(worker_id, None)

        progress_keys_to_remove = [
            key for key in self._worker_job_last_progress if key[0] == worker_id
        ]
        for key in progress_keys_to_remove:
            self._worker_job_last_progress.pop(key, None)

    def get_quorum_metrics(self) -> dict:
        """Get quorum-related metrics."""
        return {
            "active_peer_count": len(self._active_manager_peers),
            "known_peer_count": len(self._known_manager_peers),
            "dead_manager_count": len(self._dead_managers),
            "pending_provision_count": len(self._pending_provisions),
        }

    def get_worker_metrics(self) -> dict:
        """Get worker-related metrics."""
        return {
            "worker_count": len(self._workers),
            "unhealthy_worker_count": len(self._worker_unhealthy_since),
            "worker_circuits_count": len(self._worker_circuits),
        }

    def get_gate_metrics(self) -> dict:
        """Get gate-related metrics."""
        return {
            "known_gate_count": len(self._known_gates),
            "healthy_gate_count": len(self._healthy_gate_ids),
            "unhealthy_gate_count": len(self._gate_unhealthy_since),
            "has_gate_leader": self._current_gate_leader_id is not None,
        }

    def get_job_metrics(self) -> dict:
        """Get job-related metrics."""
        return {
            "job_leader_count": len(self._job_leaders),
            "job_callback_count": len(self._job_callbacks),
            "job_submission_count": len(self._job_submissions),
            "cancelled_workflow_count": len(self._cancelled_workflows),
            "pending_cancellation_count": len(self._cancellation_pending_workflows),
        }

    def record_workflow_latency(self, latency_ms: float) -> None:
        self._workflow_latency_digest.add(latency_ms)

    def get_workflow_latency_observation(self) -> "LatencyObservation | None":
        return self._workflow_latency_digest.get_recent_observation(
            target_id="workflows"
        )

    # =========================================================================
    # Worker Accessors (16 direct accesses)
    # =========================================================================

    def get_worker(self, worker_id: str) -> WorkerRegistration | None:
        return self._workers.get(worker_id)

    def get_all_workers(self) -> dict[str, WorkerRegistration]:
        return self._workers

    def iter_workers(self) -> list[tuple[str, WorkerRegistration]]:
        return list(self._workers.items())

    def add_worker(self, worker_id: str, worker: WorkerRegistration) -> None:
        self._workers[worker_id] = worker

    def remove_worker(self, worker_id: str) -> WorkerRegistration | None:
        return self._workers.pop(worker_id, None)

    def has_worker(self, worker_id: str) -> bool:
        return worker_id in self._workers

    def get_worker_count(self) -> int:
        return len(self._workers)

    def get_worker_ids(self) -> list[str]:
        return list(self._workers.keys())

    def get_worker_id_from_addr(self, addr: tuple[str, int]) -> str | None:
        return self._worker_addr_to_id.get(addr)

    def set_worker_addr_mapping(self, addr: tuple[str, int], worker_id: str) -> None:
        self._worker_addr_to_id[addr] = worker_id

    def remove_worker_addr_mapping(self, addr: tuple[str, int]) -> None:
        self._worker_addr_to_id.pop(addr, None)

    # =========================================================================
    # State Version Accessors (9 direct accesses)
    # =========================================================================

    @property
    def state_version(self) -> int:
        return self._state_version

    def set_state_version(self, version: int) -> None:
        self._state_version = version

    def set_state_version_if_higher(self, version: int) -> bool:
        if version > self._state_version:
            self._state_version = version
            return True
        return False

    # =========================================================================
    # Active Manager Peers Accessors (8 direct accesses)
    # =========================================================================

    def get_active_manager_peers(self) -> set[tuple[str, int]]:
        return self._active_manager_peers

    def get_active_manager_peer_ids(self) -> set[str]:
        return self._active_manager_peer_ids

    def add_active_manager_peer(self, addr: tuple[str, int]) -> None:
        self._active_manager_peers.add(addr)

    def remove_active_manager_peer(self, addr: tuple[str, int]) -> None:
        self._active_manager_peers.discard(addr)

    # =========================================================================
    # Job Timeout Strategies Accessors (7 direct accesses)
    # =========================================================================

    def get_job_timeout_strategy(self, job_id: str) -> "TimeoutStrategy | None":
        return self._job_timeout_strategies.get(job_id)

    def set_job_timeout_strategy(
        self, job_id: str, strategy: "TimeoutStrategy"
    ) -> None:
        self._job_timeout_strategies[job_id] = strategy

    def iter_job_timeout_strategies(
        self,
    ) -> list[tuple[str, "TimeoutStrategy"]]:
        return list(self._job_timeout_strategies.items())

    def remove_job_timeout_strategy(self, job_id: str) -> "TimeoutStrategy | None":
        return self._job_timeout_strategies.pop(job_id, None)

    # =========================================================================
    # Job Contexts Accessors (7 direct accesses)
    # =========================================================================

    def get_job_context(self, job_id: str) -> "Context | None":
        return self._job_contexts.get(job_id)

    def set_job_context(self, job_id: str, context: "Context") -> None:
        self._job_contexts[job_id] = context

    def has_job_context(self, job_id: str) -> bool:
        return job_id in self._job_contexts

    def get_or_create_job_context(self, job_id: str) -> "Context":
        """Get existing job context or create a new one if it doesn't exist."""
        context = self._job_contexts.get(job_id)
        if context is None:
            context = Context()
            self._job_contexts[job_id] = context
        return context

    # =========================================================================
    # Cancelled Workflows Accessors (7 direct accesses)
    # =========================================================================

    def get_cancelled_workflow(self, workflow_id: str) -> CancelledWorkflowInfo | None:
        return self._cancelled_workflows.get(workflow_id)

    def set_cancelled_workflow(
        self, workflow_id: str, info: CancelledWorkflowInfo
    ) -> None:
        self._cancelled_workflows[workflow_id] = info

    def has_cancelled_workflow(self, workflow_id: str) -> bool:
        return workflow_id in self._cancelled_workflows

    def iter_cancelled_workflows(self) -> list[tuple[str, CancelledWorkflowInfo]]:
        return list(self._cancelled_workflows.items())

    # =========================================================================
    # Known Manager Peers Accessors (6 direct accesses)
    # =========================================================================

    def get_known_manager_peer(self, peer_id: str) -> ManagerInfo | None:
        return self._known_manager_peers.get(peer_id)

    def set_known_manager_peer(self, peer_id: str, info: ManagerInfo) -> None:
        self._known_manager_peers[peer_id] = info

    def remove_known_manager_peer(self, peer_id: str) -> ManagerInfo | None:
        return self._known_manager_peers.pop(peer_id, None)

    def iter_known_manager_peers(self) -> list[tuple[str, ManagerInfo]]:
        return list(self._known_manager_peers.items())

    def get_known_manager_peer_values(self) -> list[ManagerInfo]:
        return list(self._known_manager_peers.values())

    def has_known_manager_peer(self, peer_id: str) -> bool:
        return peer_id in self._known_manager_peers

    def get_known_manager_peer_count(self) -> int:
        return len(self._known_manager_peers)

    # =========================================================================
    # Known Gates Accessors (6 direct accesses)
    # =========================================================================

    def get_known_gate(self, gate_id: str) -> GateInfo | None:
        return self._known_gates.get(gate_id)

    def set_known_gate(self, gate_id: str, info: GateInfo) -> None:
        self._known_gates[gate_id] = info

    def remove_known_gate(self, gate_id: str) -> GateInfo | None:
        return self._known_gates.pop(gate_id, None)

    def iter_known_gates(self) -> list[tuple[str, GateInfo]]:
        return list(self._known_gates.items())

    def get_known_gate_values(self) -> list[GateInfo]:
        return list(self._known_gates.values())

    # =========================================================================
    # Job Leaders Accessors (6 direct accesses)
    # =========================================================================

    def get_job_leader(self, job_id: str) -> str | None:
        return self._job_leaders.get(job_id)

    def set_job_leader(self, job_id: str, leader_id: str) -> None:
        self._job_leaders[job_id] = leader_id

    def has_job_leader(self, job_id: str) -> bool:
        return job_id in self._job_leaders

    def get_job_leader_addr(self, job_id: str) -> tuple[str, int] | None:
        return self._job_leader_addrs.get(job_id)

    def set_job_leader_addr(self, job_id: str, addr: tuple[str, int]) -> None:
        self._job_leader_addrs[job_id] = addr

    def iter_job_leaders(self) -> list[tuple[str, str]]:
        return list(self._job_leaders.items())

    def update_job_leaders(self, leaders: dict[str, str]) -> None:
        self._job_leaders.update(leaders)

    def update_job_leader_addrs(self, addrs: dict[str, tuple[str, int]]) -> None:
        self._job_leader_addrs.update(addrs)

    # =========================================================================
    # Worker Health Accessors (5 direct accesses each)
    # =========================================================================

    def get_worker_unhealthy_since(self, worker_id: str) -> float | None:
        return self._worker_unhealthy_since.get(worker_id)

    def set_worker_unhealthy_since(self, worker_id: str, timestamp: float) -> None:
        self._worker_unhealthy_since[worker_id] = timestamp

    def clear_worker_unhealthy_since(self, worker_id: str) -> None:
        self._worker_unhealthy_since.pop(worker_id, None)

    def setdefault_worker_unhealthy_since(
        self, worker_id: str, timestamp: float
    ) -> float:
        return self._worker_unhealthy_since.setdefault(worker_id, timestamp)

    def iter_worker_unhealthy_since(self) -> list[tuple[str, float]]:
        return list(self._worker_unhealthy_since.items())

    def has_worker_unhealthy_since(self, worker_id: str) -> bool:
        return worker_id in self._worker_unhealthy_since

    def get_worker_deadline(self, worker_id: str) -> float | None:
        return self._worker_deadlines.get(worker_id)

    def set_worker_deadline(self, worker_id: str, deadline: float) -> None:
        self._worker_deadlines[worker_id] = deadline

    def clear_worker_deadline(self, worker_id: str) -> None:
        self._worker_deadlines.pop(worker_id, None)

    def iter_worker_deadlines(self) -> list[tuple[str, float]]:
        return list(self._worker_deadlines.items())

    # =========================================================================
    # Manager Peer Health Accessors (5 direct accesses)
    # =========================================================================

    def get_peer_state_epoch(self, peer_addr: tuple[str, int]) -> int:
        return self._peer_state_epoch.get(peer_addr, 0)

    def set_peer_state_epoch(self, peer_addr: tuple[str, int], epoch: int) -> None:
        self._peer_state_epoch[peer_addr] = epoch

    def increment_peer_state_epoch(self, peer_addr: tuple[str, int]) -> int:
        new_epoch = self._peer_state_epoch.get(peer_addr, 0) + 1
        self._peer_state_epoch[peer_addr] = new_epoch
        return new_epoch

    def get_manager_tcp_from_udp(
        self, udp_addr: tuple[str, int]
    ) -> tuple[str, int] | None:
        return self._manager_udp_to_tcp.get(udp_addr)

    def set_manager_udp_to_tcp_mapping(
        self, udp_addr: tuple[str, int], tcp_addr: tuple[str, int]
    ) -> None:
        self._manager_udp_to_tcp[udp_addr] = tcp_addr

    def get_dead_managers(self) -> set[tuple[str, int]]:
        return self._dead_managers

    def add_dead_manager(self, addr: tuple[str, int], timestamp: float) -> None:
        self._dead_managers.add(addr)
        self._dead_manager_timestamps[addr] = timestamp

    def remove_dead_manager(self, addr: tuple[str, int]) -> None:
        self._dead_managers.discard(addr)
        self._dead_manager_timestamps.pop(addr, None)

    def get_dead_manager_timestamp(self, addr: tuple[str, int]) -> float | None:
        return self._dead_manager_timestamps.get(addr)

    # =========================================================================
    # Gate Leader Accessors (5 direct accesses)
    # =========================================================================

    @property
    def current_gate_leader_addr(self) -> tuple[str, int] | None:
        return self._current_gate_leader_addr

    def set_current_gate_leader(
        self, gate_id: str | None, addr: tuple[str, int] | None
    ) -> None:
        self._current_gate_leader_id = gate_id
        self._current_gate_leader_addr = addr

    @property
    def current_gate_leader_id(self) -> str | None:
        return self._current_gate_leader_id

    # =========================================================================
    # Job Origin Gates Accessors (4 direct accesses)
    # =========================================================================

    def get_job_origin_gate(self, job_id: str) -> tuple[str, int] | None:
        return self._job_origin_gates.get(job_id)

    def set_job_origin_gate(self, job_id: str, addr: tuple[str, int]) -> None:
        self._job_origin_gates[job_id] = addr

    # =========================================================================
    # Job Layer Version Accessors (4 direct accesses)
    # =========================================================================

    def get_job_layer_version(self, job_id: str, default: int = 0) -> int:
        return self._job_layer_version.get(job_id, default)

    def set_job_layer_version(self, job_id: str, version: int) -> None:
        self._job_layer_version[job_id] = version

    def setdefault_job_layer_version(self, job_id: str, default: int = 0) -> int:
        return self._job_layer_version.setdefault(job_id, default)

    def increment_job_layer_version(self, job_id: str) -> int:
        current = self._job_layer_version.get(job_id, 0)
        self._job_layer_version[job_id] = current + 1
        return current + 1

    # =========================================================================
    # Gate UDP to TCP Mapping Accessors (4 direct accesses)
    # =========================================================================

    def get_gate_tcp_from_udp(
        self, udp_addr: tuple[str, int]
    ) -> tuple[str, int] | None:
        return self._gate_udp_to_tcp.get(udp_addr)

    def set_gate_udp_to_tcp_mapping(
        self, udp_addr: tuple[str, int], tcp_addr: tuple[str, int]
    ) -> None:
        self._gate_udp_to_tcp[udp_addr] = tcp_addr

    # =========================================================================
    # Quorum Failure Accessors (4 direct accesses)
    # =========================================================================

    @property
    def consecutive_quorum_failures(self) -> int:
        return self._consecutive_quorum_failures

    def increment_quorum_failures(self) -> int:
        self._consecutive_quorum_failures += 1
        return self._consecutive_quorum_failures

    def reset_quorum_failures(self) -> None:
        self._consecutive_quorum_failures = 0

    # =========================================================================
    # Primary Gate Accessors (3 direct accesses)
    # =========================================================================

    @property
    def primary_gate_id(self) -> str | None:
        return self._primary_gate_id

    def set_primary_gate_id(self, gate_id: str | None) -> None:
        self._primary_gate_id = gate_id

    # =========================================================================
    # Job Callbacks Accessors (3 direct accesses)
    # =========================================================================

    def get_job_callback(self, job_id: str) -> tuple[str, int] | None:
        return self._job_callbacks.get(job_id)

    def set_job_callback(self, job_id: str, addr: tuple[str, int]) -> None:
        self._job_callbacks[job_id] = addr

    # =========================================================================
    # Dispatch Throughput Accessors (3 direct accesses each)
    # =========================================================================

    @property
    def dispatch_throughput_count(self) -> int:
        return self._dispatch_throughput_count

    def increment_dispatch_throughput_count(self) -> None:
        self._dispatch_throughput_count += 1

    def reset_dispatch_throughput(
        self, interval_start: float, last_value: float
    ) -> None:
        self._dispatch_throughput_count = 0
        self._dispatch_throughput_interval_start = interval_start
        self._dispatch_throughput_last_value = last_value

    @property
    def dispatch_throughput_interval_start(self) -> float:
        return self._dispatch_throughput_interval_start

    @property
    def dispatch_throughput_last_value(self) -> float:
        return self._dispatch_throughput_last_value

    # =========================================================================
    # Workflow Retries Accessors (2 direct accesses)
    # =========================================================================

    def get_workflow_retry(
        self, workflow_id: str
    ) -> tuple[int, bytes, set[str]] | None:
        return self._workflow_retries.get(workflow_id)

    def set_workflow_retry(
        self, workflow_id: str, retry_data: tuple[int, bytes, set[str]]
    ) -> None:
        self._workflow_retries[workflow_id] = retry_data

    def remove_workflow_retry(self, workflow_id: str) -> None:
        self._workflow_retries.pop(workflow_id, None)

    def iter_workflow_retries_for_job(
        self, job_id: str
    ) -> list[tuple[str, tuple[int, bytes, set[str]]]]:
        return [
            (wf_id, data)
            for wf_id, data in self._workflow_retries.items()
            if wf_id.startswith(f"{job_id}:")
        ]

    # =========================================================================
    # Workflow Completion Events Accessors (2 direct accesses)
    # =========================================================================

    def get_workflow_completion_event(self, workflow_id: str) -> asyncio.Event | None:
        return self._workflow_completion_events.get(workflow_id)

    def set_workflow_completion_event(
        self, workflow_id: str, event: asyncio.Event
    ) -> None:
        self._workflow_completion_events[workflow_id] = event

    def remove_workflow_completion_event(self, workflow_id: str) -> None:
        self._workflow_completion_events.pop(workflow_id, None)

    # =========================================================================
    # Progress Callbacks Accessors (2 direct accesses)
    # =========================================================================

    def get_progress_callback(self, job_id: str) -> tuple[str, int] | None:
        return self._progress_callbacks.get(job_id)

    def set_progress_callback(self, job_id: str, addr: tuple[str, int]) -> None:
        self._progress_callbacks[job_id] = addr

    # =========================================================================
    # Peer Manager Health States Accessors (2 direct accesses)
    # =========================================================================

    def get_peer_manager_health_state(self, peer_id: str) -> str | None:
        return self._peer_manager_health_states.get(peer_id)

    def set_peer_manager_health_state(self, peer_id: str, state: str) -> None:
        self._peer_manager_health_states[peer_id] = state

    # =========================================================================
    # Job Submissions Accessors (2 direct accesses)
    # =========================================================================

    def get_job_submission(self, job_id: str) -> JobSubmission | None:
        return self._job_submissions.get(job_id)

    def set_job_submission(self, job_id: str, submission: JobSubmission) -> None:
        self._job_submissions[job_id] = submission

    # =========================================================================
    # Healthy Gate IDs Accessors (2 direct accesses)
    # =========================================================================

    def get_healthy_gate_ids(self) -> set[str]:
        return self._healthy_gate_ids

    def get_first_healthy_gate_id(self) -> str | None:
        for gate_id in self._healthy_gate_ids:
            return gate_id
        return None

    def add_healthy_gate_id(self, gate_id: str) -> None:
        self._healthy_gate_ids.add(gate_id)

    def remove_healthy_gate_id(self, gate_id: str) -> None:
        self._healthy_gate_ids.discard(gate_id)

    # =========================================================================
    # Cancellation Accessors (2 direct accesses each)
    # =========================================================================

    def get_cancellation_pending_workflows(self, job_id: str) -> set[str]:
        return self._cancellation_pending_workflows.get(job_id, set())

    def add_cancellation_pending_workflow(self, job_id: str, workflow_id: str) -> None:
        self._cancellation_pending_workflows[job_id].add(workflow_id)

    def remove_cancellation_pending_workflow(
        self, job_id: str, workflow_id: str
    ) -> None:
        if job_id in self._cancellation_pending_workflows:
            self._cancellation_pending_workflows[job_id].discard(workflow_id)

    def get_cancellation_errors(self, job_id: str) -> list[str]:
        return self._cancellation_errors.get(job_id, [])

    def add_cancellation_error(self, job_id: str, error: str) -> None:
        self._cancellation_errors[job_id].append(error)

    def get_cancellation_completion_event(self, job_id: str) -> asyncio.Event | None:
        return self._cancellation_completion_events.get(job_id)

    def set_cancellation_completion_event(
        self, job_id: str, event: asyncio.Event
    ) -> None:
        self._cancellation_completion_events[job_id] = event

    def get_cancellation_initiated_at(self, job_id: str) -> float | None:
        return self._cancellation_initiated_at.get(job_id)

    def set_cancellation_initiated_at(self, job_id: str, timestamp: float) -> None:
        self._cancellation_initiated_at[job_id] = timestamp

    # =========================================================================
    # Single-Access Field Accessors
    # =========================================================================

    def get_manager_peer_unhealthy_since(self, peer_id: str) -> float | None:
        return self._manager_peer_unhealthy_since.get(peer_id)

    def set_manager_peer_unhealthy_since(self, peer_id: str, timestamp: float) -> None:
        self._manager_peer_unhealthy_since[peer_id] = timestamp

    def clear_manager_peer_unhealthy_since(self, peer_id: str) -> None:
        self._manager_peer_unhealthy_since.pop(peer_id, None)

    def get_gate_unhealthy_since(self, gate_id: str) -> float | None:
        return self._gate_unhealthy_since.get(gate_id)

    def set_gate_unhealthy_since(self, gate_id: str, timestamp: float) -> None:
        self._gate_unhealthy_since[gate_id] = timestamp

    def clear_gate_unhealthy_since(self, gate_id: str) -> None:
        self._gate_unhealthy_since.pop(gate_id, None)

    def get_gate_negotiated_caps(self, gate_id: str) -> NegotiatedCapabilities | None:
        return self._gate_negotiated_caps.get(gate_id)

    def set_gate_negotiated_caps(
        self, gate_id: str, caps: NegotiatedCapabilities
    ) -> None:
        self._gate_negotiated_caps[gate_id] = caps

    @property
    def dc_leader_manager_id(self) -> str | None:
        return self._dc_leader_manager_id

    def set_dc_leader_manager_id(self, manager_id: str | None) -> None:
        self._dc_leader_manager_id = manager_id

    def get_client_callback(self, job_id: str) -> tuple[str, int] | None:
        return self._client_callbacks.get(job_id)

    def set_client_callback(self, job_id: str, addr: tuple[str, int]) -> None:
        self._client_callbacks[job_id] = addr

    @property
    def manager_state_enum(self) -> ManagerStateEnum:
        return self._manager_state

    def set_manager_state_enum(self, state: ManagerStateEnum) -> None:
        self._manager_state = state
