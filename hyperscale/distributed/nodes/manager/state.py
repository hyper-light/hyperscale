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
        """Clear all state associated with a job."""
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
        self._job_reporter_tasks.pop(job_id, None)
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
        self._dispatch_semaphores.pop(worker_id, None)
        self._worker_latency_samples.pop(worker_id, None)
        self._worker_circuits.pop(worker_id, None)
        self._worker_unhealthy_since.pop(worker_id, None)
        self._worker_deadlines.pop(worker_id, None)

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
        """Record workflow completion latency for SLO tracking."""
        self._workflow_latency_digest.add(latency_ms)

    def get_workflow_latency_observation(self) -> "LatencyObservation | None":
        """Get aggregated workflow latency observation for SLO reporting."""

        return self._workflow_latency_digest.get_recent_observation(
            target_id="workflows"
        )
