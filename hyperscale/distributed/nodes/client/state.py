"""
Client runtime state for HyperscaleClient.

Manages all mutable state including job tracking, leadership, cancellations,
callbacks, and metrics.
"""

import asyncio
from typing import Callable

from hyperscale.distributed.models import (
    ClientJobResult,
    GateLeaderInfo,
    ManagerLeaderInfo,
    NegotiatedCapabilities,
)


class ClientState:
    """
    Runtime state for HyperscaleClient.

    Centralizes all mutable dictionaries and tracking structures.
    Provides clean separation between configuration (immutable) and
    runtime state (mutable).
    """

    def __init__(self) -> None:
        """Initialize empty state containers."""
        # Job tracking
        self._jobs: dict[str, ClientJobResult] = {}
        self._job_events: dict[str, asyncio.Event] = {}
        # The workflow ids each job was submitted with, and an event set
        # once its results are complete: every workflow has a result, or
        # the job's final result has supplied what exists.
        self._job_expected_workflows: dict[str, frozenset[str]] = {}
        self._job_results_events: dict[str, asyncio.Event] = {}
        self._job_callbacks: dict[str, Callable[[ClientJobResult], None]] = {}
        self._job_targets: dict[str, tuple[str, int]] = {}
        # When a retention sweep first found each job finished
        self._job_finished_seen_at: dict[str, float] = {}
        # AD-38 Part 8: per tracked job, the newest view of it a status read
        # answered (fence token, the leader's view time) -- what a SESSION
        # read must not go behind.
        self._job_read_views: dict[str, tuple[int, float]] = {}
        # Per job, the queues of its open ``stream_workflow_results``
        # iterators: each result is put to every one as it is recorded.
        self._workflow_result_streams: dict[str, set[asyncio.Queue]] = {}

        # Cancellation tracking
        self._cancellation_events: dict[str, asyncio.Event] = {}
        self._cancellation_errors: dict[str, list[str]] = {}
        self._cancellation_success: dict[str, bool] = {}

        # Reporter and workflow callbacks
        self._reporter_callbacks: dict[str, Callable] = {}
        self._workflow_callbacks: dict[str, Callable] = {}
        self._job_reporting_configs: dict[str, list] = {}

        # Progress callbacks
        self._progress_callbacks: dict[str, Callable] = {}

        # Protocol negotiation state
        self._server_negotiated_caps: dict[tuple[str, int], NegotiatedCapabilities] = {}

        # Target selection state (round-robin indices)
        self._current_manager_idx: int = 0
        self._current_gate_idx: int = 0

        # Gate leadership tracking
        self._gate_job_leaders: dict[str, GateLeaderInfo] = {}

        # Manager leadership tracking (keyed by (job_id, datacenter_id))
        self._manager_job_leaders: dict[tuple[str, str], ManagerLeaderInfo] = {}

        # Request routing locks (per-job)
        self._request_routing_locks: dict[str, asyncio.Lock] = {}

        # Leadership transfer metrics
        self._gate_transfers_received: int = 0
        self._manager_transfers_received: int = 0
        self._requests_rerouted: int = 0
        self._requests_failed_leadership_change: int = 0
        self._metrics_lock: asyncio.Lock | None = None

        # Lock creation lock (protects creation of per-resource locks)
        self._lock_creation_lock: asyncio.Lock | None = None

        # Gate connection state
        self._gate_connection_state: dict[tuple[str, int], str] = {}

    def initialize_job_tracking(
        self,
        job_id: str,
        initial_result: ClientJobResult,
        callback: Callable[[ClientJobResult], None] | None = None,
    ) -> None:
        """
        Initialize tracking structures for a new job.

        Args:
            job_id: Job identifier
            initial_result: Initial job result (typically SUBMITTED status)
            callback: Optional callback to invoke on status updates
        """
        self._jobs[job_id] = initial_result
        self._job_events[job_id] = asyncio.Event()
        if callback:
            self._job_callbacks[job_id] = callback

    def initialize_cancellation_tracking(self, job_id: str) -> None:
        """
        Initialize tracking structures for job cancellation.

        Args:
            job_id: Job identifier
        """
        self._cancellation_events[job_id] = asyncio.Event()
        self._cancellation_success[job_id] = False
        self._cancellation_errors[job_id] = []

    def record_job_read_view(self, job_id: str, fence_token: int, view_time: float) -> None:
        """A status read answered a view of a job this client tracks; only
        a newer one replaces the one held."""
        if job_id not in self._jobs:
            return
        self._hold_newer_read_view(job_id, fence_token, view_time)

    def _hold_newer_read_view(self, job_id: str, fence_token: int, view_time: float) -> None:
        """Replace the held read view of a job only with a newer one."""
        held = self._job_read_views.get(job_id)
        if held is None or (fence_token, view_time) > held:
            self._job_read_views[job_id] = (fence_token, view_time)

    def get_job_read_view(self, job_id: str) -> tuple[int, float]:
        """The newest view of ``job_id`` this client saw: (0, 0.0) if none."""
        return self._job_read_views.get(job_id, (0, 0.0))

    def subscribe_workflow_results(self, job_id: str, stream: asyncio.Queue) -> None:
        """Put each result recorded for ``job_id`` from now on to ``stream``."""
        self._workflow_result_streams.setdefault(job_id, set()).add(stream)

    def unsubscribe_workflow_results(self, job_id: str, stream: asyncio.Queue) -> None:
        if (streams := self._workflow_result_streams.get(job_id)) is not None:
            streams.discard(stream)
            if not streams:
                del self._workflow_result_streams[job_id]

    def workflow_result_streams(self, job_id: str) -> set[asyncio.Queue]:
        return self._workflow_result_streams.get(job_id, set())

    def release_job(self, job_id: str) -> None:
        """Forget everything tracked for a job."""
        self._workflow_result_streams.pop(job_id, None)
        self._job_read_views.pop(job_id, None)
        self._jobs.pop(job_id, None)
        self._job_events.pop(job_id, None)
        self._job_expected_workflows.pop(job_id, None)
        self._job_results_events.pop(job_id, None)
        self._job_callbacks.pop(job_id, None)
        self._job_targets.pop(job_id, None)
        self._job_finished_seen_at.pop(job_id, None)
        self._cancellation_events.pop(job_id, None)
        self._cancellation_errors.pop(job_id, None)
        self._cancellation_success.pop(job_id, None)
        self._reporter_callbacks.pop(job_id, None)
        self._workflow_callbacks.pop(job_id, None)
        self._job_reporting_configs.pop(job_id, None)
        self._progress_callbacks.pop(job_id, None)
        self._gate_job_leaders.pop(job_id, None)
        self._request_routing_locks.pop(job_id, None)
        for leader_key in self._manager_job_leader_keys(job_id):
            del self._manager_job_leaders[leader_key]

    def _manager_job_leader_keys(self, job_id: str) -> list:
        """The manager job-leader keys held for a job."""
        return [key for key in self._manager_job_leaders if key[0] == job_id]

    def release_finished_jobs(self, now: float, retention_seconds: float) -> list[str]:
        """Forget the jobs found finished at least ``retention_seconds``
        ago (a job's age counts from the first sweep that finds it
        finished). Returns the released job ids."""
        self._mark_newly_finished_jobs(now)

        expired_job_ids = self._expired_finished_job_ids(now, retention_seconds)
        for job_id in expired_job_ids:
            self.release_job(job_id)

        return expired_job_ids

    def _mark_newly_finished_jobs(self, now: float) -> None:
        """Note the first sweep that finds each job finished."""
        for job_id, event in self._job_events.items():
            if event.is_set():
                self._job_finished_seen_at.setdefault(job_id, now)

    def _expired_finished_job_ids(self, now: float, retention_seconds: float) -> list[str]:
        """The jobs found finished at least ``retention_seconds`` ago."""
        return [
            job_id
            for job_id, finished_seen_at in self._job_finished_seen_at.items()
            if now - finished_seen_at >= retention_seconds
        ]

    def mark_job_target(self, job_id: str, target: tuple[str, int]) -> None:
        """
        Mark the target server for a job (for sticky routing).

        Args:
            job_id: Job identifier
            target: (host, port) tuple of target server
        """
        self._job_targets[job_id] = target

    def get_job_target(self, job_id: str) -> tuple[str, int] | None:
        """
        Get the known target for a job.

        Args:
            job_id: Job identifier

        Returns:
            Target (host, port) or None if not known
        """
        return self._job_targets.get(job_id)

    async def get_or_create_routing_lock(self, job_id: str) -> asyncio.Lock:
        """
        Get or create a routing lock for a job.

        Args:
            job_id: Job identifier

        Returns:
            asyncio.Lock for this job's routing decisions
        """
        async with self._get_lock_creation_lock():
            if job_id not in self._request_routing_locks:
                self._request_routing_locks[job_id] = asyncio.Lock()
            return self._request_routing_locks[job_id]

    def initialize_locks(self) -> None:
        self._metrics_lock = asyncio.Lock()
        self._lock_creation_lock = asyncio.Lock()

    def _get_metrics_lock(self) -> asyncio.Lock:
        if self._metrics_lock is None:
            self._metrics_lock = asyncio.Lock()
        return self._metrics_lock

    def _get_lock_creation_lock(self) -> asyncio.Lock:
        if self._lock_creation_lock is None:
            self._lock_creation_lock = asyncio.Lock()
        return self._lock_creation_lock

    async def increment_gate_transfers(self) -> None:
        async with self._get_metrics_lock():
            self._gate_transfers_received += 1

    async def increment_manager_transfers(self) -> None:
        async with self._get_metrics_lock():
            self._manager_transfers_received += 1

    async def increment_rerouted(self) -> None:
        async with self._get_metrics_lock():
            self._requests_rerouted += 1

    async def increment_failed_leadership_change(self) -> None:
        async with self._get_metrics_lock():
            self._requests_failed_leadership_change += 1

    def get_leadership_metrics(self) -> dict:
        """
        Get leadership tracking metrics.

        Returns:
            Dict with transfer counts, rerouted requests and failures
        """
        return {
            "gate_transfers_received": self._gate_transfers_received,
            "manager_transfers_received": self._manager_transfers_received,
            "requests_rerouted": self._requests_rerouted,
            "requests_failed_leadership_change": self._requests_failed_leadership_change,
            "tracked_gate_leaders": len(self._gate_job_leaders),
            "tracked_manager_leaders": len(self._manager_job_leaders),
        }
