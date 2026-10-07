"""
Worker runtime state for WorkerServer.

Manages all mutable state including workflow tracking, manager peers,
core allocation, backpressure, and metrics.
"""

import asyncio
from typing import TYPE_CHECKING, Callable

from hyperscale.distributed.models import (
    WorkflowDispatch,
    WorkflowProgress,
    WorkflowStatus,
    PendingTransfer,
)
from hyperscale.distributed.reliability import BackpressureLevel

from hyperscale.distributed.runtime import Clock, RealClock


_DEFAULT_CLOCK: Clock = RealClock()

if TYPE_CHECKING:
    from hyperscale.distributed.jobs import CoreAllocator


class WorkerState:
    """
    Runtime state for WorkerServer.

    Centralizes all mutable dictionaries and tracking structures.
    Provides clean separation between configuration (immutable) and
    runtime state (mutable).

    Lock ordering (acquire in this order to avoid deadlock):
        1. _resource_creation_lock — outermost; held only briefly while
           creating per-resource locks (_job_leader_transfer_locks). Never
           held across an await on any
           other lock here.
        2. _version_lock — guards _state_version. Independent of other
           locks below; never nested with _counter_lock.
        3. _progress_buffer_lock — guards _progress_buffer. Independent
           leaf lock.
        4. _counter_lock — innermost; pure atomic-increment guard for
           transfer metrics, fence tokens, throughput counters. Never
           held across awaits on any other lock above.

    Per-resource locks (_job_leader_transfer_locks[id]) are leaf locks: acquired after the lookup-time critical section in
    _resource_creation_lock has been released, and never held while
    acquiring any of the locks above.
    """

    def __init__(
        self,
        core_allocator: "CoreAllocator",
        *,
        throughput_interval_seconds: float,
        completion_times_max_samples: int,
    ) -> None:
        """
        Initialize empty state containers.

        Args:
            core_allocator: The CoreAllocator instance for core management
            throughput_interval_seconds: The window AD-19 health throughput
                (completions per second) is measured over
                (``WORKER_THROUGHPUT_INTERVAL_SECONDS``)
            completion_times_max_samples: Recent completion times kept for
                the expected throughput (``WORKER_COMPLETION_TIMES_MAX_SAMPLES``)
        """
        self._throughput_interval_seconds = throughput_interval_seconds
        self._completion_times_max_samples = completion_times_max_samples
        # Core allocation
        self._core_allocator: "CoreAllocator" = core_allocator

        # Workflow tracking
        self._active_workflows: dict[str, WorkflowProgress] = {}
        self._workflow_tokens: dict[str, str] = {}
        self._workflow_cancel_events: dict[str, asyncio.Event] = {}
        self._cancellation_completion_events: dict[str, asyncio.Event] = {}
        self._cancellation_errors: dict[str, list[str]] = {}
        self._workflow_id_to_name: dict[str, str] = {}
        self._workflow_job_leader: dict[str, tuple[str, int]] = {}
        self._workflow_fence_tokens: dict[str, int] = {}
        self._workflow_cores_completed: dict[str, set[int]] = {}
        self._pending_workflows: list[WorkflowDispatch] = []
        self._workflow_start_times: dict[str, float] = {}
        self._workflow_timeout_seconds: dict[str, float] = {}
        self._suppressed_final_result_reasons: dict[str, str] = {}
        # Phase H4 — callbacks invoked on every workflow termination
        # path (success / failure / cancel / orphan-eviction). The
        # autonomous extension trigger registers here so its
        # per-workflow bookkeeping is dropped uniformly regardless of
        # which subsystem terminated the workflow.
        self._workflow_termination_callbacks: list[Callable[[str], None]] = []

        # Progress buffering
        self._progress_buffer: dict[str, WorkflowProgress] = {}
        self._progress_buffer_lock: asyncio.Lock = asyncio.Lock()

        # Backpressure tracking (AD-23)
        self._manager_backpressure: dict[str, BackpressureLevel] = {}
        self._backpressure_delay_ms: int = 0

        # Orphaned workflow tracking (Section 2.7)
        self._orphaned_workflows: dict[str, float] = {}
        # Manager heartbeats received so far, and the count when each
        # orphaned workflow was orphaned: heartbeats since then are the
        # progress an orphan grace extension is granted on (AD-26). The
        # longest an orphan has waited for its new leader raises the grace.
        self._manager_heartbeats_received = 0
        self._orphan_heartbeat_baselines: dict[str, int] = {}
        self._longest_orphan_rescue_seconds = 0.0

        # Job leadership transfer (Section 8)
        self._job_leader_transfer_locks: dict[str, asyncio.Lock] = {}
        self._job_fence_tokens: dict[str, int] = {}
        self._pending_transfers: dict[str, PendingTransfer] = {}

        # Transfer metrics (Section 8.6)
        self._transfer_metrics_received: int = 0
        self._transfer_metrics_accepted: int = 0
        self._transfer_metrics_rejected_stale_token: int = 0
        self._transfer_metrics_rejected_unknown_manager: int = 0
        self._transfer_metrics_rejected_other: int = 0

        # State versioning
        self._state_version: int = 0
        self._version_lock: asyncio.Lock | None = None

        # Lock for creating per-resource locks
        self._resource_creation_lock: asyncio.Lock | None = None

        # Counter protection lock (for race-free increments)
        self._counter_lock: asyncio.Lock | None = None

        # Extension request state (AD-26)
        self._extension_requested: bool = False
        self._extension_reason: str = ""
        self._extension_current_progress: float = 0.0
        self._extension_completed_items: int = 0
        self._extension_total_items: int = 0
        self._extension_estimated_completion: float = 0.0
        self._extension_active_workflow_count: int = 0
        # Phase H3 — multi-dimensional WorkflowProgressSnapshot fields
        # piggybacked alongside the AD-26 extension request. Identifies
        # which workflow the snapshot describes, plus secondary/tertiary
        # progress counters and the worker-side capture timestamp.
        self._extension_workflow_id: str = ""
        self._extension_step_transitions: int = 0
        self._extension_actions_completed: int = 0
        self._extension_snapshot_time: float = 0.0

        # Throughput tracking (AD-19)
        self._throughput_completions: int = 0
        self._throughput_interval_start: float = _DEFAULT_CLOCK.monotonic()
        self._throughput_last_value: float = 0.0
        self._completion_times: list[float] = []

    def initialize_locks(self) -> None:
        self._version_lock = asyncio.Lock()
        self._resource_creation_lock = asyncio.Lock()
        self._counter_lock = asyncio.Lock()

    def _get_version_lock(self) -> asyncio.Lock:
        if self._version_lock is None:
            self._version_lock = asyncio.Lock()
        return self._version_lock

    def _get_resource_creation_lock(self) -> asyncio.Lock:
        if self._resource_creation_lock is None:
            self._resource_creation_lock = asyncio.Lock()
        return self._resource_creation_lock

    def _get_counter_lock(self) -> asyncio.Lock:
        if self._counter_lock is None:
            self._counter_lock = asyncio.Lock()
        return self._counter_lock

    async def increment_version(self) -> int:
        async with self._get_version_lock():
            self._state_version += 1
            return self._state_version

    @property
    def state_version(self) -> int:
        return self._state_version

    # =========================================================================
    # Workflow Tracking
    # =========================================================================

    def add_active_workflow(
        self,
        workflow_id: str,
        progress: WorkflowProgress,
        job_leader_addr: tuple[str, int],
    ) -> None:
        """
        Add a workflow to active tracking.

        Args:
            workflow_id: Workflow identifier
            progress: Initial progress state
            job_leader_addr: TCP address of job leader manager
        """
        self._active_workflows[workflow_id] = progress
        self._workflow_job_leader[workflow_id] = job_leader_addr
        self._workflow_cores_completed[workflow_id] = set()

    def get_active_workflow(self, workflow_id: str) -> WorkflowProgress | None:
        """Get active workflow progress by ID."""
        return self._active_workflows.get(workflow_id)

    def remove_active_workflow(self, workflow_id: str) -> WorkflowProgress | None:
        progress = self._active_workflows.pop(workflow_id, None)
        self._workflow_job_leader.pop(workflow_id, None)
        self._workflow_cores_completed.pop(workflow_id, None)
        self._workflow_cancel_events.pop(workflow_id, None)
        self._cancellation_completion_events.pop(workflow_id, None)
        self._cancellation_errors.pop(workflow_id, None)
        self._workflow_tokens.pop(workflow_id, None)
        self._workflow_id_to_name.pop(workflow_id, None)
        self._orphaned_workflows.pop(workflow_id, None)
        self._orphan_heartbeat_baselines.pop(workflow_id, None)
        self._workflow_start_times.pop(workflow_id, None)
        self._workflow_timeout_seconds.pop(workflow_id, None)
        self._suppressed_final_result_reasons.pop(workflow_id, None)
        # Phase H4 — fire registered termination callbacks (e.g. the
        # autonomous extension trigger's forget_workflow). Catches the
        # workflow_executor termination path as well as the worker
        # server's _cleanup_workflow_state path; everyone goes through
        # remove_active_workflow eventually.
        # Every callback runs even when one fails; the failures then raise
        # together, after this workflow's state is gone.
        callback_errors = self._run_termination_callbacks(workflow_id)
        if callback_errors:
            raise ExceptionGroup(
                f"workflow {workflow_id} termination callbacks failed", callback_errors
            )
        return progress

    def _run_termination_callbacks(self, workflow_id: str) -> list[Exception]:
        """Invoke every termination callback (Phase H4), collecting their failures."""
        callback_errors: list[Exception] = []
        for callback in list(self._workflow_termination_callbacks):
            try:
                callback(workflow_id)
            except Exception as callback_error:
                callback_errors.append(callback_error)
        return callback_errors

    def register_workflow_termination_callback(
        self, callback: "Callable[[str], None]"
    ) -> None:
        """Register a callable invoked with ``workflow_id`` whenever a
        workflow finishes (any path through ``remove_active_workflow``).

        Phase H4: ``ExtensionTrigger`` registers via this to clean up
        its per-workflow bookkeeping dict.
        """
        self._workflow_termination_callbacks.append(callback)

    def suppress_final_result(self, workflow_id: str, reason: str) -> None:
        """Suppress non-success final-result pushes for a locally orphaned workflow."""
        self._suppressed_final_result_reasons[workflow_id] = reason

    def suppress_active_final_results(self, reason: str) -> None:
        """Suppress non-success final-result pushes for all active workflows."""
        for workflow_id in list(self._active_workflows.keys()):
            self.suppress_final_result(workflow_id, reason)

    def is_final_result_suppressed(self, workflow_id: str) -> bool:
        """Return whether a workflow's non-success final result is locally suppressed."""
        return workflow_id in self._suppressed_final_result_reasons

    def get_workflow_job_leader(self, workflow_id: str) -> tuple[str, int] | None:
        """Get job leader address for a workflow."""
        return self._workflow_job_leader.get(workflow_id)

    def set_workflow_job_leader(
        self, workflow_id: str, leader_addr: tuple[str, int]
    ) -> None:
        """Update job leader address for a workflow."""
        self._workflow_job_leader[workflow_id] = leader_addr

    async def update_workflow_fence_token(
        self, workflow_id: str, fence_token: int
    ) -> bool:
        """
        Update fence token if it's newer than current.

        Returns True if token was accepted, False if stale.
        """
        async with self._get_counter_lock():
            current = self._workflow_fence_tokens.get(workflow_id, -1)
            if fence_token <= current:
                return False
            self._workflow_fence_tokens[workflow_id] = fence_token
            return True

    async def get_workflow_fence_token(self, workflow_id: str) -> int:
        async with self._get_counter_lock():
            return self._workflow_fence_tokens.get(workflow_id, -1)

    def set_workflow_timeout(self, workflow_id: str, timeout_seconds: float) -> None:
        now = _DEFAULT_CLOCK.monotonic()
        self._workflow_start_times[workflow_id] = now
        self._workflow_timeout_seconds[workflow_id] = timeout_seconds

    def extend_workflow_timeout(
        self, workflow_id: str, extension_seconds: float
    ) -> bool:
        """Stretch an active workflow's LOCAL deadline by a granted
        AD-26 extension.

        The stuck-workflow enforcement loop compares elapsed against
        ``_workflow_timeout_seconds`` — the dispatch-time value. A
        manager-granted extension that stretches the job's AD-34
        budget but not this local deadline leaves the worker
        enforcing the UN-extended number and hard-cancelling the very
        workflow the manager just granted more time (measured: grant
        +30s at 15.5, local enforcement killed the workflow at
        dispatch + the base 20s anyway). Returns False when the
        workflow is no longer tracked (already drained — best-effort).
        """
        if workflow_id not in self._workflow_timeout_seconds:
            return False
        self._workflow_timeout_seconds[workflow_id] += extension_seconds
        return True

    def get_workflow_timeout(self, workflow_id: str) -> float | None:
        """Return the per-workflow timeout in seconds, or None if not set.

        Phase H4 — used by ``ExtensionTrigger`` to decide when a
        workflow is approaching its deadline.
        """
        return self._workflow_timeout_seconds.get(workflow_id)

    def get_stuck_workflows(self) -> list[tuple[str, float]]:
        """
        Returns (workflow_id, elapsed_seconds) for workflows exceeding their timeout.
        """
        now = _DEFAULT_CLOCK.monotonic()
        stuck: list[tuple[str, float]] = []
        for workflow_id in list(self._active_workflows.keys()):
            if (elapsed := self._stuck_elapsed(workflow_id, now)) is not None:
                stuck.append((workflow_id, elapsed))
        return stuck

    def _stuck_elapsed(self, workflow_id: str, now: float) -> float | None:
        """A workflow's elapsed seconds when past its timeout, else None."""
        timing = self._workflow_timing(workflow_id)
        if timing is None:
            return None
        start_time, timeout = timing
        elapsed = now - start_time
        return elapsed if elapsed > timeout else None

    def _workflow_timing(self, workflow_id: str) -> tuple[float, float] | None:
        """A workflow's (start time, timeout seconds), or None when either is unset."""
        start_time = self._workflow_start_times.get(workflow_id)
        timeout = self._workflow_timeout_seconds.get(workflow_id)
        if start_time is None or timeout is None:
            return None
        return (start_time, timeout)

    def mark_workflow_orphaned(self, workflow_id: str) -> None:
        if workflow_id not in self._orphaned_workflows:
            self._orphaned_workflows[workflow_id] = _DEFAULT_CLOCK.monotonic()
            self._orphan_heartbeat_baselines[workflow_id] = self._manager_heartbeats_received

    def clear_workflow_orphaned(self, workflow_id: str) -> None:
        """Clear orphaned status for a workflow -- its new leader was found:
        how long that took is a rescue the orphan grace learns from."""
        if (orphaned_at := self._orphaned_workflows.pop(workflow_id, None)) is not None:
            self._longest_orphan_rescue_seconds = max(
                self._longest_orphan_rescue_seconds, _DEFAULT_CLOCK.monotonic() - orphaned_at
            )
        self._orphan_heartbeat_baselines.pop(workflow_id, None)

    def drop_orphan(self, workflow_id: str) -> None:
        """Stop tracking an orphan the worker gives up on (not a rescue)."""
        self._orphaned_workflows.pop(workflow_id, None)
        self._orphan_heartbeat_baselines.pop(workflow_id, None)

    def record_manager_heartbeat(self) -> None:
        """A manager heartbeat reached this worker."""
        self._manager_heartbeats_received += 1

    @property
    def manager_heartbeats_received(self) -> int:
        return self._manager_heartbeats_received

    @property
    def longest_orphan_rescue_seconds(self) -> float:
        return self._longest_orphan_rescue_seconds

    def orphan_heartbeat_baseline(self, workflow_id: str) -> int:
        """The manager heartbeats received when ``workflow_id`` was orphaned."""
        return self._orphan_heartbeat_baselines.get(workflow_id, self._manager_heartbeats_received)

    def is_workflow_orphaned(self, workflow_id: str) -> bool:
        """Check if a workflow is orphaned."""
        return workflow_id in self._orphaned_workflows

    def get_orphaned_workflows_expired(self, grace_period_seconds: float) -> list[str]:
        """Get workflow IDs whose orphan grace period has expired."""
        current_time = _DEFAULT_CLOCK.monotonic()
        return [
            workflow_id
            for workflow_id, orphaned_at in self._orphaned_workflows.items()
            if current_time - orphaned_at > grace_period_seconds
        ]

    # =========================================================================
    # Job Leadership Transfer (Section 8)
    # =========================================================================

    async def get_or_create_job_transfer_lock(self, job_id: str) -> asyncio.Lock:
        async with self._get_resource_creation_lock():
            if job_id not in self._job_leader_transfer_locks:
                self._job_leader_transfer_locks[job_id] = asyncio.Lock()
            return self._job_leader_transfer_locks[job_id]

    async def update_job_fence_token(self, job_id: str, fence_token: int) -> bool:
        """
        Update job fence token if it's newer than current.

        Returns True if token was accepted, False if stale.
        """
        async with self._get_counter_lock():
            current = self._job_fence_tokens.get(job_id, -1)
            if fence_token <= current:
                return False
            self._job_fence_tokens[job_id] = fence_token
            return True

    async def get_job_fence_token(self, job_id: str) -> int:
        async with self._get_counter_lock():
            return self._job_fence_tokens.get(job_id, -1)

    def add_pending_transfer(self, job_id: str, transfer: PendingTransfer) -> None:
        """Store a pending transfer for late-arriving workflows."""
        self._pending_transfers[job_id] = transfer

    def get_pending_transfer(self, job_id: str) -> PendingTransfer | None:
        """Get pending transfer for a job."""
        return self._pending_transfers.get(job_id)

    def remove_pending_transfer(self, job_id: str) -> PendingTransfer | None:
        """Remove and return pending transfer for a job."""
        return self._pending_transfers.pop(job_id, None)

    async def increment_transfer_received(self) -> None:
        async with self._get_counter_lock():
            self._transfer_metrics_received += 1

    async def increment_transfer_accepted(self) -> None:
        async with self._get_counter_lock():
            self._transfer_metrics_accepted += 1

    async def increment_transfer_rejected_stale_token(self) -> None:
        async with self._get_counter_lock():
            self._transfer_metrics_rejected_stale_token += 1

    async def increment_transfer_rejected_unknown_manager(self) -> None:
        async with self._get_counter_lock():
            self._transfer_metrics_rejected_unknown_manager += 1

    async def increment_transfer_rejected_other(self) -> None:
        async with self._get_counter_lock():
            self._transfer_metrics_rejected_other += 1

    def get_transfer_metrics(self) -> dict[str, int]:
        """Get transfer metrics summary."""
        return {
            "received": self._transfer_metrics_received,
            "accepted": self._transfer_metrics_accepted,
            "rejected_stale_token": self._transfer_metrics_rejected_stale_token,
            "rejected_unknown_manager": self._transfer_metrics_rejected_unknown_manager,
            "rejected_other": self._transfer_metrics_rejected_other,
        }

    # =========================================================================
    # Backpressure (AD-23)
    # =========================================================================

    def set_manager_backpressure(
        self, manager_id: str, level: BackpressureLevel
    ) -> None:
        """Update backpressure level for a manager."""
        self._manager_backpressure[manager_id] = level

    def get_max_backpressure_level(self) -> BackpressureLevel:
        """Get maximum backpressure level across all managers."""
        if not self._manager_backpressure:
            return BackpressureLevel.NONE
        return max(self._manager_backpressure.values(), key=lambda x: x.value)

    def set_backpressure_delay_ms(self, delay_ms: int) -> None:
        """Set backpressure delay from manager."""
        self._backpressure_delay_ms = delay_ms

    def get_backpressure_delay_ms(self) -> int:
        """Get current backpressure delay."""
        return self._backpressure_delay_ms

    # =========================================================================
    # Progress Buffer (AD-37)
    # =========================================================================

    async def buffer_progress_update(
        self,
        workflow_id: str,
        progress: WorkflowProgress,
    ) -> None:
        """
        Buffer a progress update for later flush.

        Args:
            workflow_id: Workflow identifier
            progress: Progress update to buffer
        """
        async with self._progress_buffer_lock:
            self._progress_buffer[workflow_id] = progress

    async def flush_progress_buffer(self) -> dict[str, WorkflowProgress]:
        """
        Flush and return all buffered progress updates.

        Returns:
            Dictionary of workflow_id to progress updates
        """
        async with self._progress_buffer_lock:
            updates = dict(self._progress_buffer)
            self._progress_buffer.clear()
        return updates

    async def clear_progress_buffer(self) -> None:
        """Clear all buffered progress updates without returning them."""
        async with self._progress_buffer_lock:
            self._progress_buffer.clear()

    def get_buffered_update_count(self) -> int:
        """Get count of buffered progress updates."""
        return len(self._progress_buffer)

    # =========================================================================
    # Throughput Tracking (AD-19)
    # =========================================================================

    async def record_completion(self, status: str, duration_seconds: float) -> None:
        """A workflow run ended: a completed one counts toward the AD-19
        health throughput and its duration toward the expected throughput."""
        if status != WorkflowStatus.COMPLETED.value:
            return
        async with self._get_counter_lock():
            self._throughput_completions += 1
            self._completion_times.append(duration_seconds)
            if len(self._completion_times) > self._completion_times_max_samples:
                self._completion_times.pop(0)

    def get_throughput(self) -> float:
        """Get current throughput (completions per second)."""
        current_time = _DEFAULT_CLOCK.monotonic()
        elapsed = current_time - self._throughput_interval_start
        if elapsed >= self._throughput_interval_seconds:
            self._throughput_last_value = self._throughput_completions / elapsed
            self._throughput_completions = 0
            self._throughput_interval_start = current_time
        return self._throughput_last_value

    def get_expected_throughput(self) -> float:
        """Get expected throughput based on average completion time."""
        if not self._completion_times:
            return 0.0
        avg_completion_time = sum(self._completion_times) / len(self._completion_times)
        if avg_completion_time <= 0:
            return 0.0
        return 1.0 / avg_completion_time

    def get_completion_sample_count(self) -> int:
        """Get count of completion time samples."""
        return len(self._completion_times)

    def remove_job_transfer_lock(self, job_id: str) -> None:
        """Remove transfer lock and token when job completes to prevent memory leak."""
        self._job_leader_transfer_locks.pop(job_id, None)
        self._job_fence_tokens.pop(job_id, None)
        self._pending_transfers.pop(job_id, None)
