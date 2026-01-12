"""
Worker runtime state for WorkerServer.

Manages all mutable state including workflow tracking, manager peers,
core allocation, backpressure, and metrics.
"""

import asyncio
import time
from typing import TYPE_CHECKING

from hyperscale.distributed.models import (
    ManagerInfo,
    WorkflowProgress,
    PendingTransfer,
)
from hyperscale.distributed.reliability import BackpressureLevel
from hyperscale.distributed.swim.core import ErrorStats

if TYPE_CHECKING:
    from hyperscale.distributed.jobs import CoreAllocator


class WorkerState:
    """
    Runtime state for WorkerServer.

    Centralizes all mutable dictionaries and tracking structures.
    Provides clean separation between configuration (immutable) and
    runtime state (mutable).
    """

    def __init__(self, core_allocator: "CoreAllocator") -> None:
        """
        Initialize empty state containers.

        Args:
            core_allocator: The CoreAllocator instance for core management
        """
        # Core allocation
        self._core_allocator = core_allocator

        # Manager tracking
        self._known_managers: dict[str, ManagerInfo] = {}
        self._healthy_manager_ids: set[str] = set()
        self._primary_manager_id: str | None = None
        self._manager_unhealthy_since: dict[str, float] = {}
        self._manager_circuits: dict[str, ErrorStats] = {}
        self._manager_addr_circuits: dict[tuple[str, int], ErrorStats] = {}
        self._manager_state_locks: dict[str, asyncio.Lock] = {}
        self._manager_state_epoch: dict[str, int] = {}

        # Workflow tracking
        self._active_workflows: dict[str, WorkflowProgress] = {}
        self._workflow_tokens: dict[str, str] = {}
        self._workflow_cancel_events: dict[str, asyncio.Event] = {}
        self._workflow_id_to_name: dict[str, str] = {}
        self._workflow_job_leader: dict[str, tuple[str, int]] = {}
        self._workflow_fence_tokens: dict[str, int] = {}
        self._workflow_cores_completed: dict[str, set[int]] = {}
        self._pending_workflows: list = []

        # Progress buffering
        self._progress_buffer: dict[str, WorkflowProgress] = {}
        self._progress_buffer_lock = asyncio.Lock()

        # Backpressure tracking (AD-23)
        self._manager_backpressure: dict[str, BackpressureLevel] = {}
        self._backpressure_delay_ms: int = 0

        # Orphaned workflow tracking (Section 2.7)
        self._orphaned_workflows: dict[str, float] = {}

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

        # Extension request state (AD-26)
        self._extension_requested: bool = False
        self._extension_reason: str = ""
        self._extension_current_progress: float = 0.0
        self._extension_completed_items: int = 0
        self._extension_total_items: int = 0
        self._extension_estimated_completion: float = 0.0
        self._extension_active_workflow_count: int = 0

        # Throughput tracking (AD-19)
        self._throughput_completions: int = 0
        self._throughput_interval_start: float = time.monotonic()
        self._throughput_last_value: float = 0.0
        self._completion_times: list[float] = []

    def initialize_locks(self) -> None:
        self._version_lock = asyncio.Lock()

    def _get_version_lock(self) -> asyncio.Lock:
        if self._version_lock is None:
            self._version_lock = asyncio.Lock()
        return self._version_lock

    async def increment_version(self) -> int:
        async with self._get_version_lock():
            self._state_version += 1
            return self._state_version

    @property
    def state_version(self) -> int:
        return self._state_version

    # =========================================================================
    # Manager Tracking
    # =========================================================================

    def add_manager(self, manager_id: str, manager_info: ManagerInfo) -> None:
        """
        Add or update a known manager.

        Args:
            manager_id: Manager node identifier
            manager_info: Manager information
        """
        self._known_managers[manager_id] = manager_info

    def get_manager(self, manager_id: str) -> ManagerInfo | None:
        """Get manager info by ID."""
        return self._known_managers.get(manager_id)

    def mark_manager_healthy(self, manager_id: str) -> None:
        """Mark a manager as healthy."""
        self._healthy_manager_ids.add(manager_id)
        self._manager_unhealthy_since.pop(manager_id, None)

    def mark_manager_unhealthy(self, manager_id: str) -> None:
        """Mark a manager as unhealthy."""
        self._healthy_manager_ids.discard(manager_id)
        if manager_id not in self._manager_unhealthy_since:
            self._manager_unhealthy_since[manager_id] = time.monotonic()

    def is_manager_healthy(self, manager_id: str) -> bool:
        """Check if a manager is in the healthy set."""
        return manager_id in self._healthy_manager_ids

    def get_healthy_manager_tcp_addrs(self) -> list[tuple[str, int]]:
        """Get TCP addresses of all healthy managers."""
        return [
            (manager.tcp_host, manager.tcp_port)
            for manager_id in self._healthy_manager_ids
            if (manager := self._known_managers.get(manager_id))
        ]

    def get_or_create_manager_lock(self, manager_id: str) -> asyncio.Lock:
        """Get or create a state lock for a manager."""
        return self._manager_state_locks.setdefault(manager_id, asyncio.Lock())

    def increment_manager_epoch(self, manager_id: str) -> int:
        """Increment and return the epoch for a manager."""
        current = self._manager_state_epoch.get(manager_id, 0)
        self._manager_state_epoch[manager_id] = current + 1
        return self._manager_state_epoch[manager_id]

    def get_manager_epoch(self, manager_id: str) -> int:
        """Get current epoch for a manager."""
        return self._manager_state_epoch.get(manager_id, 0)

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
        """
        Remove a workflow from active tracking.

        Returns the removed progress or None if not found.
        """
        progress = self._active_workflows.pop(workflow_id, None)
        self._workflow_job_leader.pop(workflow_id, None)
        self._workflow_cores_completed.pop(workflow_id, None)
        self._workflow_cancel_events.pop(workflow_id, None)
        self._workflow_tokens.pop(workflow_id, None)
        self._workflow_id_to_name.pop(workflow_id, None)
        self._orphaned_workflows.pop(workflow_id, None)
        return progress

    def get_workflow_job_leader(self, workflow_id: str) -> tuple[str, int] | None:
        """Get job leader address for a workflow."""
        return self._workflow_job_leader.get(workflow_id)

    def set_workflow_job_leader(
        self, workflow_id: str, leader_addr: tuple[str, int]
    ) -> None:
        """Update job leader address for a workflow."""
        self._workflow_job_leader[workflow_id] = leader_addr

    def update_workflow_fence_token(self, workflow_id: str, fence_token: int) -> bool:
        """
        Update fence token if it's newer than current.

        Returns True if token was accepted, False if stale.
        """
        current = self._workflow_fence_tokens.get(workflow_id, -1)
        if fence_token <= current:
            return False
        self._workflow_fence_tokens[workflow_id] = fence_token
        return True

    def get_workflow_fence_token(self, workflow_id: str) -> int:
        """Get current fence token for a workflow, or -1 if not set."""
        return self._workflow_fence_tokens.get(workflow_id, -1)

    # =========================================================================
    # Orphan Tracking (Section 2.7)
    # =========================================================================

    def mark_workflow_orphaned(self, workflow_id: str) -> None:
        """Mark a workflow as orphaned."""
        if workflow_id not in self._orphaned_workflows:
            self._orphaned_workflows[workflow_id] = time.monotonic()

    def clear_workflow_orphaned(self, workflow_id: str) -> None:
        """Clear orphaned status for a workflow."""
        self._orphaned_workflows.pop(workflow_id, None)

    def is_workflow_orphaned(self, workflow_id: str) -> bool:
        """Check if a workflow is orphaned."""
        return workflow_id in self._orphaned_workflows

    def get_orphaned_workflows_expired(self, grace_period_seconds: float) -> list[str]:
        """Get workflow IDs whose orphan grace period has expired."""
        current_time = time.monotonic()
        return [
            workflow_id
            for workflow_id, orphaned_at in self._orphaned_workflows.items()
            if current_time - orphaned_at > grace_period_seconds
        ]

    # =========================================================================
    # Job Leadership Transfer (Section 8)
    # =========================================================================

    def get_or_create_job_transfer_lock(self, job_id: str) -> asyncio.Lock:
        """Get or create a transfer lock for a job."""
        return self._job_leader_transfer_locks.setdefault(job_id, asyncio.Lock())

    def update_job_fence_token(self, job_id: str, fence_token: int) -> bool:
        """
        Update job fence token if it's newer than current.

        Returns True if token was accepted, False if stale.
        """
        current = self._job_fence_tokens.get(job_id, -1)
        if fence_token <= current:
            return False
        self._job_fence_tokens[job_id] = fence_token
        return True

    def get_job_fence_token(self, job_id: str) -> int:
        """Get current fence token for a job, or -1 if not set."""
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

    def increment_transfer_received(self) -> None:
        """Increment transfer received counter."""
        self._transfer_metrics_received += 1

    def increment_transfer_accepted(self) -> None:
        """Increment transfer accepted counter."""
        self._transfer_metrics_accepted += 1

    def increment_transfer_rejected_stale_token(self) -> None:
        """Increment stale token rejection counter."""
        self._transfer_metrics_rejected_stale_token += 1

    def increment_transfer_rejected_unknown_manager(self) -> None:
        """Increment unknown manager rejection counter."""
        self._transfer_metrics_rejected_unknown_manager += 1

    def increment_transfer_rejected_other(self) -> None:
        """Increment other rejection counter."""
        self._transfer_metrics_rejected_other += 1

    def get_transfer_metrics(self) -> dict:
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

    def record_completion(self, duration_seconds: float) -> None:
        """Record a workflow completion for throughput tracking."""
        self._throughput_completions += 1
        self._completion_times.append(duration_seconds)
        if len(self._completion_times) > 50:
            self._completion_times.pop(0)

    def get_throughput(self) -> float:
        """Get current throughput (completions per second)."""
        current_time = time.monotonic()
        elapsed = current_time - self._throughput_interval_start
        if elapsed >= 10.0:
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

    def remove_manager_lock(self, manager_id: str) -> None:
        """Remove lock and epoch when manager disconnects to prevent memory leak."""
        self._manager_state_locks.pop(manager_id, None)
        self._manager_state_epoch.pop(manager_id, None)

    def remove_job_transfer_lock(self, job_id: str) -> None:
        """Remove transfer lock and token when job completes to prevent memory leak."""
        self._job_leader_transfer_locks.pop(job_id, None)
        self._job_fence_tokens.pop(job_id, None)
        self._pending_transfers.pop(job_id, None)
