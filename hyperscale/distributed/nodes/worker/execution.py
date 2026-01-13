"""
Worker execution module.

Handles workflow execution, progress reporting, and cleanup
for worker dispatch operations (AD-33 compliance).

Note: Throughput and progress buffer state is delegated to WorkerState
to maintain single source of truth (no duplicate state).
"""

import asyncio
import time
from typing import TYPE_CHECKING, Any

from hyperscale.distributed.models import (
    WorkflowProgress,
    WorkflowStatus,
)

if TYPE_CHECKING:
    from hyperscale.logging import Logger
    from hyperscale.distributed.jobs import CoreAllocator
    from .backpressure import WorkerBackpressureManager
    from .state import WorkerState


class WorkerExecutor:
    """
    Handles workflow execution for worker (AD-33 compliance).

    Manages workflow dispatch, progress monitoring, status transitions,
    and cleanup. Preserves AD-33 workflow state machine transitions.

    Delegates throughput tracking and progress buffering to WorkerState
    to avoid duplicate state.
    """

    def __init__(
        self,
        core_allocator: "CoreAllocator",
        logger: "Logger",
        state: "WorkerState",
        progress_update_interval: float = 1.0,
        progress_flush_interval: float = 0.5,
        backpressure_manager: "WorkerBackpressureManager | None" = None,
    ) -> None:
        """
        Initialize worker executor.

        Args:
            core_allocator: CoreAllocator for core management
            logger: Logger instance for logging
            state: WorkerState for throughput/progress tracking (single source of truth)
            progress_update_interval: Interval between progress updates
            progress_flush_interval: Interval for progress buffer flush
            backpressure_manager: Backpressure manager for AD-37 compliance
        """
        self._core_allocator: "CoreAllocator" = core_allocator
        self._logger: "Logger" = logger
        self._state: "WorkerState" = state
        self._progress_update_interval: float = progress_update_interval
        self._progress_flush_interval: float = progress_flush_interval
        self._backpressure_manager: "WorkerBackpressureManager | None" = (
            backpressure_manager
        )
        self._running: bool = False

    @property
    def available_cores(self) -> int:
        """Get number of available cores."""
        return self._core_allocator.available_cores

    @property
    def total_cores(self) -> int:
        """Get total number of cores."""
        return self._core_allocator.total_cores

    async def allocate_cores(
        self,
        workflow_id: str,
        cores_requested: int,
    ) -> tuple[bool, list[int] | None, str | None]:
        """
        Allocate cores for a workflow.

        Args:
            workflow_id: Workflow identifier
            cores_requested: Number of cores requested

        Returns:
            Tuple of (success, allocated_cores, error_message)
        """
        result = await self._core_allocator.allocate(workflow_id, cores_requested)
        if result.success:
            return (True, result.allocated_cores, None)
        return (False, None, result.error)

    async def free_cores(self, workflow_id: str) -> None:
        """Free cores allocated to a workflow."""
        await self._core_allocator.free(workflow_id)

    async def record_throughput_event(self, completion_time_seconds: float) -> None:
        """
        Record a workflow completion event for throughput tracking (AD-19).

        Delegates to WorkerState (single source of truth).

        Args:
            completion_time_seconds: Time taken to complete the workflow
        """
        await self._state.record_completion(completion_time_seconds)

    def get_throughput(self) -> float:
        """
        Get current throughput (completions per second).

        Delegates to WorkerState (single source of truth).

        Returns:
            Throughput value
        """
        return self._state.get_throughput()

    def get_expected_throughput(self) -> float:
        """
        Get expected throughput based on average completion time.

        Delegates to WorkerState (single source of truth).

        Returns:
            Expected throughput value
        """
        return self._state.get_expected_throughput()

    async def buffer_progress_update(
        self,
        workflow_id: str,
        progress: WorkflowProgress,
    ) -> None:
        """
        Buffer a progress update for later flush.

        Delegates to WorkerState (single source of truth).

        Args:
            workflow_id: Workflow identifier
            progress: Progress update to buffer
        """
        await self._state.buffer_progress_update(workflow_id, progress)

    async def flush_progress_buffer(
        self,
        send_progress: callable,
    ) -> None:
        """
        Flush buffered progress updates.

        Args:
            send_progress: Function to send progress to manager
        """
        updates = await self._state.flush_progress_buffer()

        for workflow_id, progress in updates.items():
            try:
                await send_progress(workflow_id, progress)
            except Exception:
                pass

    async def run_progress_flush_loop(
        self,
        send_progress: callable,
    ) -> None:
        """
        Background loop for flushing progress updates (AD-37 compliant).

        Respects backpressure levels from manager:
        - NONE: Flush at normal interval
        - THROTTLE: Add delay between flushes
        - BATCH: Aggregate and flush less frequently
        - REJECT: Drop non-critical updates entirely

        Args:
            send_progress: Function to send progress to manager
        """
        self._running = True
        batch_accumulation_cycles = 0

        while self._running:
            try:
                # Base sleep interval
                await asyncio.sleep(self._progress_flush_interval)

                # Check backpressure state (AD-37)
                if self._backpressure_manager is not None:
                    # REJECT level: drop non-critical updates entirely
                    if self._backpressure_manager.should_reject_updates():
                        await self._state.clear_progress_buffer()
                        batch_accumulation_cycles = 0
                        continue

                    # BATCH level: accumulate updates, flush less often
                    if self._backpressure_manager.should_batch_only():
                        batch_accumulation_cycles += 1
                        # Flush every 4 cycles in batch mode
                        if batch_accumulation_cycles < 4:
                            continue
                        batch_accumulation_cycles = 0

                    # THROTTLE level: add extra delay
                    elif self._backpressure_manager.should_throttle():
                        throttle_delay = (
                            self._backpressure_manager.get_throttle_delay_seconds()
                        )
                        if throttle_delay > 0:
                            await asyncio.sleep(throttle_delay)

                # Flush the buffer
                await self.flush_progress_buffer(send_progress)

            except asyncio.CancelledError:
                break
            except Exception:
                pass

    def stop(self) -> None:
        """Stop background loops."""
        self._running = False

    def get_execution_metrics(self) -> dict[str, Any]:
        """
        Get execution metrics summary.

        Returns:
            Dictionary with execution metrics
        """
        return {
            "available_cores": self.available_cores,
            "total_cores": self.total_cores,
            "throughput": self.get_throughput(),
            "expected_throughput": self.get_expected_throughput(),
            "completion_samples": self._state.get_completion_sample_count(),
            "buffered_updates": self._state.get_buffered_update_count(),
        }

    @staticmethod
    def create_initial_progress(
        job_id: str,
        workflow_id: str,
        allocated_cores: list[int],
        available_cores: int,
        cores_requested: int,
    ) -> WorkflowProgress:
        """
        Create initial progress tracker for a workflow.

        Args:
            job_id: Job identifier
            workflow_id: Workflow identifier
            allocated_cores: List of allocated core indices
            available_cores: Worker's available cores
            cores_requested: Number of cores requested

        Returns:
            Initial WorkflowProgress instance
        """
        return WorkflowProgress(
            job_id=job_id,
            workflow_id=workflow_id,
            workflow_name="",
            status=WorkflowStatus.RUNNING.value,
            completed_count=0,
            failed_count=0,
            rate_per_second=0.0,
            elapsed_seconds=0.0,
            timestamp=time.monotonic(),
            collected_at=time.time(),
            assigned_cores=allocated_cores,
            worker_available_cores=available_cores,
            worker_workflow_completed_cores=0,
            worker_workflow_assigned_cores=cores_requested,
        )
