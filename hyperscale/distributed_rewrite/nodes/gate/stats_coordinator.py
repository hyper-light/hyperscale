"""
Gate statistics coordination module.

Provides tiered update classification, batch stats loops, and windowed
stats aggregation following the REFACTOR.md pattern.
"""

import asyncio
from typing import TYPE_CHECKING

from hyperscale.distributed_rewrite.models import (
    JobStatus,
    UpdateTier,
    JobStatusPush,
)
from hyperscale.distributed_rewrite.jobs import WindowedStatsCollector

if TYPE_CHECKING:
    from hyperscale.distributed_rewrite.nodes.gate.state import GateRuntimeState
    from hyperscale.logging import Logger
    from hyperscale.taskex import TaskRunner


class GateStatsCoordinator:
    """
    Coordinates statistics collection, classification, and distribution.

    Responsibilities:
    - Classify update tiers (IMMEDIATE vs PERIODIC)
    - Send immediate updates to clients
    - Run batch stats aggregation loop
    - Push windowed stats to clients
    """

    def __init__(
        self,
        state: "GateRuntimeState",
        logger: "Logger",
        task_runner: "TaskRunner",
        windowed_stats: WindowedStatsCollector,
        get_job_callback: callable,
        get_job_status: callable,
        send_tcp: callable,
        stats_push_interval_ms: float = 1000.0,
    ) -> None:
        self._state = state
        self._logger = logger
        self._task_runner = task_runner
        self._windowed_stats = windowed_stats
        self._get_job_callback = get_job_callback
        self._get_job_status = get_job_status
        self._send_tcp = send_tcp
        self._stats_push_interval_ms = stats_push_interval_ms
        self._batch_stats_task: asyncio.Task | None = None

    def classify_update_tier(
        self,
        job_id: str,
        old_status: str | None,
        new_status: str,
    ) -> str:
        """
        Classify whether an update should be sent immediately or batched.

        Args:
            job_id: Job identifier
            old_status: Previous job status (None if first update)
            new_status: New job status

        Returns:
            UpdateTier value (IMMEDIATE or PERIODIC)
        """
        # Final states are always immediate
        if new_status in (
            JobStatus.COMPLETED.value,
            JobStatus.FAILED.value,
            JobStatus.CANCELLED.value,
        ):
            return UpdateTier.IMMEDIATE.value

        # First transition to RUNNING is immediate
        if old_status is None and new_status == JobStatus.RUNNING.value:
            return UpdateTier.IMMEDIATE.value

        # Any status change is immediate
        if old_status != new_status:
            return UpdateTier.IMMEDIATE.value

        # Progress updates within same status are periodic
        return UpdateTier.PERIODIC.value

    async def send_immediate_update(
        self,
        job_id: str,
        event_type: str,
        payload: bytes | None = None,
    ) -> None:
        """
        Send an immediate status update to the job's callback address.

        Args:
            job_id: Job identifier
            event_type: Type of event (status_change, progress, etc.)
            payload: Optional pre-serialized payload
        """
        if not (callback := self._get_job_callback(job_id)):
            return

        if not (job := self._get_job_status(job_id)):
            return

        # Build status push message
        push = JobStatusPush(
            job_id=job_id,
            status=job.status,
            total_completed=getattr(job, 'total_completed', 0),
            total_failed=getattr(job, 'total_failed', 0),
            overall_rate=getattr(job, 'overall_rate', 0.0),
            elapsed_seconds=getattr(job, 'elapsed_seconds', 0.0),
            is_final=job.status in (
                JobStatus.COMPLETED.value,
                JobStatus.FAILED.value,
                JobStatus.CANCELLED.value,
            ),
        )

        try:
            await self._send_tcp(callback, "job_status_push", push.dump())
        except Exception:
            pass  # Best effort - don't fail on push errors

    async def start_batch_stats_loop(self) -> None:
        """Start the background batch stats aggregation loop."""
        if self._batch_stats_task is None or self._batch_stats_task.done():
            self._batch_stats_task = self._task_runner.run(self._batch_stats_loop)

    async def stop_batch_stats_loop(self) -> None:
        """Stop the background batch stats loop."""
        if self._batch_stats_task and not self._batch_stats_task.done():
            self._batch_stats_task.cancel()
            try:
                await self._batch_stats_task
            except asyncio.CancelledError:
                pass

    async def _batch_stats_loop(self) -> None:
        """
        Background loop for periodic stats aggregation and push.

        Implements AD-37 explicit backpressure handling by adjusting
        flush interval based on system backpressure level:
        - NONE: Normal interval
        - THROTTLE: 2x interval (reduce update frequency)
        - BATCH: 4x interval (accept only batched updates)
        - REJECT: 8x interval (aggressive slowdown, drop non-critical)
        """
        from hyperscale.distributed_rewrite.reliability import BackpressureLevel

        base_interval_seconds = self._stats_push_interval_ms / 1000.0

        while True:
            try:
                # AD-37: Check backpressure level and adjust interval
                backpressure_level = self._state.get_max_backpressure_level()

                if backpressure_level == BackpressureLevel.THROTTLE:
                    interval_seconds = base_interval_seconds * 2.0
                elif backpressure_level == BackpressureLevel.BATCH:
                    interval_seconds = base_interval_seconds * 4.0
                elif backpressure_level == BackpressureLevel.REJECT:
                    interval_seconds = base_interval_seconds * 8.0
                else:
                    interval_seconds = base_interval_seconds

                await asyncio.sleep(interval_seconds)

                # Skip push entirely under REJECT backpressure (non-critical updates)
                if backpressure_level == BackpressureLevel.REJECT:
                    continue

                # Get jobs with pending stats
                pending_jobs = self._windowed_stats.get_jobs_with_pending_stats()

                for job_id in pending_jobs:
                    await self._push_windowed_stats(job_id)

            except asyncio.CancelledError:
                break
            except Exception:
                # Log and continue
                await asyncio.sleep(1.0)

    async def _push_windowed_stats(self, job_id: str) -> None:
        """
        Push aggregated windowed stats to client callback.

        Args:
            job_id: Job identifier
        """
        if not (callback := self._state._progress_callbacks.get(job_id)):
            return

        # Get aggregated stats from windowed collector
        stats = self._windowed_stats.get_aggregated_stats(job_id)
        if not stats:
            return

        try:
            await self._send_tcp(callback, "windowed_stats_push", stats.dump())
        except Exception:
            pass  # Best effort


__all__ = ["GateStatsCoordinator"]
