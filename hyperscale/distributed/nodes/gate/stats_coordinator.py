"""
Gate statistics coordination module.

Provides tiered update classification, batch stats loops, and windowed
stats aggregation following the REFACTOR.md pattern.
"""

from typing import TYPE_CHECKING, Callable

from hyperscale.distributed.models import (
    JobStatus,
    UpdateTier,
    JobStatusPush,
    JobBatchPush,
    DCStats,
    GlobalJobStatus,
)
from hyperscale.distributed.jobs import WindowedStatsCollector

if TYPE_CHECKING:
    from hyperscale.distributed.nodes.gate.state import GateRuntimeState
    from hyperscale.logging import Logger
    from hyperscale.distributed.taskex import TaskRunner


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
        get_job_callback: Callable[[str], tuple[str, int] | None],
        get_job_status: Callable[[str], GlobalJobStatus | None],
        get_all_running_jobs: Callable[[], list[tuple[str, GlobalJobStatus]]],
        send_tcp: Callable,
    ) -> None:
        self._state: "GateRuntimeState" = state
        self._logger: "Logger" = logger
        self._task_runner: "TaskRunner" = task_runner
        self._windowed_stats: WindowedStatsCollector = windowed_stats
        self._get_job_callback: Callable[[str], tuple[str, int] | None] = (
            get_job_callback
        )
        self._get_job_status: Callable[[str], GlobalJobStatus | None] = get_job_status
        self._get_all_running_jobs: Callable[[], list[tuple[str, GlobalJobStatus]]] = (
            get_all_running_jobs
        )
        self._send_tcp: Callable = send_tcp

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
        is_final = job.status in (
            JobStatus.COMPLETED.value,
            JobStatus.FAILED.value,
            JobStatus.CANCELLED.value,
        )
        message = f"Job {job_id}: {job.status}"
        if is_final:
            message = f"Job {job_id} {job.status.lower()}"

        push = JobStatusPush(
            job_id=job_id,
            status=job.status,
            message=message,
            total_completed=getattr(job, "total_completed", 0),
            total_failed=getattr(job, "total_failed", 0),
            overall_rate=getattr(job, "overall_rate", 0.0),
            elapsed_seconds=getattr(job, "elapsed_seconds", 0.0),
            is_final=is_final,
        )

        try:
            await self._send_tcp(callback, "job_status_push", push.dump())
        except Exception:
            pass  # Best effort - don't fail on push errors

    async def batch_stats_update(self) -> None:
        """
        Process a batch of Tier 2 (Periodic) updates per AD-15.

        Aggregates pending progress updates and pushes JobBatchPush messages
        to clients that have registered callbacks. This is more efficient than
        sending each update individually.
        """
        running_jobs = self._get_all_running_jobs()
        jobs_with_callbacks: list[tuple[str, GlobalJobStatus, tuple[str, int]]] = []

        for job_id, job in running_jobs:
            if callback := self._get_job_callback(job_id):
                jobs_with_callbacks.append((job_id, job, callback))

        if not jobs_with_callbacks:
            return

        for job_id, job, callback in jobs_with_callbacks:
            all_step_stats: list = []
            for datacenter_progress in job.datacenters:
                if (
                    hasattr(datacenter_progress, "step_stats")
                    and datacenter_progress.step_stats
                ):
                    all_step_stats.extend(datacenter_progress.step_stats)

            per_dc_stats = [
                DCStats(
                    datacenter=datacenter_progress.datacenter,
                    status=datacenter_progress.status,
                    completed=datacenter_progress.total_completed,
                    failed=datacenter_progress.total_failed,
                    rate=datacenter_progress.overall_rate,
                )
                for datacenter_progress in job.datacenters
            ]

            batch_push = JobBatchPush(
                job_id=job_id,
                status=job.status,
                step_stats=all_step_stats,
                total_completed=job.total_completed,
                total_failed=job.total_failed,
                overall_rate=job.overall_rate,
                elapsed_seconds=job.elapsed_seconds,
                per_dc_stats=per_dc_stats,
            )

            try:
                await self._send_tcp(
                    callback,
                    "job_batch_push",
                    batch_push.dump(),
                    timeout=2.0,
                )
            except Exception:
                pass  # Client unreachable - continue with others

    async def push_windowed_stats(self) -> None:
        """
        Push windowed stats for all jobs with pending aggregated data.

        Iterates over jobs that have accumulated windowed stats and pushes
        them to their registered callback addresses.
        """
        pending_jobs = self._windowed_stats.get_jobs_with_pending_stats()

        for job_id in pending_jobs:
            await self._push_windowed_stats(job_id)

    async def _push_windowed_stats(self, job_id: str) -> None:
        """
        Push aggregated windowed stats to client callback.

        Args:
            job_id: Job identifier
        """
        if not (callback := self._state._progress_callbacks.get(job_id)):
            return

        stats_list = await self._windowed_stats.get_aggregated_stats(job_id)
        if not stats_list:
            return

        for stats in stats_list:
            try:
                await self._send_tcp(callback, "windowed_stats_push", stats.dump())
            except Exception:
                pass


__all__ = ["GateStatsCoordinator"]
