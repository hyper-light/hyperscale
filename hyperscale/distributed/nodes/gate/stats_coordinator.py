"""
Gate statistics coordination module.

Provides tiered update classification, batch stats loops, and windowed
stats aggregation following the REFACTOR.md pattern.
"""

import asyncio
from typing import TYPE_CHECKING, Callable, Coroutine, Any

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


ForwardStatusPushFunc = Callable[[str, bytes], Coroutine[Any, Any, bool]]


class GateStatsCoordinator:
    """
    Coordinates statistics collection, classification, and distribution.

    Responsibilities:
    - Classify update tiers (IMMEDIATE vs PERIODIC)
    - Send immediate updates to clients
    - Run batch stats aggregation loop
    - Push windowed stats to clients
    """

    CALLBACK_PUSH_MAX_RETRIES: int = 3
    CALLBACK_PUSH_BASE_DELAY_SECONDS: float = 0.5
    CALLBACK_PUSH_MAX_DELAY_SECONDS: float = 2.0

    def __init__(
        self,
        state: "GateRuntimeState",
        logger: "Logger",
        task_runner: "TaskRunner",
        windowed_stats: WindowedStatsCollector,
        get_job_callback: Callable[[str], tuple[str, int] | None],
        get_job_status: Callable[[str], GlobalJobStatus | None],
        get_all_running_jobs: Callable[[], list[tuple[str, GlobalJobStatus]]],
        has_job: Callable[[str], bool],
        send_tcp: Callable,
        forward_status_push_to_peers: ForwardStatusPushFunc | None = None,
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
        self._has_job: Callable[[str], bool] = has_job
        self._send_tcp: Callable = send_tcp
        self._forward_status_push_to_peers: ForwardStatusPushFunc | None = (
            forward_status_push_to_peers
        )

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
        if not self._has_job(job_id):
            return

        if not (callback := self._get_job_callback(job_id)):
            return

        if not (job := self._get_job_status(job_id)):
            return

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

        push_data = push.dump()

        await self._send_status_push_with_retry(
            job_id,
            callback,
            push_data,
            allow_peer_forwarding=True,
        )

    async def _send_status_push_with_retry(
        self,
        job_id: str,
        callback: tuple[str, int],
        push_data: bytes,
        allow_peer_forwarding: bool,
    ) -> None:
        last_error: Exception | None = None

        for attempt in range(self.CALLBACK_PUSH_MAX_RETRIES):
            try:
                await self._send_tcp(callback, "job_status_push", push_data)
                return
            except Exception as send_error:
                last_error = send_error
                if attempt < self.CALLBACK_PUSH_MAX_RETRIES - 1:
                    delay = min(
                        self.CALLBACK_PUSH_BASE_DELAY_SECONDS * (2**attempt),
                        self.CALLBACK_PUSH_MAX_DELAY_SECONDS,
                    )
                    await asyncio.sleep(delay)

        if allow_peer_forwarding and self._forward_status_push_to_peers:
            try:
                forwarded = await self._forward_status_push_to_peers(job_id, push_data)
            except Exception as forward_error:
                last_error = forward_error
            else:
                if forwarded:
                    return

        await self._logger.log(
            {
                "level": "error",
                "message": (
                    f"Failed to deliver status push for job {job_id} after "
                    f"{self.CALLBACK_PUSH_MAX_RETRIES} retries: {last_error}"
                ),
            }
        )

    async def _send_periodic_push_with_retry(
        self,
        callback: tuple[str, int],
        message_type: str,
        data: bytes,
        timeout: float = 2.0,
    ) -> bool:
        for attempt in range(self.PERIODIC_PUSH_MAX_RETRIES):
            try:
                await self._send_tcp(callback, message_type, data, timeout=timeout)
                return True
            except Exception:
                if attempt < self.PERIODIC_PUSH_MAX_RETRIES - 1:
                    await asyncio.sleep(
                        self.PERIODIC_PUSH_BASE_DELAY_SECONDS * (2**attempt)
                    )
        return False

    async def batch_stats_update(self) -> None:
        running_jobs = self._get_all_running_jobs()
        jobs_with_callbacks: list[tuple[str, GlobalJobStatus, tuple[str, int]]] = []

        for job_id, job in running_jobs:
            if not self._has_job(job_id):
                continue
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

            await self._send_periodic_push_with_retry(
                callback,
                "job_batch_push",
                batch_push.dump(),
                timeout=2.0,
            )

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
        if not self._has_job(job_id):
            return

        if not (callback := self._state._progress_callbacks.get(job_id)):
            return

        stats_list = await self._windowed_stats.get_aggregated_stats(job_id)
        if not stats_list:
            return

        for stats in stats_list:
            await self._send_periodic_push_with_retry(
                callback,
                "windowed_stats_push",
                stats.dump(),
            )


__all__ = ["GateStatsCoordinator"]
