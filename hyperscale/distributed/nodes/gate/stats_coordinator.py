"""
Gate statistics coordination module.

Provides tiered update classification, batch stats loops, and windowed
stats aggregation following the REFACTOR.md pattern.
"""

import asyncio
from typing import TYPE_CHECKING, Awaitable, Callable

from hyperscale.distributed.models import (
    JobStatus,
    UpdateTier,
    JobStatusPush,
    JobBatchPush,
    DCStats,
    GlobalJobStatus,
)
from hyperscale.distributed.jobs import WindowedStatsCollector
from hyperscale.logging.hyperscale_logging_models import ServerDebug, ServerError

from hyperscale.distributed.runtime import Clock



if TYPE_CHECKING:
    from hyperscale.distributed.nodes.gate.state import GateRuntimeState
    from hyperscale.logging import Logger
    from hyperscale.distributed.taskex import TaskRunner


ForwardStatusPushFunc = Callable[[str, bytes], Awaitable[bool]]

TERMINAL_JOB_STATUSES = frozenset(
    {
        JobStatus.COMPLETED.value,
        JobStatus.FAILED.value,
        JobStatus.CANCELLED.value,
        JobStatus.TIMEOUT.value,
    }
)


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
        node_host: str,
        node_port: int,
        node_id: str,
        task_runner: "TaskRunner",
        windowed_stats: WindowedStatsCollector,
        get_job_callback: Callable[[str], tuple[str, int] | None],
        get_job_status: Callable[[str], GlobalJobStatus | None],
        get_all_running_jobs: Callable[[], list[tuple[str, GlobalJobStatus]]],
        has_job: Callable[[str], bool],
        send_tcp: Callable,
        client_push_timeout_seconds: float,
        clock: Clock,
        forward_status_push_to_peers: ForwardStatusPushFunc | None = None,
    ) -> None:
        self._clock: Clock = clock
        self._state: "GateRuntimeState" = state
        self._logger: "Logger" = logger
        self._node_host: str = node_host
        self._node_port: int = node_port
        self._node_id: str = node_id
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
        self._client_push_timeout_seconds: float = client_push_timeout_seconds
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
            JobStatus.TIMEOUT.value,
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

        await self._deliver_immediate_update(job_id, callback)

    async def _deliver_immediate_update(
        self,
        job_id: str,
        callback: tuple[str, int],
    ) -> None:
        """Build, record and push the job's current status to its callback (AD-15 immediate tier)."""
        if not (job := self._get_job_status(job_id)):
            return

        push_data = self._build_job_status_push(job_id, job, callback).dump()
        sequence = await self._state.record_client_update(
            job_id,
            "job_status_push",
            push_data,
            self._clock.monotonic(),
        )

        delivered = await self._send_status_push_with_retry(
            job_id,
            callback,
            push_data,
            allow_peer_forwarding=True,
        )
        await self._record_position_if_delivered(job_id, callback, sequence, delivered)

    def _build_job_status_push(
        self,
        job_id: str,
        job: GlobalJobStatus,
        callback: tuple[str, int],
    ) -> JobStatusPush:
        """Build the status push for a job, marking it final when the job is terminal."""
        is_final = job.status in TERMINAL_JOB_STATUSES
        message = f"Job {job_id} {job.status.lower()}" if is_final else f"Job {job_id}: {job.status}"

        return JobStatusPush(
            job_id=job_id,
            status=job.status,
            message=message,
            total_completed=getattr(job, "total_completed", 0),
            total_failed=getattr(job, "total_failed", 0),
            overall_rate=getattr(job, "overall_rate", 0.0),
            elapsed_seconds=getattr(job, "elapsed_seconds", 0.0),
            is_final=is_final,
            callback_addr=callback,
        )

    async def _record_position_if_delivered(
        self,
        job_id: str,
        callback: tuple[str, int],
        sequence: int,
        delivered: bool,
    ) -> None:
        """Advance the callback's replay position only once its push was delivered."""
        if delivered:
            await self._state.set_client_update_position(job_id, callback, sequence)

    async def _send_status_push_with_retry(
        self,
        job_id: str,
        callback: tuple[str, int],
        push_data: bytes,
        allow_peer_forwarding: bool,
    ) -> bool:
        delivered, last_error = await self._attempt_status_push(callback, push_data)
        if delivered:
            return True

        forwarded, last_error, forward_note = await self._try_peer_forwarding(
            job_id,
            push_data,
            allow_peer_forwarding,
            last_error,
        )
        if forwarded:
            return True

        await self._logger.log(
            ServerError(
                message=(
                    f"Failed to deliver status push for job {job_id} after "
                    f"{self.CALLBACK_PUSH_MAX_RETRIES} retries{forward_note}: {last_error}"
                ),
                node_host=self._node_host,
                node_port=self._node_port,
                node_id=self._node_id,
            )
        )
        return False

    async def _attempt_status_push(
        self,
        callback: tuple[str, int],
        push_data: bytes,
    ) -> tuple[bool, Exception | None]:
        """Push a job status to the client with backoff; returns (delivered, last error)."""
        last_error: Exception | None = None

        for attempt in range(self.CALLBACK_PUSH_MAX_RETRIES):
            try:
                response, _ = await self._send_tcp(
                    callback,
                    "job_status_push",
                    push_data,
                )
                self._raise_on_rejected_status_push(response)
                return True, None
            except Exception as send_error:
                last_error = send_error
                await self._sleep_before_callback_retry(attempt)

        return False, last_error

    @staticmethod
    def _raise_on_rejected_status_push(response: bytes | Exception | None) -> None:
        """Raise the transport error or a rejection so the status push is retried."""
        if isinstance(response, Exception):
            raise response
        if response not in (b"ok", None):
            raise RuntimeError(
                f"job_status_push rejected with {response!r}"
            )

    async def _sleep_before_callback_retry(self, attempt: int) -> None:
        """Back off exponentially (capped) before every attempt but the last."""
        if attempt < self.CALLBACK_PUSH_MAX_RETRIES - 1:
            delay = min(
                self.CALLBACK_PUSH_BASE_DELAY_SECONDS * (2**attempt),
                self.CALLBACK_PUSH_MAX_DELAY_SECONDS,
            )
            await self._clock.sleep(delay)

    async def _try_peer_forwarding(
        self,
        job_id: str,
        push_data: bytes,
        allow_peer_forwarding: bool,
        last_error: Exception | None,
    ) -> tuple[bool, Exception | None, str]:
        """Forward an undeliverable status push through peer gates when allowed and wired."""
        if not (allow_peer_forwarding and self._forward_status_push_to_peers):
            return False, last_error, ""

        forwarded, last_error = await self._forward_status_push_once(job_id, push_data, last_error)
        return forwarded, last_error, " and peer forwarding failed"

    async def _forward_status_push_once(
        self,
        job_id: str,
        push_data: bytes,
        last_error: Exception | None,
    ) -> tuple[bool, Exception | None]:
        """Run one peer forward, carrying its error forward as the last error on failure."""
        try:
            forwarded = await self._forward_status_push_to_peers(job_id, push_data)
        except Exception as forward_error:
            return False, forward_error
        return forwarded, last_error

    async def _send_periodic_push_with_retry(
        self,
        callback: tuple[str, int],
        message_type: str,
        data: bytes,
        timeout: float = 2.0,
    ) -> bool:
        last_error: Exception | None = None

        for attempt in range(self.CALLBACK_PUSH_MAX_RETRIES):
            try:
                response, _ = await self._send_tcp(callback, message_type, data, timeout=timeout)
                self._raise_on_transport_error(response)
                return True
            except Exception as send_error:
                last_error = send_error
                await self._sleep_before_callback_retry(attempt)

        await self._logger.log(
            ServerError(
                message=(
                    f"Failed to deliver {message_type} to client {callback} after "
                    f"{self.CALLBACK_PUSH_MAX_RETRIES} retries: {last_error}"
                ),
                node_host=self._node_host,
                node_port=self._node_port,
                node_id=self._node_id,
            )
        )

        return False

    @staticmethod
    def _raise_on_transport_error(response: bytes | Exception | None) -> None:
        """Raise a returned transport error so the periodic push is retried."""
        # send_tcp returns transport errors rather than raising.
        if isinstance(response, Exception):
            raise response

    def _build_job_batch_push(
        self,
        job_id: str,
        job: GlobalJobStatus,
    ) -> JobBatchPush:
        all_step_stats = self._collect_step_stats(job)

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

        return JobBatchPush(
            job_id=job_id,
            status=job.status,
            step_stats=all_step_stats,
            total_completed=job.total_completed,
            total_failed=job.total_failed,
            overall_rate=job.overall_rate,
            elapsed_seconds=job.elapsed_seconds,
            per_dc_stats=per_dc_stats,
        )

    @staticmethod
    def _collect_step_stats(job: GlobalJobStatus) -> list:
        """Flatten every datacenter's non-empty step stats, in datacenter order."""
        all_step_stats: list = []
        for datacenter_progress in job.datacenters:
            if step_stats := getattr(datacenter_progress, "step_stats", None):
                all_step_stats.extend(step_stats)
        return all_step_stats

    def _get_progress_callbacks(self, job_id: str) -> list[tuple[str, int]]:
        candidate_callbacks = (
            self._get_job_callback(job_id),
            self._state._progress_callbacks.get(job_id),
        )
        return list(dict.fromkeys(filter(None, candidate_callbacks)))

    async def _send_batch_push_to_callbacks(
        self,
        job_id: str,
        job: GlobalJobStatus,
        callbacks: list[tuple[str, int]],
    ) -> None:
        unique_callbacks = list(dict.fromkeys(callbacks))
        if not unique_callbacks:
            return

        batch_push = self._build_job_batch_push(job_id, job)
        payload = batch_push.dump()
        sequence = await self._state.record_client_update(
            job_id,
            "job_batch_push",
            payload,
            self._clock.monotonic(),
        )

        for callback in unique_callbacks:
            delivered = await self._send_periodic_push_with_retry(
                callback,
                "job_batch_push",
                payload,
                timeout=self._client_push_timeout_seconds,
            )
            await self._record_position_if_delivered(job_id, callback, sequence, delivered)

    async def _send_batch_push_logging_failure(
        self,
        job_id: str,
        job: GlobalJobStatus,
        callbacks: list[tuple[str, int]],
        failure_action: str,
    ) -> None:
        """Send a batch push, logging (not raising) a failure so other jobs still get theirs."""
        try:
            await self._send_batch_push_to_callbacks(job_id, job, callbacks)
        except Exception as error:
            await self._logger.log(
                ServerError(
                    message=(
                        f"Failed to {failure_action} batch stats update for job "
                        f"{job_id}: {error}"
                    ),
                    node_host=self._node_host,
                    node_port=self._node_port,
                    node_id=self._node_id,
                )
            )

    async def send_progress_replay(self, job_id: str) -> None:
        if not self._has_job(job_id):
            return

        callbacks = self._get_progress_callbacks(job_id)
        if not callbacks:
            return

        await self._replay_progress_to_callbacks(job_id, callbacks)

    async def _replay_progress_to_callbacks(
        self,
        job_id: str,
        callbacks: list[tuple[str, int]],
    ) -> None:
        """Replay the job's current batch stats to its callbacks when the job status exists."""
        if not (job := self._get_job_status(job_id)):
            return

        await self._send_batch_push_logging_failure(job_id, job, callbacks, "replay")

    async def batch_stats_update(self) -> None:
        jobs_with_callbacks = self._collect_jobs_with_callbacks(self._get_all_running_jobs())

        if not jobs_with_callbacks:
            return

        for job_id, job, callbacks in jobs_with_callbacks:
            await self._send_batch_push_logging_failure(job_id, job, callbacks, "send")

    def _collect_jobs_with_callbacks(
        self,
        running_jobs: list[tuple[str, GlobalJobStatus]],
    ) -> list[tuple[str, GlobalJobStatus, list[tuple[str, int]]]]:
        """Pair each still-known running job with its progress callbacks, skipping jobs without any."""
        return [
            job_with_callbacks
            for job_id, job in running_jobs
            if (job_with_callbacks := self._job_with_callbacks(job_id, job))
        ]

    def _job_with_callbacks(
        self,
        job_id: str,
        job: GlobalJobStatus,
    ) -> tuple[str, GlobalJobStatus, list[tuple[str, int]]] | None:
        """Return the job with its progress callbacks, or None when unknown or callback-less."""
        if not self._has_job(job_id):
            return None
        callbacks = self._get_progress_callbacks(job_id)
        return (job_id, job, callbacks) if callbacks else None

    async def push_windowed_stats_for_job(self, job_id: str) -> None:
        await self._push_windowed_stats(job_id)

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
        if (discard_message := self._windowed_stats_discard_message(job_id)) is not None:
            await self._discard_windowed_stats(job_id, discard_message)
            return

        if not (callback := self._state._progress_callbacks.get(job_id)):
            await self._discard_windowed_stats(
                job_id,
                f"No progress callback registered for job {job_id}, cleaning up windows",
            )
            return

        await self._push_aggregated_windowed_stats(job_id, callback)

    def _windowed_stats_discard_message(self, job_id: str) -> str | None:
        """Why a job's windowed stats must be discarded (unknown or terminal job), or None."""
        if not self._has_job(job_id):
            return f"Discarding windowed stats for unknown job {job_id}"
        return self._terminal_windowed_stats_discard_message(job_id)

    def _terminal_windowed_stats_discard_message(self, job_id: str) -> str | None:
        """Discard message for a missing or terminal job's windowed stats, or None while it runs."""
        if not (job_status := self._get_job_status(job_id)):
            return f"Discarding windowed stats for job {job_id} in terminal state missing"
        if job_status.status in TERMINAL_JOB_STATUSES:
            return f"Discarding windowed stats for job {job_id} in terminal state {job_status.status}"
        return None

    async def _discard_windowed_stats(self, job_id: str, message: str) -> None:
        """Log why a job's windowed stats are dropped, then release its windows."""
        await self._logger.log(
            ServerDebug(
                message=message,
                node_host=self._node_host,
                node_port=self._node_port,
                node_id=self._node_id,
            )
        )
        await self._windowed_stats.cleanup_job_windows(job_id)

    async def _push_aggregated_windowed_stats(
        self,
        job_id: str,
        callback: tuple[str, int],
    ) -> None:
        """Record and push each aggregated windowed stats entry to the job's callback."""
        stats_list = await self._windowed_stats.get_aggregated_stats(job_id)
        if not stats_list:
            return

        for stats in stats_list:
            payload = stats.dump()
            sequence = await self._state.record_client_update(
                job_id,
                "windowed_stats_push",
                payload,
                self._clock.monotonic(),
            )
            delivered = await self._send_periodic_push_with_retry(
                callback,
                "windowed_stats_push",
                payload,
            )
            await self._record_position_if_delivered(job_id, callback, sequence, delivered)


__all__ = ["GateStatsCoordinator"]
