"""
Job tracking for HyperscaleClient.

Handles job lifecycle tracking, status updates, completion events, and callbacks.
"""

import asyncio
from typing import Callable, Coroutine, Any

from hyperscale.distributed.models import (
    JobStatus,
    ClientJobResult,
    JobStatusPush,
    WorkflowResultPush,
    ReporterResultPush,
    GlobalJobStatus,
)
from hyperscale.distributed.nodes.client.state import ClientState
from hyperscale.logging import Logger
from hyperscale.logging.hyperscale_logging_models import ServerDebug

PollGateForStatusFunc = Callable[[str], Coroutine[Any, Any, GlobalJobStatus | None]]

TERMINAL_STATUSES = frozenset(
    {
        JobStatus.COMPLETED.value,
        JobStatus.FAILED.value,
        JobStatus.CANCELLED.value,
    }
)


class ClientJobTracker:
    """
    Manages job lifecycle tracking and completion events.

    Tracks job status, manages completion events, and invokes user callbacks
    for status updates, progress, workflow results, and reporter results.
    """

    DEFAULT_POLL_INTERVAL_SECONDS: float = 5.0

    def __init__(
        self,
        state: ClientState,
        logger: Logger,
        poll_gate_for_status: PollGateForStatusFunc | None = None,
    ) -> None:
        self._state = state
        self._logger = logger
        self._poll_gate_for_status = poll_gate_for_status

    def initialize_job_tracking(
        self,
        job_id: str,
        on_status_update: Callable[[JobStatusPush], None] | None = None,
        on_progress_update: Callable | None = None,
        on_workflow_result: Callable[[WorkflowResultPush], None] | None = None,
        on_reporter_result: Callable[[ReporterResultPush], None] | None = None,
    ) -> None:
        """
        Initialize tracking structures for a new job.

        Creates job result, completion event, and registers callbacks.

        Args:
            job_id: Job identifier
            on_status_update: Optional callback for JobStatusPush updates
            on_progress_update: Optional callback for WindowedStatsPush updates
            on_workflow_result: Optional callback for WorkflowResultPush updates
            on_reporter_result: Optional callback for ReporterResultPush updates
        """
        # Create initial job result with SUBMITTED status
        self._state._jobs[job_id] = ClientJobResult(
            job_id=job_id,
            status=JobStatus.SUBMITTED.value,
        )

        # Create completion event
        self._state._job_events[job_id] = asyncio.Event()

        # Register callbacks if provided
        if on_status_update:
            self._state._job_callbacks[job_id] = on_status_update
        if on_progress_update:
            self._state._progress_callbacks[job_id] = on_progress_update
        if on_workflow_result:
            self._state._workflow_callbacks[job_id] = on_workflow_result
        if on_reporter_result:
            self._state._reporter_callbacks[job_id] = on_reporter_result

    def update_job_status(self, job_id: str, status: str) -> None:
        """
        Update job status and signal completion event.

        Args:
            job_id: Job identifier
            status: New status (JobStatus value)
        """
        job = self._state._jobs.get(job_id)
        if job:
            job.status = status

        # Signal completion event
        event = self._state._job_events.get(job_id)
        if event:
            event.set()

    def mark_job_failed(self, job_id: str, error: str | None) -> None:
        """
        Mark a job as failed and signal completion.

        Args:
            job_id: Job identifier
            error: Error message
        """
        job = self._state._jobs.get(job_id)
        if job:
            job.status = JobStatus.FAILED.value
            job.error = error

        # Signal completion event
        event = self._state._job_events.get(job_id)
        if event:
            event.set()

    async def wait_for_job(
        self,
        job_id: str,
        timeout: float | None = None,
        poll_interval: float | None = None,
    ) -> ClientJobResult:
        """
        Wait for a job to complete with periodic gate polling for reliability.

        Blocks until the job reaches a terminal state (COMPLETED, FAILED, etc.)
        or timeout is exceeded. Periodically polls the gate to recover from
        missed status pushes.

        Args:
            job_id: Job identifier from submit_job
            timeout: Maximum time to wait in seconds (None = wait forever)
            poll_interval: Interval for polling gate (None = use default)

        Returns:
            ClientJobResult with final status

        Raises:
            KeyError: If job_id not found
            asyncio.TimeoutError: If timeout exceeded
        """
        if job_id not in self._state._jobs:
            raise KeyError(f"Unknown job: {job_id}")

        event = self._state._job_events[job_id]
        effective_poll_interval = poll_interval or self.DEFAULT_POLL_INTERVAL_SECONDS

        async def poll_until_complete():
            while not event.is_set():
                await asyncio.sleep(effective_poll_interval)
                if event.is_set():
                    break
                await self._poll_and_update_status(job_id)

        poll_task: asyncio.Task | None = None
        if self._poll_gate_for_status:
            poll_task = asyncio.create_task(poll_until_complete())

        try:
            if timeout:
                await asyncio.wait_for(event.wait(), timeout=timeout)
            else:
                await event.wait()
        finally:
            if poll_task and not poll_task.done():
                poll_task.cancel()
                try:
                    await poll_task
                except asyncio.CancelledError:
                    pass

        return self._state._jobs[job_id]

    async def _poll_and_update_status(self, job_id: str) -> None:
        if not self._poll_gate_for_status:
            return

        try:
            remote_status = await self._poll_gate_for_status(job_id)
            if not remote_status:
                return

            job = self._state._jobs.get(job_id)
            if not job:
                return

            job.status = remote_status.status
            job.total_completed = remote_status.total_completed
            job.total_failed = remote_status.total_failed
            if hasattr(remote_status, "overall_rate"):
                job.overall_rate = remote_status.overall_rate
            if hasattr(remote_status, "elapsed_seconds"):
                job.elapsed_seconds = remote_status.elapsed_seconds

            if remote_status.status in TERMINAL_STATUSES:
                event = self._state._job_events.get(job_id)
                if event:
                    event.set()

        except Exception as poll_error:
            await self._logger.log(
                ServerDebug(
                    message=f"Status poll failed for job {job_id[:8]}...: {poll_error}",
                    node_host="client",
                    node_port=0,
                    node_id="tracker",
                )
            )

    def get_job_status(self, job_id: str) -> ClientJobResult | None:
        """
        Get current status of a job (non-blocking).

        Args:
            job_id: Job identifier

        Returns:
            ClientJobResult if job exists, else None
        """
        return self._state._jobs.get(job_id)
