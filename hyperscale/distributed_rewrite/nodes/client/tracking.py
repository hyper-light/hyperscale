"""
Job tracking for HyperscaleClient.

Handles job lifecycle tracking, status updates, completion events, and callbacks.
"""

import asyncio
from typing import Callable

from hyperscale.distributed_rewrite.models import (
    JobStatus,
    ClientJobResult,
    JobStatusPush,
    WorkflowResultPush,
    ReporterResultPush,
)
from hyperscale.distributed_rewrite.nodes.client.state import ClientState
from hyperscale.logging import Logger


class ClientJobTracker:
    """
    Manages job lifecycle tracking and completion events.

    Tracks job status, manages completion events, and invokes user callbacks
    for status updates, progress, workflow results, and reporter results.
    """

    def __init__(self, state: ClientState, logger: Logger) -> None:
        self._state = state
        self._logger = logger

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
    ) -> ClientJobResult:
        """
        Wait for a job to complete.

        Blocks until the job reaches a terminal state (COMPLETED, FAILED, etc.)
        or timeout is exceeded.

        Args:
            job_id: Job identifier from submit_job
            timeout: Maximum time to wait in seconds (None = wait forever)

        Returns:
            ClientJobResult with final status

        Raises:
            KeyError: If job_id not found
            asyncio.TimeoutError: If timeout exceeded
        """
        if job_id not in self._state._jobs:
            raise KeyError(f"Unknown job: {job_id}")

        event = self._state._job_events[job_id]

        if timeout:
            await asyncio.wait_for(event.wait(), timeout=timeout)
        else:
            await event.wait()

        return self._state._jobs[job_id]

    def get_job_status(self, job_id: str) -> ClientJobResult | None:
        """
        Get current status of a job (non-blocking).

        Args:
            job_id: Job identifier

        Returns:
            ClientJobResult if job exists, else None
        """
        return self._state._jobs.get(job_id)
