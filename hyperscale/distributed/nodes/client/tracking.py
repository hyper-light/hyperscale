"""
Job tracking for HyperscaleClient.

Handles job lifecycle tracking, status updates, completion events, and callbacks.
"""

import asyncio
from collections.abc import AsyncIterator
from typing import Awaitable, Callable

from hyperscale.distributed.models import (
    JobStatus,
    ClientJobResult,
    ClientWorkflowResult,
    JobStatusPush,
    WorkflowResultPush,
    ReporterResultPush,
    GlobalJobStatus,
)
from hyperscale.distributed.nodes.client.state import ClientState
from hyperscale.logging import Logger
from hyperscale.logging.hyperscale_logging_models import ServerDebug, ServerWarning

from hyperscale.distributed.runtime import Clock, RealClock
from hyperscale.distributed.nodes.client.status_application import (
    JobStatusApplier,
)


_DEFAULT_CLOCK: Clock = RealClock()

PollGateForStatusFunc = Callable[[str], Awaitable[GlobalJobStatus | None]]
# Has the job's gate send again what it recorded for this client and could
# not deliver; True once it has.
RequestReplayFunc = Callable[[str], Awaitable[bool]]

TERMINAL_STATUSES = frozenset(
    {
        JobStatus.COMPLETED.value,
        JobStatus.FAILED.value,
        JobStatus.CANCELLED.value,
        JobStatus.TIMEOUT.value,
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
        result_drain_timeout_seconds: float,
        poll_gate_for_status: PollGateForStatusFunc | None = None,
        request_replay: RequestReplayFunc | None = None,
    ) -> None:
        self._state = state
        self._logger = logger
        self._result_drain_timeout_seconds = result_drain_timeout_seconds
        self._poll_gate_for_status = poll_gate_for_status
        self._request_replay = request_replay
        self._status_applier = JobStatusApplier()

    def initialize_job_tracking(
        self,
        job_id: str,
        expected_workflow_ids: frozenset[str],
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
            expected_workflow_ids: The workflow ids the job is submitted with
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
        self._state._job_expected_workflows[job_id] = expected_workflow_ids
        self._state._job_results_events[job_id] = asyncio.Event()

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
        Update job status (order-guarded) and signal completion event.

        The write flows through ``JobStatusApplier``: backward
        transitions and post-terminal mutations are rejected, so e.g. a
        local CANCELLED mark cannot overwrite an already-COMPLETED job
        (AD-20: completed-after-cancel keeps COMPLETED).

        Args:
            job_id: Job identifier
            status: New status (JobStatus value)
        """
        job = self._state._jobs.get(job_id)
        if job:
            self._status_applier.apply_status(job, status)

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
        if (
            job
            and self._status_applier.apply_status(
                job, JobStatus.FAILED.value
            ).applied
        ):
            job.error = error

        # Signal completion event
        event = self._state._job_events.get(job_id)
        if event:
            event.set()

    async def stream_workflow_results(
        self,
        job_id: str,
        timeout: float | None = None,
    ) -> AsyncIterator[ClientWorkflowResult]:
        """
        Each of a job's workflow results as it arrives -- those that already
        arrived first -- until the job is done: the iteration ends when
        ``wait_for_job`` would return (terminal, with the results still in
        flight waited out), and raises what it would raise.

        Args:
            job_id: Job identifier from submit_job
            timeout: Maximum time to wait for the job in seconds (None =
                wait forever)

        Raises:
            KeyError: If job_id not found
            asyncio.TimeoutError: If timeout exceeded
        """
        if job_id not in self._state._jobs:
            raise KeyError(f"Unknown job: {job_id}")

        # Snapshot and subscription in one step (no await between): every
        # result arrives exactly once -- replayed, or put to the stream.
        stream: asyncio.Queue[ClientWorkflowResult | None] = asyncio.Queue()
        for result in self._state._jobs[job_id].workflow_results.values():
            stream.put_nowait(result)
        self._state.subscribe_workflow_results(job_id, stream)
        # The job being done ends the stream, behind the results before it.
        completion = asyncio.get_running_loop().create_task(self.wait_for_job(job_id, timeout=timeout))
        completion.add_done_callback(lambda _completion: stream.put_nowait(None))
        try:
            while (result := await stream.get()) is not None:
                yield result
            completion.result()
        finally:
            self._state.unsubscribe_workflow_results(job_id, stream)
            if completion.done() and not completion.cancelled():
                # A reader that stopped early: the wait's outcome is read
                # here rather than reported as never retrieved.
                completion.exception()
            if not completion.done():
                completion.cancel()
                cancels_requested_before_wait = asyncio.current_task().cancelling()
                try:
                    await completion
                except asyncio.CancelledError:
                    # The wait we cancelled ended; a cancel aimed at this
                    # task while it waited goes on.
                    if asyncio.current_task().cancelling() > cancels_requested_before_wait:
                        raise

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
        missed status pushes. A completed job then waits out its workflow
        results still in flight; the workflows left without one are named
        in ``missing_workflow_results``.

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

        # Held for the whole wait: a release of the job while it is waited
        # on cannot take its result away from this caller.
        job = self._state._jobs[job_id]
        event = self._state._job_events[job_id]
        effective_poll_interval = poll_interval or self.DEFAULT_POLL_INTERVAL_SECONDS

        async def poll_until_complete():
            while not event.is_set():
                await _DEFAULT_CLOCK.sleep(effective_poll_interval)
                if event.is_set():
                    break
                await self._poll_and_update_status(job_id)

        poll_task: asyncio.Task | None = None
        if self._poll_gate_for_status:
            # Phase 6b: explicit ``loop.create_task`` so the task binds to
            # the loop ``wait_for_job`` was called from rather than
            # implicitly going through ``get_running_loop`` at task-
            # creation time.
            poll_task = asyncio.get_running_loop().create_task(
                poll_until_complete()
            )

        try:
            if timeout:
                await _DEFAULT_CLOCK.wait_for(event.wait(), timeout=timeout)
            else:
                await event.wait()
        finally:
            if poll_task and not poll_task.done():
                poll_task.cancel()
                cancels_requested_before_wait = asyncio.current_task().cancelling()
                try:
                    await poll_task
                except asyncio.CancelledError:
                    # The task we cancelled ended; a cancel aimed at this task
                    # while it waited goes on.
                    if asyncio.current_task().cancelling() > cancels_requested_before_wait:
                        raise

        expected_workflow_ids = self._state._job_expected_workflows.get(job_id, frozenset())

        # Workflow results travel apart from the terminal status, with no
        # order between them, so a job can be reported complete before its
        # last results land: a completed job waits for those in flight
        # (every workflow of a completed job has one).
        results_event = self._state._job_results_events.get(job_id)
        if (
            job.status == JobStatus.COMPLETED.value
            and results_event is not None
            and not results_event.is_set()
            and not expected_workflow_ids <= job.workflow_results.keys()
        ):
            drained = False
            try:
                await _DEFAULT_CLOCK.wait_for(
                    results_event.wait(),
                    timeout=self._result_drain_timeout_seconds,
                )
                drained = True
            except asyncio.TimeoutError:
                # Not in flight, then: the gate recorded them for this
                # client and could not send them (the client was out of
                # reach a while). Re-registering this client's callback
                # has the gate send them again.
                if self._request_replay is not None and await self._request_replay(job_id):
                    try:
                        await _DEFAULT_CLOCK.wait_for(
                            results_event.wait(),
                            timeout=self._result_drain_timeout_seconds,
                        )
                        drained = True
                    except asyncio.TimeoutError:
                        drained = False
            if not drained:
                await self._logger.log(
                    ServerWarning(
                        message=(
                            f"Job {job_id[:8]}... completed, but not every workflow "
                            f"result arrived within {self._result_drain_timeout_seconds}s"
                        ),
                        node_host="client",
                        node_port=0,
                        node_id="tracker",
                    )
                )

        job.missing_workflow_results = sorted(expected_workflow_ids - job.workflow_results.keys())
        return job

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

            # Poll responses race pushes with no wire sequence — the
            # applier's order guard is what stops a stale response from
            # regressing a fresher (or terminal) status.
            poll_outcome = self._status_applier.apply_push(
                job,
                remote_status.status,
                remote_status.total_completed,
                remote_status.total_failed,
                getattr(remote_status, "overall_rate", job.overall_rate),
                getattr(
                    remote_status, "elapsed_seconds", job.elapsed_seconds
                ),
            )
            if poll_outcome.unknown_vocabulary:
                await self._logger.log(
                    ServerWarning(
                        message=(
                            f"Poll for job {job_id[:8]} returned status "
                            f"{remote_status.status!r} outside the "
                            "lifecycle vocabulary; not applied"
                        ),
                        node_host="client",
                        node_port=0,
                        node_id="client",
                    )
                )

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
