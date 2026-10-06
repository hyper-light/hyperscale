"""
Worker workflow execution module.

Handles actual workflow execution, progress monitoring, and status transitions.
Extracted from worker_impl.py for modularity (AD-54 compliance).
"""

import asyncio
from typing import TYPE_CHECKING

import cloudpickle

from hyperscale.distributed.resources.workflow_resource_tracker import (
    BYTES_PER_MEGABYTE,
    WorkflowResourceTracker,
)
from hyperscale.core.jobs.models.workflow_status import (
    WorkflowStatus as CoreWorkflowStatus,
)
from hyperscale.core.jobs.models import Env as CoreEnv
from hyperscale.distributed.models import (
    StepStats,
    WorkflowDispatch,
    WorkflowDispatchAck,
    WorkflowFinalResult,
    WorkflowProgress,
    WorkflowStatus,
)
from hyperscale.logging.hyperscale_logging_models import (
    ServerError,
    WorkerJobReceived,
    WorkerJobStarted,
    WorkerJobCompleted,
    WorkerJobFailed,
)

from hyperscale.distributed.runtime import Clock, RealClock, RunTask
from collections.abc import Awaitable, Callable


_DEFAULT_CLOCK: Clock = RealClock()

if TYPE_CHECKING:
    from hyperscale.core.jobs.models.workflow_status_update import WorkflowStatusUpdate
    from hyperscale.logging import Logger
    from hyperscale.distributed.env import Env
    from hyperscale.reporting.common.results_types import WorkflowContextResult, WorkflowStats
    from hyperscale.distributed.jobs import CoreAllocator
    from .lifecycle import WorkerLifecycleManager
    from .state import WorkerState
    from .backpressure import WorkerBackpressureManager
    from hyperscale.distributed.taskex import TaskRunner
    from hyperscale.core.jobs.graphs.remote_graph_manager import RemoteGraphManager
    from hyperscale.core.state.context import Context


class WorkerWorkflowExecutor:
    """
    Executes workflows on the worker.

    Handles dispatch processing, actual execution via RemoteGraphManager,
    progress monitoring, and status transitions. Maintains AD-54 workflow
    state machine compliance.
    """

    # Worker progress status for each runner status a progress update maps.
    _UPDATE_STATUS_VALUES: dict[CoreWorkflowStatus, str] = {
        CoreWorkflowStatus.RUNNING: WorkflowStatus.RUNNING.value,
        CoreWorkflowStatus.COMPLETED: WorkflowStatus.COMPLETED.value,
        CoreWorkflowStatus.FAILED: WorkflowStatus.FAILED.value,
        CoreWorkflowStatus.PENDING: WorkflowStatus.ASSIGNED.value,
    }

    def __init__(
        self,
        core_allocator: "CoreAllocator",
        state: "WorkerState",
        lifecycle: "WorkerLifecycleManager",
        backpressure_manager: "WorkerBackpressureManager | None" = None,
        env: "Env | None" = None,
        logger: "Logger | None" = None,
        *,
        resource_tracker: "WorkflowResourceTracker",
        task_runner: "TaskRunner",
        execution_update_wait_seconds: float,
    ) -> None:
        """
        Initialize workflow executor.

        Args:
            core_allocator: CoreAllocator for core management
            state: WorkerState for workflow tracking
            lifecycle: WorkerLifecycleManager for monitor access
            backpressure_manager: Optional backpressure manager
            env: Environment configuration
            logger: Logger instance
        """
        self._core_allocator: "CoreAllocator" = core_allocator
        self._state: "WorkerState" = state
        self._lifecycle: "WorkerLifecycleManager" = lifecycle
        self._backpressure_manager: "WorkerBackpressureManager | None" = (
            backpressure_manager
        )
        self._env: "Env | None" = env
        self._logger: "Logger | None" = logger
        # AD-41 per-workflow resource estimates (Kalman over the executors'
        # measurements), released with each workflow.
        self._resource_tracker = resource_tracker
        self._task_runner = task_runner
        # Bounds how long a cancellation goes unnoticed while the next
        # status update of a running workflow is awaited.
        self._execution_update_wait_seconds = execution_update_wait_seconds

        # Event logger for crash forensics (AD-47)
        self._event_logger: Logger | None = None

        # Core environment for workflow runner (lazily initialized)
        self._core_env: CoreEnv | None = None

    def set_event_logger(self, logger: "Logger | None") -> None:
        """
        Set the event logger for crash forensics.

        Args:
            logger: Logger instance configured for event logging, or None to disable.
        """
        self._event_logger = logger

    def _get_core_env(self) -> CoreEnv:
        """Get or create CoreEnv for workflow execution."""
        if self._core_env is None and self._env:
            total_cores = self._core_allocator.total_cores
            self._core_env = CoreEnv(
                MERCURY_SYNC_AUTH_SECRET=self._env.MERCURY_SYNC_AUTH_SECRET,
                MERCURY_SYNC_AUTH_SECRET_PREVIOUS=self._env.MERCURY_SYNC_AUTH_SECRET_PREVIOUS,
                MERCURY_SYNC_LOGS_DIRECTORY=self._env.MERCURY_SYNC_LOGS_DIRECTORY,
                MERCURY_SYNC_LOG_LEVEL=self._env.MERCURY_SYNC_LOG_LEVEL,
                MERCURY_SYNC_MAX_CONCURRENCY=self._env.MERCURY_SYNC_MAX_CONCURRENCY,
                MERCURY_SYNC_TASK_RUNNER_MAX_THREADS=total_cores,
                MERCURY_SYNC_MAX_RUNNING_WORKFLOWS=total_cores,
                MERCURY_SYNC_MAX_PENDING_WORKFLOWS=100,
            )
        return self._core_env

    async def handle_dispatch_execution(
        self,
        dispatch: WorkflowDispatch,
        dispatching_addr: tuple[str, int],
        allocated_cores: list[int],
        cores_version: int,
        task_runner_run: RunTask,
        increment_version: Callable[[], Awaitable[int]],
        node_id_full: str,
        node_host: str,
        node_port: int,
        send_final_result_callback: Callable[[WorkflowFinalResult], Awaitable[None]],
    ) -> bytes:
        """
        Handle the execution phase of a workflow dispatch.

        Called after successful core allocation. Sets up workflow tracking,
        creates progress tracker, and starts execution task.

        Args:
            dispatch: WorkflowDispatch request
            dispatching_addr: Address of dispatching manager
            allocated_cores: List of allocated core indices
            task_runner_run: Function to run tasks via TaskRunner
            increment_version: Function to increment state version
            node_id_full: Full node identifier
            node_host: Worker host address
            node_port: Worker port
            send_final_result_callback: Callback to send final result to manager

        Returns:
            Serialized WorkflowDispatchAck
        """
        workflow_id = dispatch.workflow_id
        vus_for_workflow = dispatch.vus
        cores_to_allocate = dispatch.cores

        await self._log_job_received(dispatch, dispatching_addr, node_id_full, node_host, node_port)

        await increment_version()

        # Create initial progress tracker
        progress = WorkflowProgress(
            job_id=dispatch.job_id,
            workflow_id=workflow_id,
            workflow_name="",
            status=WorkflowStatus.RUNNING.value,
            completed_count=0,
            failed_count=0,
            rate_per_second=0.0,
            elapsed_seconds=0.0,
            timestamp=_DEFAULT_CLOCK.monotonic(),
            collected_at=_DEFAULT_CLOCK.time(),
            assigned_cores=allocated_cores,
            worker_available_cores=self._core_allocator.available_cores,
            worker_cores_version=self._core_allocator.availability_version,
            worker_workflow_completed_cores=0,
            worker_workflow_assigned_cores=cores_to_allocate,
        )

        self._state.add_active_workflow(workflow_id, progress, dispatching_addr)

        if dispatch.timeout_seconds > 0:
            self._state.set_workflow_timeout(workflow_id, dispatch.timeout_seconds)

        cancel_event = asyncio.Event()
        self._state._workflow_cancel_events[workflow_id] = cancel_event

        try:
            run = task_runner_run(
                self._execute_workflow,
                dispatch,
                progress,
                cancel_event,
                vus_for_workflow,
                len(allocated_cores),
                increment_version,
                node_id_full,
                node_host,
                node_port,
                send_final_result_callback,
                alias=f"workflow:{workflow_id}",
            )
        except Exception:
            await self._core_allocator.free(dispatch.workflow_id)
            raise

        # Store token for cancellation
        self._state._workflow_tokens[workflow_id] = run.token

        return WorkflowDispatchAck(
            workflow_id=workflow_id,
            accepted=True,
            cores_assigned=cores_to_allocate,
            cores_version=cores_version,
        ).dump()

    async def _log_job_received(
        self,
        dispatch: WorkflowDispatch,
        dispatching_addr: tuple[str, int],
        node_id_full: str,
        node_host: str,
        node_port: int,
    ) -> None:
        """Record a received dispatch in the event log (AD-47), when one is open."""
        if self._event_logger is not None:
            await self._event_logger.log(
                WorkerJobReceived(
                    message=f"Received job {dispatch.job_id}",
                    node_id=node_id_full,
                    node_host=node_host,
                    node_port=node_port,
                    job_id=dispatch.job_id,
                    workflow_id=dispatch.workflow_id,
                    source_manager_host=dispatching_addr[0],
                    source_manager_port=dispatching_addr[1],
                ),
                name="worker_events",
            )

    async def _execute_workflow(
        self,
        dispatch: WorkflowDispatch,
        progress: WorkflowProgress,
        cancel_event: asyncio.Event,
        allocated_vus: int,
        allocated_cores: int,
        increment_version: Callable[[], Awaitable[int]],
        node_id_full: str,
        node_host: str,
        node_port: int,
        send_final_result_callback: Callable[[WorkflowFinalResult], Awaitable[None]],
    ):
        """
        Execute a workflow using RemoteGraphManager.

        Args:
            dispatch: WorkflowDispatch request
            progress: Progress tracker
            cancel_event: Cancellation event
            allocated_vus: Number of VUs allocated
            allocated_cores: Number of cores allocated
            increment_version: Function to increment state version
            node_id_full: Full node identifier
        """
        start_time = _DEFAULT_CLOCK.monotonic()
        run_id = hash(dispatch.workflow_id) % (2**31)
        error: Exception | None = None
        workflow_error: str | None = None
        workflow_results: WorkflowStats | list[WorkflowStats | WorkflowContextResult] = {}
        context_updates: bytes = b""
        progress_monitor_token: str | None = None

        await self._log_job_started(dispatch, node_id_full, node_host, node_port, allocated_vus, allocated_cores)

        try:
            # Phase 1: Setup
            workflow, context_dict = await self._prepare_workflow_run(dispatch, progress, increment_version)

            # Phase 2: Execute
            remote_manager = self._require_remote_manager()

            # In-flight progress for the whole run: live stats, AD-26
            # extension evidence, AD-41 resource estimates. The monolithic
            # worker started this monitor here; the modular split kept
            # the method and dropped the call, so a running workflow
            # reported nothing until its final result. One alias for
            # every run keeps the TaskRunner's task map bounded.
            progress_monitor_token = self._start_progress_monitor(
                dispatch,
                progress,
                run_id,
                cancel_event,
                node_host,
                node_port,
                node_id_full,
            )

            (
                _,
                workflow_results,
                context,
                error,
                status,
            ) = await remote_manager.execute_workflow(
                run_id,
                workflow,
                context_dict,
                allocated_vus,
                max(allocated_cores, 1),
            )

            progress.cores_completed = len(progress.assigned_cores)

            # The run's final counts arrive with its completion (the runner
            # sends them before its results); the monitor may not have read
            # them before the run returned, and the final progress -- what
            # the job's totals are built from -- must carry them.
            await self._task_runner.cancel(progress_monitor_token)
            progress_monitor_token = None
            await self._apply_final_update(remote_manager, run_id, workflow.name, progress)

            # Phase 3: Determine final status
            workflow_error = self._settle_run_status(progress, status, error)

            context_updates = self._encode_context_updates(context)

        except asyncio.CancelledError:
            workflow_error = "Cancelled"
            progress.status = WorkflowStatus.CANCELLED.value

        except Exception as exc:
            workflow_error = self._error_text(exc)
            error = exc
            progress.status = WorkflowStatus.FAILED.value

        finally:
            # Workflow DURATION is deliberately NOT fed to the overload
            # detector. It was recorded here as a "latency" sample, and
            # the detector's absolute bounds (200/500/2000ms) classified
            # any workflow longer than 2 SECONDS as an overloaded
            # worker at drain — permanently, because a worker the
            # manager stops routing to produces no further samples to
            # de-escalate with (hysteresis needs consecutive better
            # readings). One completed long workflow therefore poisoned
            # its worker out of the pool forever: the manager's
            # allocation bucket put it in UNHEALTHY and every later
            # job's dispatch starved (measured live as the second-job
            # dispatch failure and dependent-workflow stalls; a load
            # generator runs minutes-long workflows BY DESIGN, so
            # duration is a category error as a latency signal). The
            # detector keeps its per-heartbeat CPU/memory resource
            # signal; its latency paths stay dormant until a genuine
            # per-operation latency source feeds them.

            await self._finish_workflow_run(progress_monitor_token, dispatch.workflow_id, increment_version)

        elapsed_seconds = _DEFAULT_CLOCK.monotonic() - start_time

        # AD-19: the worker's health throughput (completions per window)
        # and expected throughput (from mean completion time) are read off
        # these samples for every heartbeat; the manager's AD-26 throughput
        # witness judges extension requests on them.
        await self._state.record_completion(progress.status, elapsed_seconds)

        await self._log_job_outcome(
            dispatch,
            progress,
            node_id_full,
            node_host,
            node_port,
            elapsed_seconds,
            workflow_error,
            error,
        )

        final_result = self._build_final_result(
            dispatch,
            progress,
            workflow_results,
            context_updates,
            workflow_error,
            node_id_full,
        )

        await self._publish_final_result(dispatch, progress, final_result, send_final_result_callback)

    async def _log_job_started(
        self,
        dispatch: WorkflowDispatch,
        node_id_full: str,
        node_host: str,
        node_port: int,
        allocated_vus: int,
        allocated_cores: int,
    ) -> None:
        """Record a started job in the event log (AD-47), when one is open."""
        if self._event_logger is not None:
            await self._event_logger.log(
                WorkerJobStarted(
                    message=f"Started job {dispatch.job_id}",
                    node_id=node_id_full,
                    node_host=node_host,
                    node_port=node_port,
                    job_id=dispatch.job_id,
                    workflow_id=dispatch.workflow_id,
                    allocated_vus=allocated_vus,
                    allocated_cores=allocated_cores,
                ),
                name="worker_events",
            )

    async def _prepare_workflow_run(
        self,
        dispatch: WorkflowDispatch,
        progress: WorkflowProgress,
        increment_version: Callable[[], Awaitable[int]],
    ) -> tuple[object, dict]:
        """Load the dispatched workflow and context, register it, and mark its progress RUNNING."""
        workflow = dispatch.load_workflow()
        context_dict = dispatch.load_context()

        progress.workflow_name = workflow.name
        await increment_version()

        self._state._workflow_id_to_name[dispatch.workflow_id] = workflow.name
        self._state._workflow_cores_completed[dispatch.workflow_id] = set()

        # Transition to RUNNING
        progress.status = WorkflowStatus.RUNNING.value
        progress.timestamp = _DEFAULT_CLOCK.monotonic()
        progress.collected_at = _DEFAULT_CLOCK.time()
        return workflow, context_dict

    def _require_remote_manager(self) -> "RemoteGraphManager":
        """The lifecycle's RemoteGraphManager; RuntimeError when it is not available."""
        remote_manager = self._lifecycle.remote_manager
        if not remote_manager:
            raise RuntimeError("RemoteGraphManager not available")
        return remote_manager

    def _start_progress_monitor(
        self,
        dispatch: WorkflowDispatch,
        progress: WorkflowProgress,
        run_id: int,
        cancel_event: asyncio.Event,
        node_host: str,
        node_port: int,
        node_id_full: str,
    ) -> str:
        """Start the run's progress monitor under its shared alias; the token that cancels it."""
        progress_monitor_run = self._task_runner.run(
            self.monitor_workflow_progress,
            dispatch,
            progress,
            run_id,
            cancel_event,
            node_host,
            node_port,
            node_id_full,
            alias="workflow_progress_monitor",
        )
        return (
            f"{progress_monitor_run.task_name}:{progress_monitor_run.run_id}"
        )

    async def _apply_final_update(
        self,
        remote_manager: "RemoteGraphManager",
        run_id: int,
        workflow_name: str,
        progress: WorkflowProgress,
    ) -> None:
        """Copy the run's final counts into progress, when an update is still queued."""
        if (
            final_update := await remote_manager.drain_workflow_updates(
                run_id, workflow_name
            )
        ) is not None:
            self._apply_status_counts(progress, final_update)

    def _settle_run_status(
        self,
        progress: WorkflowProgress,
        status: CoreWorkflowStatus,
        error: Exception | None,
    ) -> str | None:
        """Mark progress COMPLETED or FAILED by the run's status; the failure's text, else None."""
        if status != CoreWorkflowStatus.COMPLETED:
            workflow_error = self._error_text(error)
            progress.status = WorkflowStatus.FAILED.value
            return workflow_error
        progress.status = WorkflowStatus.COMPLETED.value
        return None

    @staticmethod
    def _error_text(error: Exception | None) -> str:
        """An error's text; ``Unknown error`` when there is none."""
        return str(error) if error else "Unknown error"

    @staticmethod
    def _encode_context_updates(context: "Context | None") -> bytes:
        """The run's context updates, pickled (an empty dict when it has none)."""
        return cloudpickle.dumps(context.dict() if context else {})

    async def _finish_workflow_run(
        self,
        progress_monitor_token: str | None,
        workflow_id: str,
        increment_version: Callable[[], Awaitable[int]],
    ) -> None:
        """Stop a still-running progress monitor, free the cores and start server cleanup."""
        if progress_monitor_token is not None:
            await self._task_runner.cancel(progress_monitor_token)

        # Free cores
        await self._core_allocator.free(workflow_id)

        await increment_version()

        self._lifecycle.start_server_cleanup()

    async def _log_job_outcome(
        self,
        dispatch: WorkflowDispatch,
        progress: WorkflowProgress,
        node_id_full: str,
        node_host: str,
        node_port: int,
        elapsed_seconds: float,
        workflow_error: str | None,
        error: Exception | None,
    ) -> None:
        """Record the job's outcome in the event log (AD-47), when one is open."""
        if self._event_logger is not None:
            await self._log_job_outcome_event(
                dispatch,
                progress,
                node_id_full,
                node_host,
                node_port,
                elapsed_seconds,
                workflow_error,
                error,
            )

    async def _log_job_outcome_event(
        self,
        dispatch: WorkflowDispatch,
        progress: WorkflowProgress,
        node_id_full: str,
        node_host: str,
        node_port: int,
        elapsed_seconds: float,
        workflow_error: str | None,
        error: Exception | None,
    ) -> None:
        """Log a completed job, or a failed or cancelled one."""
        if progress.status == WorkflowStatus.COMPLETED.value:
            await self._event_logger.log(
                WorkerJobCompleted(
                    message=f"Completed job {dispatch.job_id}",
                    node_id=node_id_full,
                    node_host=node_host,
                    node_port=node_port,
                    job_id=dispatch.job_id,
                    workflow_id=dispatch.workflow_id,
                    elapsed_seconds=elapsed_seconds,
                    completed_count=progress.completed_count,
                    failed_count=progress.failed_count,
                ),
                name="worker_events",
            )
        elif progress.status in (
            WorkflowStatus.FAILED.value,
            WorkflowStatus.CANCELLED.value,
        ):
            await self._log_job_failed(
                dispatch,
                node_id_full,
                node_host,
                node_port,
                elapsed_seconds,
                workflow_error,
                error,
            )

    async def _log_job_failed(
        self,
        dispatch: WorkflowDispatch,
        node_id_full: str,
        node_host: str,
        node_port: int,
        elapsed_seconds: float,
        workflow_error: str | None,
        error: Exception | None,
    ) -> None:
        """Log a failed or cancelled job with its error."""
        await self._event_logger.log(
            WorkerJobFailed(
                message=f"Failed job {dispatch.job_id}",
                node_id=node_id_full,
                node_host=node_host,
                node_port=node_port,
                job_id=dispatch.job_id,
                workflow_id=dispatch.workflow_id,
                elapsed_seconds=elapsed_seconds,
                error_message=workflow_error,
                error_type=type(error).__name__ if error else None,
            ),
            name="worker_events",
        )

    def _build_final_result(
        self,
        dispatch: WorkflowDispatch,
        progress: WorkflowProgress,
        workflow_results: "WorkflowStats | list[WorkflowStats | WorkflowContextResult]",
        context_updates: bytes,
        workflow_error: str | None,
        node_id_full: str,
    ) -> WorkflowFinalResult:
        """The workflow's final result, carrying its final progress."""
        return WorkflowFinalResult(
            job_id=dispatch.job_id,
            workflow_id=dispatch.workflow_id,
            workflow_name=progress.workflow_name,
            status=progress.status,
            results=workflow_results if workflow_results else [],
            context_updates=context_updates if context_updates else b"",
            error=workflow_error,
            worker_id=node_id_full,
            worker_available_cores=self._core_allocator.available_cores,
            worker_cores_version=self._core_allocator.availability_version,
            fence_token=dispatch.fence_token,
            job_leader_addr=self._final_result_job_leader(dispatch),
            final_progress=progress,
        )

    def _final_result_job_leader(self, dispatch: WorkflowDispatch) -> tuple[str, int] | None:
        """The job leader named by the dispatch, else the one tracked for the workflow."""
        return dispatch.job_leader_addr or self._state.get_workflow_job_leader(dispatch.workflow_id)

    async def _publish_final_result(
        self,
        dispatch: WorkflowDispatch,
        progress: WorkflowProgress,
        final_result: WorkflowFinalResult,
        send_final_result_callback: Callable[[WorkflowFinalResult], Awaitable[None]],
    ) -> None:
        """Send the final result unless suppressed, then drop all of the workflow's local state."""
        try:
            if self._should_send_final_result(dispatch.workflow_id, progress.status):
                await send_final_result_callback(final_result)
        finally:
            self._resource_tracker.release(dispatch.workflow_id)
            self._state._workflow_fence_tokens.pop(dispatch.workflow_id, None)
            self._state._workflow_cancel_events.pop(dispatch.workflow_id, None)
            self._state._workflow_tokens.pop(dispatch.workflow_id, None)
            self._state._workflow_id_to_name.pop(dispatch.workflow_id, None)
            self._state._workflow_cores_completed.pop(dispatch.workflow_id, None)
            # Last: its termination callbacks' failures raise.
            self._state.remove_active_workflow(dispatch.workflow_id)

    def _should_send_final_result(self, workflow_id: str, status: str) -> bool:
        """Return whether this worker may publish the workflow's final result."""
        if status == WorkflowStatus.COMPLETED.value:
            return True

        return not self._state.is_final_result_suppressed(workflow_id)

    @staticmethod
    def _apply_status_counts(
        progress: WorkflowProgress,
        workflow_status_update: "WorkflowStatusUpdate",
    ) -> None:
        """Copy the runner's cumulative counts and per-step stats."""
        progress.completed_count = workflow_status_update.completed_count
        progress.failed_count = workflow_status_update.failed_count
        progress.step_stats = [
            StepStats(
                step_name=step_name,
                completed_count=stats.get("ok", 0),
                failed_count=stats.get("err", 0),
                total_count=stats.get("total", 0),
            )
            for step_name, stats in workflow_status_update.step_stats.items()
        ]

    async def monitor_workflow_progress(
        self,
        dispatch: WorkflowDispatch,
        progress: WorkflowProgress,
        run_id: int,
        cancel_event: asyncio.Event,
        node_host: str,
        node_port: int,
        node_id_short: str,
    ) -> None:
        """
        Monitor workflow progress and send updates.

        Uses event-driven waiting on update queue instead of polling.

        Args:
            dispatch: WorkflowDispatch request
            progress: Progress tracker
            run_id: Workflow run ID
            cancel_event: Cancellation event
            node_host: This worker's host
            node_port: This worker's port
            node_id_short: This worker's short node ID
        """
        start_time = _DEFAULT_CLOCK.monotonic()
        workflow_name = progress.workflow_name
        remote_manager = self._lifecycle.remote_manager

        if not remote_manager:
            return

        await self._watch_workflow_updates(
            remote_manager,
            dispatch,
            progress,
            run_id,
            cancel_event,
            workflow_name,
            start_time,
            node_host,
            node_port,
            node_id_short,
        )

    async def _watch_workflow_updates(
        self,
        remote_manager: "RemoteGraphManager",
        dispatch: WorkflowDispatch,
        progress: WorkflowProgress,
        run_id: int,
        cancel_event: asyncio.Event,
        workflow_name: str,
        start_time: float,
        node_host: str,
        node_port: int,
        node_id_short: str,
    ) -> None:
        """Apply the run's progress updates until cancelled."""
        while not cancel_event.is_set():
            if not await self._monitor_iteration(
                remote_manager,
                dispatch,
                progress,
                run_id,
                workflow_name,
                start_time,
                node_host,
                node_port,
                node_id_short,
            ):
                break

    async def _monitor_iteration(
        self,
        remote_manager: "RemoteGraphManager",
        dispatch: WorkflowDispatch,
        progress: WorkflowProgress,
        run_id: int,
        workflow_name: str,
        start_time: float,
        node_host: str,
        node_port: int,
        node_id_short: str,
    ) -> bool:
        """Apply the next progress update; False once cancelled, an error logged and the watch kept."""
        try:
            await self._process_next_workflow_update(
                remote_manager,
                dispatch,
                progress,
                run_id,
                workflow_name,
                start_time,
            )
            return True

        except asyncio.CancelledError:
            return False

        except Exception as err:
            await self._log_update_error(err, workflow_name, progress, node_host, node_port, node_id_short)
            return True

    async def _log_update_error(
        self,
        err: Exception,
        workflow_name: str,
        progress: WorkflowProgress,
        node_host: str,
        node_port: int,
        node_id_short: str,
    ) -> None:
        """Log a failed progress update, when a logger is set."""
        if self._logger:
            await self._logger.log(
                ServerError(
                    node_host=node_host,
                    node_port=node_port,
                    node_id=node_id_short,
                    message=f"Update Error: {str(err)} for workflow: {workflow_name} id: {progress.workflow_id}",
                )
            )

    async def _process_next_workflow_update(
        self,
        remote_manager: "RemoteGraphManager",
        dispatch: WorkflowDispatch,
        progress: WorkflowProgress,
        run_id: int,
        workflow_name: str,
        start_time: float,
    ) -> None:
        """Wait (bounded) for the run's next status update and apply it."""
        # Wait for update from remote manager
        workflow_status_update = await remote_manager.wait_for_workflow_update(
            run_id,
            workflow_name,
            timeout=self._execution_update_wait_seconds,
        )

        if workflow_status_update is None:
            return

        await self._apply_workflow_update(dispatch, progress, workflow_status_update, start_time)

    async def _apply_workflow_update(
        self,
        dispatch: WorkflowDispatch,
        progress: WorkflowProgress,
        workflow_status_update: "WorkflowStatusUpdate",
        start_time: float,
    ) -> None:
        """Fold a status update into progress (counts, resources, cores, status) and buffer it."""
        status = CoreWorkflowStatus(workflow_status_update.status)

        self._record_update_progress(progress, workflow_status_update, start_time)
        self._record_update_resources(dispatch, progress, workflow_status_update)
        self._record_update_availability(progress)

        total_cores = self._estimate_cores_completed(dispatch, progress, workflow_status_update)

        # Map status
        self._map_update_status(progress, status, total_cores)

        # Buffer progress for sending
        await self._state.buffer_progress_update(progress.workflow_id, progress)

    def _record_update_progress(
        self,
        progress: WorkflowProgress,
        workflow_status_update: "WorkflowStatusUpdate",
        start_time: float,
    ) -> None:
        """Copy an update's counts, rate, timestamps and per-executor resource averages into progress."""
        # Per-workflow resource use as measured by the executors
        # running it. (Read from the worker's own monitors, keyed
        # (run_id, workflow_name) while those sample under
        # (datacenter_id, node_id), it was always 0.)
        avg_cpu = workflow_status_update.avg_cpu_usage or 0.0
        avg_mem = workflow_status_update.avg_memory_usage_mb or 0.0

        # Update progress
        self._apply_status_counts(progress, workflow_status_update)
        progress.elapsed_seconds = _DEFAULT_CLOCK.monotonic() - start_time
        progress.rate_per_second = self._completion_rate(workflow_status_update, progress.elapsed_seconds)
        progress.timestamp = _DEFAULT_CLOCK.monotonic()
        progress.collected_at = _DEFAULT_CLOCK.time()
        progress.avg_cpu_percent = avg_cpu
        progress.avg_memory_mb = avg_mem

    @staticmethod
    def _completion_rate(workflow_status_update: "WorkflowStatusUpdate", elapsed_seconds: float) -> float:
        """Completions per second so far; 0.0 before any time has elapsed."""
        return (
            workflow_status_update.completed_count / elapsed_seconds
            if elapsed_seconds > 0
            else 0.0
        )

    def _record_update_resources(
        self,
        dispatch: WorkflowDispatch,
        progress: WorkflowProgress,
        workflow_status_update: "WorkflowStatusUpdate",
    ) -> None:
        """Feed an update's total resource use to the AD-41 estimator and record its estimate."""
        resource_estimate = self._resource_tracker.observe(
            dispatch.workflow_id,
            cpu_percent=workflow_status_update.total_cpu_usage or 0.0,
            memory_megabytes=workflow_status_update.total_memory_usage_mb or 0.0,
            process_count=max(len(progress.assigned_cores), 1),
        )
        progress.total_cpu_percent = resource_estimate.cpu_percent
        progress.total_cpu_uncertainty = resource_estimate.cpu_uncertainty
        progress.total_memory_mb = resource_estimate.memory_bytes / BYTES_PER_MEGABYTE
        progress.total_memory_uncertainty_mb = (
            resource_estimate.memory_uncertainty / BYTES_PER_MEGABYTE
        )

    def _record_update_availability(self, progress: WorkflowProgress) -> None:
        """Record the graph manager's latest core availability in progress."""
        # Get availability
        (
            workflow_assigned_cores,
            workflow_completed_cores,
            _worker_available_cores,
        ) = self._lifecycle.get_availability()

        # The workflow's cores are freed when its run returns, never
        # here: its executor nodes stay allocated until then, and the
        # availability read above is the graph manager's latest for
        # ANY run -- at a run's first update, often the previous
        # run's final, all cores free. Freed from it, a running
        # workflow's cores admitted the next dispatch onto busy
        # nodes, and that workflow failed "No nodes available".

        progress.worker_workflow_assigned_cores = workflow_assigned_cores
        progress.worker_workflow_completed_cores = workflow_completed_cores
        progress.worker_available_cores = self._core_allocator.available_cores
        progress.worker_cores_version = self._core_allocator.availability_version

    @staticmethod
    def _estimate_cores_completed(
        dispatch: WorkflowDispatch,
        progress: WorkflowProgress,
        workflow_status_update: "WorkflowStatusUpdate",
    ) -> int:
        """Estimate the cores done from completions against the VU work; the workflow's core count."""
        # Estimate cores_completed
        total_cores = len(progress.assigned_cores)
        if total_cores > 0:
            total_work = max(dispatch.vus * 100, 1)
            estimated_complete = min(
                total_cores,
                int(
                    total_cores
                    * (workflow_status_update.completed_count / total_work)
                ),
            )
            progress.cores_completed = estimated_complete
        return total_cores

    def _map_update_status(
        self,
        progress: WorkflowProgress,
        status: CoreWorkflowStatus,
        total_cores: int,
    ) -> None:
        """Map the runner status onto progress; a completed run has all its cores done."""
        if (mapped_status := self._UPDATE_STATUS_VALUES.get(status)) is not None:
            progress.status = mapped_status
        if status == CoreWorkflowStatus.COMPLETED:
            progress.cores_completed = total_cores
