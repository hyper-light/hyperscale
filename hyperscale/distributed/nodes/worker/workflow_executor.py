"""
Worker workflow execution module.

Handles actual workflow execution, progress monitoring, and status transitions.
Extracted from worker_impl.py for modularity (AD-33 compliance).
"""

import asyncio
from typing import Any, TYPE_CHECKING

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

from hyperscale.distributed.runtime import Clock, RealClock


_DEFAULT_CLOCK: Clock = RealClock()

if TYPE_CHECKING:
    from hyperscale.core.jobs.models.workflow_status_update import WorkflowStatusUpdate
    from hyperscale.logging import Logger
    from hyperscale.distributed.env import Env
    from hyperscale.distributed.jobs import CoreAllocator
    from .lifecycle import WorkerLifecycleManager
    from .state import WorkerState
    from .backpressure import WorkerBackpressureManager
    from hyperscale.distributed.taskex import TaskRunner


class WorkerWorkflowExecutor:
    """
    Executes workflows on the worker.

    Handles dispatch processing, actual execution via RemoteGraphManager,
    progress monitoring, and status transitions. Maintains AD-33 workflow
    state machine compliance.
    """

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
        task_runner_run: callable,
        increment_version: callable,
        node_id_full: str,
        node_host: str,
        node_port: int,
        send_final_result_callback: callable,
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

        if self._event_logger is not None:
            await self._event_logger.log(
                WorkerJobReceived(
                    message=f"Received job {dispatch.job_id}",
                    node_id=node_id_full,
                    node_host=node_host,
                    node_port=node_port,
                    job_id=dispatch.job_id,
                    workflow_id=workflow_id,
                    source_manager_host=dispatching_addr[0],
                    source_manager_port=dispatching_addr[1],
                ),
                name="worker_events",
            )

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
        ).dump()

    async def _execute_workflow(
        self,
        dispatch: WorkflowDispatch,
        progress: WorkflowProgress,
        cancel_event: asyncio.Event,
        allocated_vus: int,
        allocated_cores: int,
        increment_version: callable,
        node_id_full: str,
        node_host: str,
        node_port: int,
        send_final_result_callback: callable,
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
        workflow_results: Any = {}
        context_updates: bytes = b""
        progress_monitor_token: str | None = None

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

        try:
            # Phase 1: Setup
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

            # Phase 2: Execute
            remote_manager = self._lifecycle.remote_manager
            if not remote_manager:
                raise RuntimeError("RemoteGraphManager not available")

            # In-flight progress for the whole run: live stats, AD-26
            # extension evidence, AD-41 resource estimates. The monolithic
            # worker started this monitor here; the modular split kept
            # the method and dropped the call, so a running workflow
            # reported nothing until its final result. One alias for
            # every run keeps the TaskRunner's task map bounded.
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
            progress_monitor_token = (
                f"{progress_monitor_run.task_name}:{progress_monitor_run.run_id}"
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
            if (
                final_update := await remote_manager.drain_workflow_updates(
                    run_id, workflow.name
                )
            ) is not None:
                self._apply_status_counts(progress, final_update)

            # Phase 3: Determine final status
            if status != CoreWorkflowStatus.COMPLETED:
                workflow_error = str(error) if error else "Unknown error"
                progress.status = WorkflowStatus.FAILED.value
            else:
                progress.status = WorkflowStatus.COMPLETED.value

            context_updates = cloudpickle.dumps(context.dict() if context else {})

        except asyncio.CancelledError:
            workflow_error = "Cancelled"
            progress.status = WorkflowStatus.CANCELLED.value

        except Exception as exc:
            workflow_error = str(exc) if exc else "Unknown error"
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

            if progress_monitor_token is not None:
                await self._task_runner.cancel(progress_monitor_token)

            # Free cores
            await self._core_allocator.free(dispatch.workflow_id)

            await increment_version()

            self._lifecycle.start_server_cleanup()

        elapsed_seconds = _DEFAULT_CLOCK.monotonic() - start_time

        if self._event_logger is not None:
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

        final_result = WorkflowFinalResult(
            job_id=dispatch.job_id,
            workflow_id=dispatch.workflow_id,
            workflow_name=progress.workflow_name,
            status=progress.status,
            results=workflow_results if workflow_results else [],
            context_updates=context_updates if context_updates else b"",
            error=workflow_error,
            worker_id=node_id_full,
            worker_available_cores=self._core_allocator.available_cores,
            fence_token=dispatch.fence_token,
            job_leader_addr=dispatch.job_leader_addr
            or self._state.get_workflow_job_leader(dispatch.workflow_id),
            final_progress=progress,
        )

        try:
            if self._should_send_final_result(dispatch.workflow_id, progress.status):
                await send_final_result_callback(final_result)
        finally:
            self._resource_tracker.release(dispatch.workflow_id)
            self._state.remove_active_workflow(dispatch.workflow_id)
            self._state._workflow_fence_tokens.pop(dispatch.workflow_id, None)
            self._state._workflow_cancel_events.pop(dispatch.workflow_id, None)
            self._state._workflow_tokens.pop(dispatch.workflow_id, None)
            self._state._workflow_id_to_name.pop(dispatch.workflow_id, None)
            self._state._workflow_cores_completed.pop(dispatch.workflow_id, None)

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

        while not cancel_event.is_set():
            try:
                # Wait for update from remote manager
                workflow_status_update = await remote_manager.wait_for_workflow_update(
                    run_id,
                    workflow_name,
                    timeout=0.5,
                )

                if workflow_status_update is None:
                    continue

                status = CoreWorkflowStatus(workflow_status_update.status)

                # Per-workflow resource use as measured by the executors
                # running it. (Read from the worker's own monitors, keyed
                # (run_id, workflow_name) while those sample under
                # (datacenter_id, node_id), it was always 0.)
                avg_cpu = workflow_status_update.avg_cpu_usage or 0.0
                avg_mem = workflow_status_update.avg_memory_usage_mb or 0.0

                # Update progress
                self._apply_status_counts(progress, workflow_status_update)
                progress.elapsed_seconds = _DEFAULT_CLOCK.monotonic() - start_time
                progress.rate_per_second = (
                    workflow_status_update.completed_count / progress.elapsed_seconds
                    if progress.elapsed_seconds > 0
                    else 0.0
                )
                progress.timestamp = _DEFAULT_CLOCK.monotonic()
                progress.collected_at = _DEFAULT_CLOCK.time()
                progress.avg_cpu_percent = avg_cpu
                progress.avg_memory_mb = avg_mem
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

                # Get availability
                (
                    workflow_assigned_cores,
                    workflow_completed_cores,
                    worker_available_cores,
                ) = self._lifecycle.get_availability()

                if worker_available_cores > 0:
                    await self._core_allocator.free_subset(
                        progress.workflow_id,
                        worker_available_cores,
                    )

                progress.worker_workflow_assigned_cores = workflow_assigned_cores
                progress.worker_workflow_completed_cores = workflow_completed_cores
                progress.worker_available_cores = self._core_allocator.available_cores

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

                # Map status
                if status == CoreWorkflowStatus.RUNNING:
                    progress.status = WorkflowStatus.RUNNING.value
                elif status == CoreWorkflowStatus.COMPLETED:
                    progress.status = WorkflowStatus.COMPLETED.value
                    progress.cores_completed = total_cores
                elif status == CoreWorkflowStatus.FAILED:
                    progress.status = WorkflowStatus.FAILED.value
                elif status == CoreWorkflowStatus.PENDING:
                    progress.status = WorkflowStatus.ASSIGNED.value

                # Buffer progress for sending
                await self._state.buffer_progress_update(progress.workflow_id, progress)

            except asyncio.CancelledError:
                break

            except Exception as err:
                if self._logger:
                    await self._logger.log(
                        ServerError(
                            node_host=node_host,
                            node_port=node_port,
                            node_id=node_id_short,
                            message=f"Update Error: {str(err)} for workflow: {workflow_name} id: {progress.workflow_id}",
                        )
                    )
