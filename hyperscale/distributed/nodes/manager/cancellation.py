"""
Manager cancellation module for workflow cancellation propagation.

Handles AD-20 compliant job and workflow cancellation coordination.
"""

import asyncio
import time
from typing import Any, Callable, Coroutine, TYPE_CHECKING

from hyperscale.distributed.models import (
    JobCancelRequest,
    JobCancelResponse,
    WorkflowCancelRequest,
    WorkflowCancelResponse,
    WorkflowCancellationComplete,
    JobCancellationComplete,
    CancelledWorkflowInfo,
)
from hyperscale.distributed.models.jobs import TrackingToken
from hyperscale.logging.hyperscale_logging_models import ServerInfo, ServerWarning

if TYPE_CHECKING:
    from hyperscale.distributed.jobs.job_manager import JobManager
    from hyperscale.distributed.nodes.manager.state import ManagerState
    from hyperscale.distributed.nodes.manager.config import ManagerConfig
    from hyperscale.distributed.taskex import TaskRunner
    from hyperscale.logging import Logger

# Type alias for send functions
SendFunc = Callable[..., Coroutine[Any, Any, tuple[bytes, float] | None]]


class ManagerCancellationCoordinator:
    """
    Coordinates job and workflow cancellation (AD-20).

    Handles:
    - Job cancellation requests from clients/gates
    - Workflow cancellation propagation to workers
    - Cancellation completion tracking
    - Client notification when all workflows cancelled
    """

    def __init__(
        self,
        state: "ManagerState",
        config: "ManagerConfig",
        logger: "Logger",
        node_id: str,
        task_runner: "TaskRunner",
        send_to_worker: SendFunc,
        send_to_client: SendFunc,
        job_manager: "JobManager | None" = None,
    ) -> None:
        self._state: "ManagerState" = state
        self._config: "ManagerConfig" = config
        self._logger: "Logger" = logger
        self._node_id: str = node_id
        self._task_runner: "TaskRunner" = task_runner
        self._send_to_worker: SendFunc = send_to_worker
        self._send_to_client: SendFunc = send_to_client
        self._job_manager: "JobManager | None" = job_manager

    async def cancel_job(
        self,
        request: JobCancelRequest,
        source_addr: tuple[str, int],
    ) -> bytes:
        """
        Cancel all workflows in a job.

        Args:
            request: Job cancellation request
            source_addr: Source address for response

        Returns:
            Serialized JobCancelResponse
        """
        job_id = request.job_id

        # Check if job exists
        if job_id not in self._state._job_submissions:
            return JobCancelResponse(
                job_id=job_id,
                success=False,
                error="Job not found",
            ).dump()

        # Initialize cancellation tracking
        self._state._cancellation_initiated_at[job_id] = time.monotonic()
        self._state._cancellation_completion_events[job_id] = asyncio.Event()

        # Get workflows to cancel
        # Note: In the full implementation, this would get workflows from JobManager
        workflow_ids = self._get_job_workflow_ids(job_id)

        if not workflow_ids:
            return JobCancelResponse(
                job_id=job_id,
                success=True,
                cancelled_workflow_count=0,
            ).dump()

        # Track pending cancellations
        self._state._cancellation_pending_workflows[job_id] = set(workflow_ids)

        # Send cancellation to workers
        cancel_count = 0
        for workflow_id in workflow_ids:
            await self._cancel_workflow(job_id, workflow_id, request.reason)
            cancel_count += 1

        self._task_runner.run(
            self._logger.log,
            ServerInfo(
                message=f"Job {job_id[:8]}... cancellation initiated for {cancel_count} workflows",
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            ),
        )

        return JobCancelResponse(
            job_id=job_id,
            success=True,
            cancelled_workflow_count=cancel_count,
        ).dump()

    async def _cancel_workflow(
        self,
        job_id: str,
        workflow_id: str,
        reason: str,
    ) -> None:
        """
        Cancel a single workflow by sending request to its worker.

        Args:
            job_id: Job ID
            workflow_id: Workflow ID to cancel
            reason: Cancellation reason
        """
        # Mark workflow as cancelled in tracking
        if workflow_id not in self._state._cancelled_workflows:
            self._state._cancelled_workflows[workflow_id] = CancelledWorkflowInfo(
                workflow_id=workflow_id,
                job_id=job_id,
                cancelled_at=time.time(),
                request_id=reason,
                dependents=[],
            )

        try:
            workflow_token = TrackingToken.parse(workflow_id)
        except ValueError as error:
            await self._record_workflow_cancellation_failure(
                job_id,
                workflow_id,
                f"Invalid workflow token: {error}",
            )
            return

        if not workflow_token.worker_id:
            await self._record_workflow_cancellation_failure(
                job_id,
                workflow_id,
                "Workflow token missing worker id for cancellation",
            )
            return

        worker = self._state.get_worker(workflow_token.worker_id)
        if not worker:
            await self._record_workflow_cancellation_failure(
                job_id,
                workflow_id,
                f"Worker {workflow_token.worker_id} not found for workflow cancellation",
            )
            return

        cancel_request = WorkflowCancelRequest(
            job_id=job_id,
            workflow_id=workflow_id,
            requester_id=self._node_id,
            timestamp=time.time(),
            reason=reason,
        )

        response = await self._send_to_worker(
            (worker.node.host, worker.node.port),
            "cancel_workflow",
            cancel_request.dump(),
            timeout=self._config.tcp_timeout_standard_seconds,
        )

        if not isinstance(response, bytes):
            if isinstance(response, Exception):
                error_message = (
                    f"Failed to send cancellation to worker {workflow_token.worker_id}:"
                    f" {response}"
                )
            else:
                error_message = (
                    f"No response from worker {workflow_token.worker_id} for workflow"
                    f" {workflow_id}"
                )
            await self._record_workflow_cancellation_failure(
                job_id,
                workflow_id,
                error_message,
            )
            return

        try:
            cancel_response = WorkflowCancelResponse.load(response)
        except Exception as error:
            await self._record_workflow_cancellation_failure(
                job_id,
                workflow_id,
                f"Failed to parse cancellation response: {error}",
            )
            return

        if cancel_response.success:
            if cancel_response.already_completed:
                await self._finalize_workflow_cancellation(
                    job_id,
                    workflow_id,
                    success=True,
                    errors=[],
                )
            return

        error_message = cancel_response.error or "Worker reported cancellation failure"
        await self._record_workflow_cancellation_failure(
            job_id,
            workflow_id,
            error_message,
        )

    async def _finalize_workflow_cancellation(
        self,
        job_id: str,
        workflow_id: str,
        success: bool,
        errors: list[str],
    ) -> None:
        notification = WorkflowCancellationComplete(
            job_id=job_id,
            workflow_id=workflow_id,
            success=success,
            errors=errors,
            cancelled_at=time.monotonic(),
            node_id=self._node_id,
        )
        await self.handle_workflow_cancelled(notification)

    async def _record_workflow_cancellation_failure(
        self,
        job_id: str,
        workflow_id: str,
        error_message: str,
    ) -> None:
        self._task_runner.run(
            self._logger.log,
            ServerWarning(
                message=(
                    f"Workflow {workflow_id[:8]}... cancellation failed:"
                    f" {error_message}"
                ),
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            ),
        )
        await self._finalize_workflow_cancellation(
            job_id,
            workflow_id,
            success=False,
            errors=[error_message],
        )

    async def handle_workflow_cancelled(
        self,
        notification: WorkflowCancellationComplete,
    ) -> None:
        """
        Handle workflow cancellation completion from worker.

        Updates tracking and notifies client when all workflows done.

        Args:
            notification: Cancellation completion notification
        """
        job_id = notification.job_id
        workflow_id = notification.workflow_id

        # Remove from pending set
        pending = self._state._cancellation_pending_workflows.get(job_id, set())
        pending.discard(workflow_id)

        # Track any errors
        if notification.errors:
            self._state._cancellation_errors[job_id].extend(notification.errors)

        self._task_runner.run(
            self._logger.log,
            ServerInfo(
                message=f"Workflow {workflow_id[:8]}... cancellation complete for job {job_id[:8]}..., {len(pending)} remaining",
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            ),
        )

        # Check if all workflows are cancelled
        if not pending:
            await self._notify_job_cancelled(job_id)

    async def _notify_job_cancelled(self, job_id: str) -> None:
        """
        Notify client that job cancellation is complete.

        Args:
            job_id: Job ID that completed cancellation
        """
        # Signal completion event
        event = self._state._cancellation_completion_events.get(job_id)
        if event:
            event.set()

        # Get client callback if registered
        callback_addr = self._state._job_callbacks.get(job_id)
        if not callback_addr:
            callback_addr = self._state._client_callbacks.get(job_id)

        if callback_addr:
            errors = self._state._cancellation_errors.get(job_id, [])
            notification = JobCancellationComplete(
                job_id=job_id,
                success=len(errors) == 0,
                errors=errors,
            )

            try:
                await self._send_to_client(
                    callback_addr,
                    "job_cancellation_complete",
                    notification.dump(),
                )
            except Exception as e:
                self._task_runner.run(
                    self._logger.log,
                    ServerWarning(
                        message=f"Failed to notify client of job {job_id[:8]}... cancellation: {e}",
                        node_host=self._config.host,
                        node_port=self._config.tcp_port,
                        node_id=self._node_id,
                    ),
                )

        # Cleanup tracking
        self._state.clear_cancellation_state(job_id)

    def _get_job_workflow_ids(self, job_id: str) -> list[str]:
        """
        Get sub-workflow IDs for a job from the JobManager.

        Args:
            job_id: Job ID

        Returns:
            List of sub-workflow token strings
        """
        if not self._job_manager:
            return []
        if not (job := self._job_manager.get_job_by_id(job_id)):
            return []
        return list(job.sub_workflows.keys())

    def is_workflow_cancelled(self, workflow_id: str) -> bool:
        """
        Check if a workflow has been cancelled.

        Args:
            workflow_id: Workflow ID to check

        Returns:
            True if workflow is cancelled
        """
        return workflow_id in self._state._cancelled_workflows

    def cleanup_old_cancellations(self, max_age_seconds: float) -> int:
        """
        Cleanup old cancelled workflow records.

        Args:
            max_age_seconds: Maximum age for cancelled workflow records

        Returns:
            Number of records cleaned up
        """
        now = time.time()
        to_remove = [
            workflow_id
            for workflow_id, info in self._state._cancelled_workflows.items()
            if (now - info.cancelled_at) > max_age_seconds
        ]

        for workflow_id in to_remove:
            self._state._cancelled_workflows.pop(workflow_id, None)
            self._state._workflow_cancellation_locks.pop(workflow_id, None)

        return len(to_remove)
