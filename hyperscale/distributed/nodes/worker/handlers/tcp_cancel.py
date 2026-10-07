"""
Workflow cancellation TCP handler for worker.

Handles workflow cancellation requests from managers (AD-20 compliance).
"""

from typing import TYPE_CHECKING

from hyperscale.distributed.models import (
    WorkflowCancelRequest,
    WorkflowCancelResponse,
    WorkflowProgress,
    WorkflowStatus,
)
from hyperscale.logging.hyperscale_logging_models import ServerError, ServerInfo

if TYPE_CHECKING:
    from hyperscale.distributed.nodes.worker.server import WorkerServer


class WorkflowCancelHandler:
    """
    Handler for workflow cancellation requests from managers.

    Cancels specific workflows while preserving AD-20 (Cancellation Propagation)
    protocol compliance.
    """

    def __init__(self, server: "WorkerServer") -> None:
        """
        Initialize handler with server reference.

        Args:
            server: WorkerServer instance for state access
        """
        self._server: "WorkerServer" = server

    async def handle(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """
        Handle workflow cancellation request.

        Cancels a specific workflow rather than all workflows for a job.

        Args:
            addr: Source address (manager TCP address)
            data: Serialized WorkflowCancelRequest
            clock_time: Logical clock time

        Returns:
            Serialized WorkflowCancelResponse
        """
        try:
            request = WorkflowCancelRequest.load(data)
            return await self._cancel_requested_workflow(request)

        except Exception as error:
            await self._server._udp_logger.log(
                ServerError(
                    message=f"Failed to cancel workflow: {error}",
                    node_host=self._server._host,
                    node_port=self._server._tcp_port,
                    node_id=self._server._node_id.short,
                )
            )
            return WorkflowCancelResponse(
                job_id="unknown",
                workflow_id="unknown",
                success=False,
                error=str(error),
            ).dump()

    async def _cancel_requested_workflow(self, request: WorkflowCancelRequest) -> bytes:
        """Answer an already-finished or mismatched workflow, else cancel it."""
        # Workflow not found - already completed/cancelled (walrus for single lookup)
        if not (
            progress := self._server._active_workflows.get(request.workflow_id)
        ):
            return self._build_already_completed_response(
                request.job_id, request.workflow_id
            )

        if (refusal := self._precancel_refusal(request, progress)) is not None:
            return refusal

        return await self._cancel_active_workflow(request, progress)

    def _precancel_refusal(
        self,
        request: WorkflowCancelRequest,
        progress: WorkflowProgress,
    ) -> bytes | None:
        """The response for a workflow of another job or already terminal, else None."""
        # Safety check: verify workflow belongs to specified job
        if progress.job_id != request.job_id:
            return WorkflowCancelResponse(
                job_id=request.job_id,
                workflow_id=request.workflow_id,
                success=False,
                error=f"Workflow {request.workflow_id} belongs to job {progress.job_id}, not {request.job_id}",
            ).dump()

        # Already in terminal state
        terminal_statuses = (
            WorkflowStatus.CANCELLED.value,
            WorkflowStatus.COMPLETED.value,
            WorkflowStatus.FAILED.value,
        )
        if progress.status in terminal_statuses:
            return self._build_already_completed_response(
                request.job_id, request.workflow_id
            )
        return None

    async def _cancel_active_workflow(
        self,
        request: WorkflowCancelRequest,
        progress: WorkflowProgress,
    ) -> bytes:
        """Cancel a non-terminal workflow and report whether it was running."""
        # Cancel the workflow
        was_running = progress.status == WorkflowStatus.RUNNING.value
        # The manager's own cause (job timeout, AD-41 kill, client cancel)
        # rides the request; the run's final result names it.
        cancelled, _ = await self._server._cancel_workflow(
            request.workflow_id, request.reason or "manager_cancel_request"
        )

        if cancelled:
            await self._server._udp_logger.log(
                ServerInfo(
                    message=f"Cancelled workflow {request.workflow_id} for job {request.job_id}",
                    node_host=self._server._host,
                    node_port=self._server._tcp_port,
                    node_id=self._server._node_id.short,
                )
            )

        return WorkflowCancelResponse(
            job_id=request.job_id,
            workflow_id=request.workflow_id,
            success=cancelled,
            was_running=was_running,
            already_completed=False,
        ).dump()

    def _build_already_completed_response(self, job_id: str, workflow_id: str) -> bytes:
        """Build response for already completed workflow."""
        return WorkflowCancelResponse(
            job_id=job_id,
            workflow_id=workflow_id,
            success=True,
            was_running=False,
            already_completed=True,
        ).dump()
