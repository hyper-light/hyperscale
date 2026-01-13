"""
Workflow dispatch TCP handler for worker.

Handles workflow dispatch requests from managers, allocates cores,
and starts workflow execution.
"""

from typing import TYPE_CHECKING

from hyperscale.distributed.models import (
    WorkflowDispatch,
    WorkflowDispatchAck,
    WorkerState,
)

if TYPE_CHECKING:
    from ..server import WorkerServer


class WorkflowDispatchHandler:
    """
    Handler for workflow dispatch requests from managers.

    Validates fence tokens, allocates cores, and starts workflow execution.
    Preserves AD-33 (Workflow State Machine) compliance.
    """

    def __init__(self, server: "WorkerServer") -> None:
        """
        Initialize handler with server reference.

        Args:
            server: WorkerServer instance for state access
        """
        self._server = server

    async def handle(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """
        Handle workflow dispatch request.

        Validates fence token, allocates cores, starts execution task.

        Args:
            addr: Source address (manager TCP address)
            data: Serialized WorkflowDispatch
            clock_time: Logical clock time

        Returns:
            Serialized WorkflowDispatchAck
        """
        dispatch: WorkflowDispatch | None = None
        allocation_succeeded = False

        try:
            dispatch = WorkflowDispatch.load(data)

            # Check backpressure first (fast path rejection)
            if self._server._get_worker_state() == WorkerState.DRAINING:
                return WorkflowDispatchAck(
                    workflow_id=dispatch.workflow_id,
                    accepted=False,
                    error="Worker is draining, not accepting new work",
                ).dump()

            # Check queue depth backpressure
            max_pending = self._server.env.MERCURY_SYNC_MAX_PENDING_WORKFLOWS
            current_pending = len(self._server._pending_workflows)
            if current_pending >= max_pending:
                return WorkflowDispatchAck(
                    workflow_id=dispatch.workflow_id,
                    accepted=False,
                    error=f"Queue depth limit reached: {current_pending}/{max_pending} pending",
                ).dump()

            token_accepted = (
                await self._server._worker_state.update_workflow_fence_token(
                    dispatch.workflow_id, dispatch.fence_token
                )
            )
            if not token_accepted:
                current = await self._server._worker_state.get_workflow_fence_token(
                    dispatch.workflow_id
                )
                return WorkflowDispatchAck(
                    workflow_id=dispatch.workflow_id,
                    accepted=False,
                    error=f"Stale fence token: {dispatch.fence_token} <= {current}",
                ).dump()

            # Atomic core allocation
            allocation_result = await self._server._core_allocator.allocate(
                dispatch.workflow_id,
                dispatch.cores,
            )

            if not allocation_result.success:
                return WorkflowDispatchAck(
                    workflow_id=dispatch.workflow_id,
                    accepted=False,
                    error=allocation_result.error
                    or f"Failed to allocate {dispatch.cores} cores",
                ).dump()

            allocation_succeeded = True

            # Delegate to server's dispatch execution logic
            return await self._server._handle_dispatch_execution(
                dispatch, addr, allocation_result
            )

        except Exception as exc:
            # Free any allocated cores if task didn't start successfully
            if dispatch and allocation_succeeded:
                await self._server._core_allocator.free(dispatch.workflow_id)
                self._server._cleanup_workflow_state(dispatch.workflow_id)

            workflow_id = dispatch.workflow_id if dispatch else "unknown"
            return WorkflowDispatchAck(
                workflow_id=workflow_id,
                accepted=False,
                error=str(exc),
            ).dump()
