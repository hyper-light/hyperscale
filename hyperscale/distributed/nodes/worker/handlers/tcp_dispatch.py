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
    from hyperscale.distributed.jobs.allocation_result import AllocationResult
    from hyperscale.distributed.nodes.worker.server import WorkerServer


class WorkflowDispatchHandler:
    """
    Handler for workflow dispatch requests from managers.

    Validates fence tokens, allocates cores, and starts workflow execution.
    Preserves AD-54 (Workflow State Machine) compliance.
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

            rejection, allocation_result = await self._admit_and_allocate(dispatch)
            if rejection is not None:
                return rejection

            allocation_succeeded = True

            # Delegate to server's dispatch execution logic
            return await self._server._handle_dispatch_execution(
                dispatch, addr, allocation_result
            )

        except Exception as exc:
            return await self._dispatch_failure_ack(dispatch, allocation_succeeded, exc)

    async def _dispatch_failure_ack(
        self,
        dispatch: WorkflowDispatch | None,
        allocation_succeeded: bool,
        exc: Exception,
    ) -> bytes:
        """Release a failed dispatch's allocation and answer with the error."""
        # Free any allocated cores if task didn't start successfully
        await self._release_failed_allocation(dispatch, allocation_succeeded)

        workflow_id = dispatch.workflow_id if dispatch else "unknown"
        return WorkflowDispatchAck(
            workflow_id=workflow_id,
            accepted=False,
            error=str(exc),
        ).dump()

    async def _admit_and_allocate(
        self,
        dispatch: WorkflowDispatch,
    ) -> tuple[bytes | None, "AllocationResult | None"]:
        """Admit the dispatch and allocate its cores; (rejection ack, None) on refusal."""
        if (rejection := await self._admission_rejection(dispatch)) is not None:
            return (rejection, None)

        # Atomic core allocation
        allocation_result = await self._server._core_allocator.allocate(
            dispatch.workflow_id,
            dispatch.cores,
        )

        if not allocation_result.success:
            return (self._allocation_failure_ack(dispatch, allocation_result), None)

        return (None, allocation_result)

    async def _admission_rejection(self, dispatch: WorkflowDispatch) -> bytes | None:
        """The rejection ack for backpressure or a stale fence token, else None."""
        if (rejection := self._backpressure_rejection(dispatch)) is not None:
            return rejection
        return await self._stale_fence_rejection(dispatch)

    def _backpressure_rejection(self, dispatch: WorkflowDispatch) -> bytes | None:
        """Reject while draining or at the pending-workflow queue limit."""
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
        return None

    async def _stale_fence_rejection(self, dispatch: WorkflowDispatch) -> bytes | None:
        """Reject a dispatch whose fence token is not newer than the recorded one."""
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
        return None

    @staticmethod
    def _allocation_failure_ack(
        dispatch: WorkflowDispatch,
        allocation_result: "AllocationResult",
    ) -> bytes:
        """The rejection ack for a failed core allocation."""
        return WorkflowDispatchAck(
            workflow_id=dispatch.workflow_id,
            accepted=False,
            error=allocation_result.error
            or f"Failed to allocate {dispatch.cores} cores",
        ).dump()

    async def _release_failed_allocation(
        self,
        dispatch: WorkflowDispatch | None,
        allocation_succeeded: bool,
    ) -> None:
        """Free the cores and state of a dispatch that failed after allocation."""
        if dispatch and allocation_succeeded:
            await self._server._core_allocator.free(dispatch.workflow_id)
            self._server._cleanup_workflow_state(dispatch.workflow_id)
