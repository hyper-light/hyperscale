"""
Workflow throttle TCP handler for worker (AD-41 THROTTLE).

Applies a manager's throttle or release to a workflow running on this
worker's executors.
"""

from typing import TYPE_CHECKING

from hyperscale.distributed.models import WorkflowStatus
from hyperscale.distributed.resources.workflow_throttle_request import WorkflowThrottleRequest
from hyperscale.distributed.resources.workflow_throttle_response import WorkflowThrottleResponse
from hyperscale.logging.hyperscale_logging_models import ServerError

if TYPE_CHECKING:
    from hyperscale.core.jobs.graphs.remote_graph_manager import RemoteGraphManager
    from hyperscale.distributed.nodes.worker.server import WorkerServer


class WorkflowThrottleHandler:
    """Handler for workflow throttle requests from managers."""

    def __init__(self, server: "WorkerServer") -> None:
        self._server: "WorkerServer" = server
        self._remote_manager: "RemoteGraphManager | None" = None

    def set_remote_manager(self, remote_manager: "RemoteGraphManager") -> None:
        self._remote_manager = remote_manager

    async def handle(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Apply a ``WorkflowThrottleRequest``; a ``WorkflowThrottleResponse``.

        A failure is answered (and logged) rather than raised: the manager
        treats an unapplied throttle as no throttle and escalates on its
        own schedule.
        """
        try:
            request = WorkflowThrottleRequest.load(data)
            return (await self._apply(request)).dump()
        except Exception as error:
            await self._server._udp_logger.log(
                ServerError(
                    message=f"Failed to throttle workflow: {error!r}",
                    node_host=self._server._host,
                    node_port=self._server._tcp_port,
                    node_id=self._server._node_id.short,
                )
            )
            return WorkflowThrottleResponse(
                job_id="unknown", workflow_id="unknown", error=repr(error)
            ).dump()

    async def _apply(self, request: WorkflowThrottleRequest) -> WorkflowThrottleResponse:
        progress = self._server._active_workflows.get(request.workflow_id)
        workflow_name = self._server._worker_state._workflow_id_to_name.get(request.workflow_id)
        if (
            progress is None
            or progress.job_id != request.job_id
            or progress.status != WorkflowStatus.RUNNING.value
            or workflow_name is None
        ):
            return WorkflowThrottleResponse(
                job_id=request.job_id,
                workflow_id=request.workflow_id,
                error="workflow is not running on this worker",
            )
        if self._remote_manager is None:
            return WorkflowThrottleResponse(
                job_id=request.job_id,
                workflow_id=request.workflow_id,
                error="worker executors are not started",
            )

        # The run id the worker dispatched this workflow under (the same
        # mapping its executor and cancellation paths use).
        updates = await self._remote_manager.throttle_workflow(
            hash(request.workflow_id) % (2**31), workflow_name, request.scale
        )
        concurrency_caps = [update.concurrency_cap for update in updates if update.concurrency_cap is not None]
        return WorkflowThrottleResponse(
            job_id=request.job_id,
            workflow_id=request.workflow_id,
            applied=any(update.applied for update in updates),
            concurrency_cap=sum(concurrency_caps) if concurrency_caps else None,
        )
