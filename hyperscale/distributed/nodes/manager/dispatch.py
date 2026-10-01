"""
Manager dispatch module: sending one workflow dispatch to its worker.

WorkflowDispatcher decides what to dispatch and to whom (allocation,
retry); this coordinator owns the send and what its outcome means for
the worker's routing state.
"""

from typing import TYPE_CHECKING, Any, Awaitable, Callable

from hyperscale.distributed.models import WorkflowDispatch, WorkflowDispatchAck
from hyperscale.logging.hyperscale_logging_models import ServerError, ServerWarning

if TYPE_CHECKING:
    from hyperscale.distributed.jobs.worker_pool import WorkerPool
    from hyperscale.distributed.nodes.manager.registry import ManagerRegistry
    from hyperscale.distributed.nodes.manager.stats import ManagerStatsCoordinator
    from hyperscale.logging import Logger

SendTcp = Callable[..., Awaitable[tuple[Any, Any]]]

# Worker rejections meaning "not ready for more work right now": they cool
# the worker's routing down instead of counting as a delivered dispatch.
READINESS_REJECTION_MARKERS = (
    "draining",
    "not accepting",
    "queue depth",
    "pending",
    "capacity",
    "allocate",
    "cores",
)


class ManagerDispatchCoordinator:
    """Sends workflow dispatches to workers (the WorkflowDispatcher's
    ``send_dispatch``) and records each outcome on the worker pool without
    touching SWIM health: success, a readiness rejection (routing cools
    down), or a transport failure."""

    def __init__(
        self,
        registry: "ManagerRegistry",
        worker_pool: "WorkerPool",
        stats: "ManagerStatsCoordinator",
        send_tcp: SendTcp,
        logger: "Logger",
        node_host: str,
        node_port: int,
        node_id: str,
        dispatch_timeout_seconds: float,
    ) -> None:
        self._registry = registry
        self._worker_pool = worker_pool
        self._stats = stats
        self._send_tcp = send_tcp
        self._logger = logger
        self._node_host = node_host
        self._node_port = node_port
        self._node_id = node_id
        self._dispatch_timeout_seconds = dispatch_timeout_seconds

    async def send_workflow_dispatch(self, worker_id: str, dispatch: WorkflowDispatch) -> bool:
        """Send ``dispatch`` to ``worker_id``; True when the worker accepted.

        A worker the registry does not know is a stale pool entry (the
        registry is the registration truth): it is purged so the
        dispatcher's retry cannot re-select it.
        """
        registration = self._registry.get_worker(worker_id)
        if registration is None:
            await self._purge_stale_worker(worker_id)
            return False
        if dispatch.job_leader_addr is None:
            dispatch.job_leader_addr = (self._node_host, self._node_port)
        try:
            response, _clock = await self._send_tcp(
                (registration.node.host, registration.node.port),
                "workflow_dispatch",
                dispatch.dump(),
                timeout=self._dispatch_timeout_seconds,
            )
        except Exception as error:
            await self._record_transport_failure(worker_id, str(error))
            await self._logger.log(
                ServerError(
                    message=f"Workflow dispatch error: {error}",
                    node_host=self._node_host,
                    node_port=self._node_port,
                    node_id=self._node_id,
                )
            )
            return False

        if not response or isinstance(response, Exception):
            await self._record_transport_failure(worker_id, "workflow dispatch returned no response")
            return False
        return await self._record_answer(worker_id, WorkflowDispatchAck.load(response))

    async def _record_answer(self, worker_id: str, ack: WorkflowDispatchAck) -> bool:
        if bool(getattr(ack, "accepted", True)):
            await self._record_success(worker_id)
            await self._stats.record_dispatch()
            return True
        error = getattr(ack, "error", None)
        if self.is_readiness_rejection(error):
            if self._worker_pool.record_dispatch_readiness_rejection(worker_id, error or "workflow dispatch rejected"):
                await self._worker_pool.notify_cores_available()
        else:
            await self._record_success(worker_id)
        return False

    @staticmethod
    def is_readiness_rejection(error: str | None) -> bool:
        """Whether a dispatch rejection should cool down worker routing."""
        if not error:
            return False
        normalized_error = error.lower()
        return any(marker in normalized_error for marker in READINESS_REJECTION_MARKERS)

    async def _record_success(self, worker_id: str) -> None:
        if self._worker_pool.record_dispatch_success(worker_id):
            await self._worker_pool.notify_cores_available()

    async def _record_transport_failure(self, worker_id: str, error: str) -> None:
        if self._worker_pool.record_dispatch_transport_failure(worker_id, error):
            await self._worker_pool.notify_cores_available()

    async def _purge_stale_worker(self, worker_id: str) -> None:
        if await self._worker_pool.deregister_worker(worker_id):
            await self._worker_pool.notify_cores_available()
        await self._logger.log(
            ServerWarning(
                message=f"Workflow dispatch: unknown worker {worker_id[:8]}... — purged stale pool entry",
                node_host=self._node_host,
                node_port=self._node_port,
                node_id=self._node_id,
            )
        )
