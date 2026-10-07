"""
Manager dispatch module: sending one workflow dispatch to its worker.

WorkflowDispatcher decides what to dispatch and to whom (allocation,
retry); this coordinator owns the send and what its outcome means for
the worker's routing state.
"""

from typing import TYPE_CHECKING, Callable

from hyperscale.distributed.jobs.dispatch_outcome import DispatchOutcome
from hyperscale.distributed.models import WorkflowDispatch, WorkflowDispatchAck
from hyperscale.distributed.runtime import Clock, SendTcp
from hyperscale.logging.hyperscale_logging_models import ServerError, ServerWarning

if TYPE_CHECKING:
    from hyperscale.distributed.jobs.worker_pool import WorkerPool
    from hyperscale.distributed.nodes.manager.registry import ManagerRegistry
    from hyperscale.distributed.nodes.manager.stats import ManagerStatsCoordinator
    from hyperscale.logging import Logger

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

# The outcomes a send can have (WITHHELD is the dispatcher's, never sent).
SENT_DISPATCH_OUTCOMES = (
    DispatchOutcome.ACCEPTED,
    DispatchOutcome.NOT_READY,
    DispatchOutcome.UNROUTABLE,
    DispatchOutcome.UNREACHABLE,
    DispatchOutcome.REJECTED,
)


class ManagerDispatchCoordinator:
    """Sends workflow dispatches to workers (the WorkflowDispatcher's
    ``send_dispatch``) and records each outcome on the worker pool without
    touching SWIM health: success, a readiness rejection (routing cools
    down), or a transport failure.

    Every dispatch's round trip to its worker's answer is the datacenter's
    AD-42 latency sample (dispatch -> response), and the worker's own (D-5).
    A dispatch the worker never answered counts at the timeout it waited,
    the least its latency was: dropping it would report a DC whose workers
    stop answering as fast. Each send's outcome is counted for the metrics
    surface (D-68)."""

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
        clock: Clock,
        record_dispatch_latency: Callable[[str, float, float], None],
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
        self._clock = clock
        self._record_dispatch_latency = record_dispatch_latency
        self._dispatch_outcome_counts: dict[DispatchOutcome, int] = dict.fromkeys(SENT_DISPATCH_OUTCOMES, 0)

    async def send_workflow_dispatch(
        self, worker_id: str, dispatch: WorkflowDispatch
    ) -> tuple[DispatchOutcome, str]:
        """Send ``dispatch`` to ``worker_id``: how the worker answered, and
        the answer's detail (the worker's error, or the transport's).

        A worker the registry does not know is a stale pool entry (the
        registry is the registration truth): it is purged so the
        dispatcher's retry cannot re-select it.
        """
        registration = self._registry.get_worker(worker_id)
        if registration is None:
            await self._purge_stale_worker(worker_id)
            self._dispatch_outcome_counts[DispatchOutcome.UNROUTABLE] += 1
            return DispatchOutcome.UNROUTABLE, f"worker {worker_id} is no longer registered"
        self._default_job_leader_addr(dispatch)
        dispatched_at = self._clock.monotonic()
        try:
            response, _clock = await self._send_tcp(
                (registration.node.host, registration.node.port),
                "workflow_dispatch",
                dispatch.dump(),
                timeout=self._dispatch_timeout_seconds,
            )
        except Exception as error:
            self._dispatch_outcome_counts[DispatchOutcome.UNREACHABLE] += 1
            return await self._fail_dispatch_unreachable(worker_id, error)

        answered_at = self._clock.monotonic()
        self._record_response_latency(worker_id, response, dispatched_at, answered_at)

        outcome, detail = await self._classify_dispatch_response(worker_id, response)
        self._dispatch_outcome_counts[outcome] += 1
        return outcome, detail

    def dispatch_outcome_counts(self) -> dict[str, int]:
        """Dispatch sends by outcome (``DispatchOutcome`` value) since this manager started."""
        return {outcome.value: count for outcome, count in self._dispatch_outcome_counts.items()}

    def _default_job_leader_addr(self, dispatch: WorkflowDispatch) -> None:
        """Name this manager as the job leader when the dispatch names none."""
        if dispatch.job_leader_addr is None:
            dispatch.job_leader_addr = (self._node_host, self._node_port)

    async def _fail_dispatch_unreachable(
        self,
        worker_id: str,
        error: Exception,
    ) -> tuple[DispatchOutcome, str]:
        """Record and log a transport failure (raised or returned); the worker is unreachable."""
        await self._record_transport_failure(worker_id, str(error))
        await self._logger.log(
            ServerError(
                message=f"Workflow dispatch error: {error}",
                node_host=self._node_host,
                node_port=self._node_port,
                node_id=self._node_id,
            )
        )
        return DispatchOutcome.UNREACHABLE, f"{type(error).__name__}: {error}"

    @staticmethod
    def _is_answered_dispatch(response: bytes | Exception | None) -> bool:
        """Whether the worker answered with a non-empty payload."""
        return isinstance(response, bytes) and response

    def _record_response_latency(
        self,
        worker_id: str,
        response: bytes | Exception | None,
        dispatched_at: float,
        answered_at: float,
    ) -> None:
        """Sample the round-trip for an answer, or the full timeout for a timed-out send."""
        if self._is_answered_dispatch(response):
            self._record_dispatch_latency(worker_id, (answered_at - dispatched_at) * 1000.0, answered_at)
        elif isinstance(response, TimeoutError):
            self._record_dispatch_latency(worker_id, self._dispatch_timeout_seconds * 1000.0, answered_at)

    async def _classify_dispatch_response(
        self,
        worker_id: str,
        response: bytes | Exception | None,
    ) -> tuple[DispatchOutcome, str]:
        """Turn the worker's reply into the dispatch outcome and its detail."""
        # send_tcp returns transport errors rather than raising: the same
        # failure, cause and log as one it raised.
        if isinstance(response, Exception):
            return await self._fail_dispatch_unreachable(worker_id, response)
        if not response:
            await self._record_transport_failure(worker_id, "workflow dispatch returned no response")
            return DispatchOutcome.UNREACHABLE, "workflow dispatch returned no response"
        return await self._record_answer(worker_id, WorkflowDispatchAck.load(response))

    async def _record_answer(
        self, worker_id: str, ack: WorkflowDispatchAck
    ) -> tuple[DispatchOutcome, str]:
        if bool(getattr(ack, "accepted", True)):
            await self._worker_pool.record_dispatch_taken(worker_id, ack.workflow_id, ack.cores_version)
            await self._record_success(worker_id)
            await self._stats.record_dispatch()
            return DispatchOutcome.ACCEPTED, ""
        error = self._dispatch_ack_error(ack)
        if self.is_readiness_rejection(error):
            await self._record_readiness_rejection(worker_id, error)
            return DispatchOutcome.NOT_READY, error
        await self._record_success(worker_id)
        return DispatchOutcome.REJECTED, error

    @staticmethod
    def _dispatch_ack_error(ack: WorkflowDispatchAck) -> str:
        """The worker's rejection reason, or a generic one when it gave none."""
        return getattr(ack, "error", None) or "workflow dispatch rejected"

    async def _record_readiness_rejection(self, worker_id: str, error: str) -> None:
        """Note a not-ready rejection; wake dispatch when the pool says cores may be free."""
        if self._worker_pool.record_dispatch_readiness_rejection(worker_id, error):
            await self._worker_pool.notify_cores_available()

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
