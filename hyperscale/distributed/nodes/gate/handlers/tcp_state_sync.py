"""
TCP handlers for gate state synchronization operations.

Handles state sync between gates:
- Gate state sync requests and responses
- Lease transfers for gate scaling
- Job final results from managers
- Job leadership notifications
"""

import asyncio
from typing import TYPE_CHECKING, Callable

from hyperscale.distributed.health import CircuitBreakerManager
from hyperscale.distributed.models import (
    GateStateSnapshot,
    GateStateSyncRequest,
    GateStateSyncResponse,
    JobFinalResult,
)
from hyperscale.logging import Logger
from hyperscale.logging.hyperscale_logging_models import (
    ServerDebug,
    ServerInfo,
    ServerWarning,
)

from hyperscale.distributed.nodes.gate.state import GateRuntimeState

if TYPE_CHECKING:
    from hyperscale.distributed.swim.core import NodeId
    from hyperscale.distributed.jobs import JobLeadershipTracker
    from hyperscale.distributed.jobs.gates import GateJobManager
    from hyperscale.distributed.server.events.lamport_clock import VersionedStateClock
    from hyperscale.distributed.taskex import TaskRunner


class GateStateSyncHandler:
    """
    Handles gate state synchronization operations.

    Provides TCP handler methods for state sync between gates during
    startup, scaling, and failover scenarios.
    """

    def __init__(
        self,
        state: GateRuntimeState,
        logger: Logger,
        task_runner: "TaskRunner",
        job_manager: "GateJobManager",
        job_leadership_tracker: "JobLeadershipTracker",
        versioned_clock: "VersionedStateClock",
        peer_circuit_breaker: CircuitBreakerManager,
        send_tcp: Callable,
        get_node_id: Callable[[], "NodeId"],
        get_host: Callable[[], str],
        get_tcp_port: Callable[[], int],
        is_leader: Callable[[], bool],
        get_term: Callable[[], int],
        get_state_snapshot: Callable[[], GateStateSnapshot],
        apply_state_snapshot: Callable[[GateStateSnapshot], None],
        peer_forward_timeout_seconds: float,
        get_known_leader_manager_term: Callable[[str], int],
    ) -> None:
        """
        Initialize the state sync handler.

        Args:
            state: Runtime state container
            logger: Async logger instance
            task_runner: Background task executor
            job_manager: Job management service
            job_leadership_tracker: Per-job leadership tracker
            versioned_clock: Version tracking for stale update rejection
            peer_circuit_breaker: Circuit breaker manager for peer gate calls
            send_tcp: Callback to send TCP messages
            get_node_id: Callback to get this gate's node ID
            get_host: Callback to get this gate's host
            get_tcp_port: Callback to get this gate's TCP port
            is_leader: Callback to check if this gate is SWIM cluster leader
            get_term: Callback to get current leadership term
            get_state_snapshot: Callback to get full state snapshot
            apply_state_snapshot: Callback to apply state snapshot
            get_known_leader_manager_term: Lookup the highest
                leader ``ManagerHeartbeat.term`` observed for a DC. Used in
                ``handle_job_final_result`` to validate the producing
                manager against per-DC manager leadership independently
                of any gate-leader fence; 0 where no term is known yet,
                which judges no result stale.
        """
        self._state: GateRuntimeState = state
        self._logger: Logger = logger
        self._task_runner: "TaskRunner" = task_runner
        self._job_manager: "GateJobManager" = job_manager
        self._job_leadership_tracker: "JobLeadershipTracker" = job_leadership_tracker
        self._versioned_clock: "VersionedStateClock" = versioned_clock
        self._peer_circuit_breaker: CircuitBreakerManager = peer_circuit_breaker
        self._send_tcp: Callable = send_tcp
        self._peer_forward_timeout_seconds: float = peer_forward_timeout_seconds
        self._get_node_id: Callable[[], "NodeId"] = get_node_id
        self._get_host: Callable[[], str] = get_host
        self._get_tcp_port: Callable[[], int] = get_tcp_port
        self._is_leader: Callable[[], bool] = is_leader
        self._get_term: Callable[[], int] = get_term
        self._get_state_snapshot: Callable[[], GateStateSnapshot] = get_state_snapshot
        self._apply_state_snapshot: Callable[[GateStateSnapshot], None] = (
            apply_state_snapshot
        )
        self._get_known_leader_manager_term: Callable[[str], int] = get_known_leader_manager_term

    async def handle_state_sync_request(
        self,
        addr: tuple[str, int],
        data: bytes,
        handle_exception: Callable,
    ) -> bytes:
        """
        Handle gate state sync request from peer.

        Returns full state snapshot for the requesting gate to apply.

        Args:
            addr: Peer gate address
            data: Serialized GateStateSyncRequest
            handle_exception: Callback for exception handling

        Returns:
            Serialized GateStateSyncResponse
        """
        try:
            request = GateStateSyncRequest.load(data)

            await self._logger.log(
                ServerInfo(
                    message=f"State sync request from gate {request.requester_id[:8]}... (version {request.known_version})",
                    node_host=self._get_host(),
                    node_port=self._get_tcp_port(),
                    node_id=self._get_node_id().short,
                ),
            )

            snapshot = self._get_state_snapshot()
            state_version = snapshot.version

            if request.known_version >= state_version:
                response = GateStateSyncResponse(
                    responder_id=self._get_node_id().full,
                    is_leader=self._is_leader(),
                    term=self._get_term(),
                    state_version=state_version,
                    snapshot=None,
                )
                return response.dump()

            response = GateStateSyncResponse(
                responder_id=self._get_node_id().full,
                is_leader=self._is_leader(),
                term=self._get_term(),
                state_version=state_version,
                snapshot=snapshot,
            )

            return response.dump()

        except Exception as error:
            await handle_exception(error, "handle_state_sync_request")
            return GateStateSyncResponse(
                responder_id=self._get_node_id().full,
                is_leader=self._is_leader(),
                term=self._get_term(),
                state_version=0,
                snapshot=None,
                error=str(error),
            ).dump()

    async def _forward_job_final_result_to_leader(
        self,
        job_id: str,
        leader_addr: tuple[str, int],
        data: bytes,
    ) -> bool:
        if await self._peer_circuit_breaker.is_circuit_open(leader_addr):
            await self._logger.log(
                ServerWarning(
                    message=(
                        f"Circuit open for leader gate {leader_addr}, "
                        f"cannot forward final result for {job_id[:8]}..."
                    ),
                    node_host=self._get_host(),
                    node_port=self._get_tcp_port(),
                    node_id=self._get_node_id().short,
                )
            )
            return False

        # One attempt: the manager that sent this result holds a
        # completion-notice obligation and resends until a gate accepts,
        # so a retry here would only multiply its attempts.
        circuit = await self._peer_circuit_breaker.get_circuit(leader_addr)
        response, _ = await self._send_tcp(
            leader_addr,
            "job_final_result_forwarded",
            data,
            timeout=self._peer_forward_timeout_seconds,
        )
        if response in (b"ok", b"already_completed"):
            circuit.record_success()
            return True

        circuit.record_failure()
        await self._logger.log(
            ServerWarning(
                message=(
                    f"Failed to forward final result for job {job_id[:8]}... "
                    f"to leader gate {leader_addr}: {response!r}"
                ),
                node_host=self._get_host(),
                node_port=self._get_tcp_port(),
                node_id=self._get_node_id().short,
            )
        )
        return False

    def _manager_term_to_judge(self, result: JobFinalResult) -> int:
        """The datacenter's highest known manager-leadership term, when
        ``result`` carries a manager term to judge against it; otherwise
        the result's own fence, which judges it current."""
        if result.producer_role != "manager" or result.manager_fence_token <= 0:
            return result.manager_fence_token
        return self._get_known_leader_manager_term(result.datacenter)

    async def _is_from_a_stale_producer(self, result: JobFinalResult) -> bool:
        """Whether a manager produced ``result`` under a manager-leadership
        term its datacenter has since moved past.

        ``result.fence_token`` is the manager-side fence at the moment the
        worker delivered the final result, not a gate-leadership claim.
        Comparing it to the gate's per-job fence (which orphan-takeover
        bumps) would drop completed work whenever a gate takeover landed
        before the manager learned the new fence. Gate-leader fencing
        protects gate-claim messages; manager-originated terminal data is
        validated at the manager-leadership layer here, before any
        forwarding or local completion side effects.
        """
        known_term = self._manager_term_to_judge(result)
        if result.manager_fence_token >= known_term:
            return False
        await self._logger.log(
            ServerDebug(
                message=(
                    f"Rejecting final result for {result.job_id}: "
                    f"producer {result.producer_id[:8]}... term "
                    f"{result.manager_fence_token} < DC manager "
                    f"term {known_term}"
                ),
                node_host=self._get_host(),
                node_port=self._get_tcp_port(),
                node_id=self._get_node_id().short,
            ),
        )
        return True

    async def handle_job_final_result(
        self,
        addr: tuple[str, int],
        data: bytes,
        complete_job: Callable[[str, object], "asyncio.Coroutine[None, None, bool]"],
        handle_exception: Callable,
        forward_final_result: Callable[[bytes], "asyncio.Coroutine[None, None, bool]"]
        | None = None,
    ) -> bytes:
        """Apply a datacenter's final result for a job, or route it to the
        gate that leads the job.

        ``forward_final_result`` is None for a result a peer gate forwarded
        (``job_final_result_forwarded``): that hop is the last, so a result
        no gate leads or knows can never circle between them.
        """
        try:
            result = JobFinalResult.load(data)

            await self._logger.log(
                ServerInfo(
                    message=f"Received final result for job {result.job_id[:8]}... "
                    f"(status={result.status}, from DC {result.datacenter})",
                    node_host=self._get_host(),
                    node_port=self._get_tcp_port(),
                    node_id=self._get_node_id().short,
                ),
            )

            if await self._is_from_a_stale_producer(result):
                return b"stale_producer"

            leader_id = self._job_leadership_tracker.get_leader(result.job_id)
            is_job_leader = self._job_leadership_tracker.is_leader(result.job_id)
            if leader_id and not is_job_leader:
                # A result a peer gate forwarded ends here: forwarding it on
                # could send it back round the gates that do not lead it.
                if forward_final_result is None:
                    return b"not_leader"
                leader_addr = self._job_leadership_tracker.get_leader_addr(
                    result.job_id
                )
                if leader_addr:
                    forwarded = await self._forward_job_final_result_to_leader(
                        result.job_id,
                        leader_addr,
                        data,
                    )
                    if forwarded:
                        return b"forwarded"
                    return b"error"

                await self._logger.log(
                    ServerWarning(
                        message=(
                            f"Leader gate {leader_id[:8]}... for job "
                            f"{result.job_id[:8]}... has no known address; "
                            "attempting peer forward."
                        ),
                        node_host=self._get_host(),
                        node_port=self._get_tcp_port(),
                        node_id=self._get_node_id().short,
                    )
                )
                if forward_final_result:
                    forwarded = await forward_final_result(data)
                    if forwarded:
                        return b"forwarded"
                    await self._logger.log(
                        ServerWarning(
                            message=(
                                "Failed to forward job final result for "
                                f"{result.job_id[:8]}... to peer gates"
                            ),
                            node_host=self._get_host(),
                            node_port=self._get_tcp_port(),
                            node_id=self._get_node_id().short,
                        )
                    )
                return b"error"

            job_exists = self._job_manager.get_job(result.job_id) is not None
            if not job_exists:
                if forward_final_result:
                    forwarded = await forward_final_result(data)
                    if forwarded:
                        return b"forwarded"
                    await self._logger.log(
                        ServerWarning(
                            message=(
                                "Failed to forward final result for unknown job "
                                f"{result.job_id[:8]}... to peer gates"
                            ),
                            node_host=self._get_host(),
                            node_port=self._get_tcp_port(),
                            node_id=self._get_node_id().short,
                        )
                    )
                return b"unknown_job"

            completed = await complete_job(result.job_id, result)
            if not completed:
                return b"already_completed"

            return b"ok"

        except Exception as error:
            await handle_exception(error, "handle_job_final_result")
            return b"error"

