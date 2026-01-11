"""
TCP handlers for gate state synchronization operations.

Handles state sync between gates:
- Gate state sync requests and responses
- Lease transfers for gate scaling
- Job final results from managers
- Job leadership notifications
"""

import asyncio
import time
from typing import TYPE_CHECKING, Callable

from hyperscale.distributed.models import (
    GateStateSnapshot,
    GateStateSyncRequest,
    GateStateSyncResponse,
    JobFinalResult,
    JobLeadershipNotification,
    LeaseTransfer,
    LeaseTransferAck,
)
from hyperscale.logging import Logger
from hyperscale.logging.hyperscale_logging_models import (
    ServerDebug,
    ServerError,
    ServerInfo,
    ServerWarning,
)

from ..state import GateRuntimeState

if TYPE_CHECKING:
    from hyperscale.distributed.swim.core import NodeId
    from hyperscale.distributed.tracking import JobLeadershipTracker, GateJobManager
    from hyperscale.distributed.versioning import VersionedClock
    from taskex import TaskRunner


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
        versioned_clock: "VersionedClock",
        get_node_id: Callable[[], "NodeId"],
        get_host: Callable[[], str],
        get_tcp_port: Callable[[], int],
        is_leader: Callable[[], bool],
        get_term: Callable[[], int],
        get_state_snapshot: Callable[[], GateStateSnapshot],
        apply_state_snapshot: Callable[[GateStateSnapshot], None],
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
            get_node_id: Callback to get this gate's node ID
            get_host: Callback to get this gate's host
            get_tcp_port: Callback to get this gate's TCP port
            is_leader: Callback to check if this gate is SWIM cluster leader
            get_term: Callback to get current leadership term
            get_state_snapshot: Callback to get full state snapshot
            apply_state_snapshot: Callback to apply state snapshot
        """
        self._state = state
        self._logger = logger
        self._task_runner = task_runner
        self._job_manager = job_manager
        self._job_leadership_tracker = job_leadership_tracker
        self._versioned_clock = versioned_clock
        self._get_node_id = get_node_id
        self._get_host = get_host
        self._get_tcp_port = get_tcp_port
        self._is_leader = is_leader
        self._get_term = get_term
        self._get_state_snapshot = get_state_snapshot
        self._apply_state_snapshot = apply_state_snapshot

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

            self._task_runner.run(
                self._logger.log,
                ServerInfo(
                    message=f"State sync request from gate {request.requester_id[:8]}... (version {request.known_version})",
                    node_host=self._get_host(),
                    node_port=self._get_tcp_port(),
                    node_id=self._get_node_id().short,
                )
            )

            snapshot = self._get_state_snapshot()

            response = GateStateSyncResponse(
                responder_id=self._get_node_id().full,
                is_leader=self._is_leader(),
                term=self._get_term(),
                state_version=self._state.get_state_version(),
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

    async def handle_state_sync_response(
        self,
        addr: tuple[str, int],
        data: bytes,
        handle_exception: Callable,
    ) -> bytes:
        """
        Handle gate state sync response from peer.

        Applies the received state snapshot if newer than local state.

        Args:
            addr: Peer gate address
            data: Serialized GateStateSyncResponse
            handle_exception: Callback for exception handling

        Returns:
            b'ok' on success, b'error' on failure
        """
        try:
            response = GateStateSyncResponse.load(data)

            if response.error:
                self._task_runner.run(
                    self._logger.log,
                    ServerWarning(
                        message=f"State sync response error from {response.responder_id[:8]}...: {response.error}",
                        node_host=self._get_host(),
                        node_port=self._get_tcp_port(),
                        node_id=self._get_node_id().short,
                    )
                )
                return b'error'

            if response.state_version <= self._state.get_state_version():
                self._task_runner.run(
                    self._logger.log,
                    ServerDebug(
                        message=f"Ignoring stale state sync from {response.responder_id[:8]}... "
                                f"(remote version {response.state_version} <= local {self._state.get_state_version()})",
                        node_host=self._get_host(),
                        node_port=self._get_tcp_port(),
                        node_id=self._get_node_id().short,
                    )
                )
                return b'ok'

            if response.snapshot:
                self._apply_state_snapshot(response.snapshot)

            self._task_runner.run(
                self._logger.log,
                ServerInfo(
                    message=f"Applied state sync from {response.responder_id[:8]}... (version {response.state_version})",
                    node_host=self._get_host(),
                    node_port=self._get_tcp_port(),
                    node_id=self._get_node_id().short,
                )
            )

            return b'ok'

        except Exception as error:
            await handle_exception(error, "handle_state_sync_response")
            return b'error'

    async def handle_lease_transfer(
        self,
        addr: tuple[str, int],
        data: bytes,
        handle_exception: Callable,
    ) -> bytes:
        """
        Handle lease transfer during gate scaling.

        When a gate is scaling down, it transfers job leases to peer gates.

        Args:
            addr: Source gate address
            data: Serialized LeaseTransfer
            handle_exception: Callback for exception handling

        Returns:
            Serialized LeaseTransferAck
        """
        try:
            transfer = LeaseTransfer.load(data)

            self._task_runner.run(
                self._logger.log,
                ServerInfo(
                    message=f"Receiving lease transfer from {transfer.source_gate_id[:8]}... "
                            f"for job {transfer.job_id[:8]}...",
                    node_host=self._get_host(),
                    node_port=self._get_tcp_port(),
                    node_id=self._get_node_id().short,
                )
            )

            if self._job_manager.has_job(transfer.job_id):
                return LeaseTransferAck(
                    job_id=transfer.job_id,
                    accepted=False,
                    error="Job already exists on this gate",
                    new_fence_token=0,
                ).dump()

            new_fence_token = transfer.fence_token + 1

            self._job_leadership_tracker.assume_leadership(
                job_id=transfer.job_id,
                metadata=transfer.metadata,
                fence_token=new_fence_token,
            )

            if transfer.job_status:
                self._job_manager.set_job(transfer.job_id, transfer.job_status)

            self._state.increment_state_version()

            self._task_runner.run(
                self._logger.log,
                ServerInfo(
                    message=f"Accepted lease transfer for job {transfer.job_id[:8]}... "
                            f"(new fence token: {new_fence_token})",
                    node_host=self._get_host(),
                    node_port=self._get_tcp_port(),
                    node_id=self._get_node_id().short,
                )
            )

            return LeaseTransferAck(
                job_id=transfer.job_id,
                accepted=True,
                new_fence_token=new_fence_token,
            ).dump()

        except Exception as error:
            await handle_exception(error, "handle_lease_transfer")
            return LeaseTransferAck(
                job_id="unknown",
                accepted=False,
                error=str(error),
                new_fence_token=0,
            ).dump()

    async def handle_job_final_result(
        self,
        addr: tuple[str, int],
        data: bytes,
        complete_job: Callable[[str, object], "asyncio.Task"],
        handle_exception: Callable,
    ) -> bytes:
        """
        Handle job final result from manager.

        Marks job as complete and pushes result to client callback if registered.

        Args:
            addr: Manager address
            data: Serialized JobFinalResult
            complete_job: Callback to complete the job
            handle_exception: Callback for exception handling

        Returns:
            b'ok' on success, b'error' on failure
        """
        try:
            result = JobFinalResult.load(data)

            self._task_runner.run(
                self._logger.log,
                ServerInfo(
                    message=f"Received final result for job {result.job_id[:8]}... "
                            f"(status={result.status}, from DC {result.datacenter})",
                    node_host=self._get_host(),
                    node_port=self._get_tcp_port(),
                    node_id=self._get_node_id().short,
                )
            )

            current_fence = self._job_manager.get_fence_token(result.job_id)
            if result.fence_token < current_fence:
                self._task_runner.run(
                    self._logger.log,
                    ServerDebug(
                        message=f"Rejecting stale final result for {result.job_id}: "
                                f"fence_token {result.fence_token} < {current_fence}",
                        node_host=self._get_host(),
                        node_port=self._get_tcp_port(),
                        node_id=self._get_node_id().short,
                    )
                )
                return b'ok'

            await complete_job(result.job_id, result)

            return b'ok'

        except Exception as error:
            await handle_exception(error, "handle_job_final_result")
            return b'error'

    async def handle_job_leadership_notification(
        self,
        addr: tuple[str, int],
        data: bytes,
        handle_exception: Callable,
    ) -> bytes:
        """
        Handle job leadership notification from peer gate.

        Updates local tracking of which gate owns which job.

        Args:
            addr: Source gate address
            data: Serialized JobLeadershipNotification
            handle_exception: Callback for exception handling

        Returns:
            b'ok' on success, b'error' on failure
        """
        try:
            notification = JobLeadershipNotification.load(data)

            my_id = self._get_node_id().full
            if notification.leader_gate_id == my_id:
                return b'ok'

            if self._versioned_clock.is_entity_stale(
                f"job-leader:{notification.job_id}",
                notification.fence_token,
            ):
                return b'ok'

            self._job_leadership_tracker.record_peer_leadership(
                job_id=notification.job_id,
                leader_id=notification.leader_gate_id,
                leader_addr=notification.leader_addr,
                fence_token=notification.fence_token,
            )

            self._task_runner.run(
                self._versioned_clock.update_entity,
                f"job-leader:{notification.job_id}",
                notification.fence_token,
            )

            self._task_runner.run(
                self._logger.log,
                ServerDebug(
                    message=f"Recorded job leadership: {notification.job_id[:8]}... -> "
                            f"{notification.leader_gate_id[:8]}... (fence {notification.fence_token})",
                    node_host=self._get_host(),
                    node_port=self._get_tcp_port(),
                    node_id=self._get_node_id().short,
                )
            )

            return b'ok'

        except Exception as error:
            await handle_exception(error, "handle_job_leadership_notification")
            return b'error'
