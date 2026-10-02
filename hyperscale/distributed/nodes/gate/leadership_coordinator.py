"""
Gate leadership coordination module.

Coordinates job leadership, lease management, and peer gate coordination.
"""

from typing import TYPE_CHECKING, Callable

from hyperscale.distributed.models import (
    GateJobLeaderTransfer,
    JobLeadershipAnnouncement,
)
from hyperscale.logging.hyperscale_logging_models import (
    ServerDebug,
    ServerWarning,
)

if TYPE_CHECKING:
    from hyperscale.distributed.nodes.gate.state import GateRuntimeState
    from hyperscale.distributed.jobs import JobLeadershipTracker
    from hyperscale.logging import Logger
    from hyperscale.distributed.taskex import TaskRunner


class GateLeadershipCoordinator:
    """
    Announces this gate's job leadership to peer gates and the client,
    and answers the gate cluster's quorum questions.

    Receiving announcements and gate-to-gate transfers is the server's
    job_leadership_announcement endpoint and GateJobHandler; orphan
    tracking is GateOrphanJobCoordinator's.
    """

    def __init__(
        self,
        state: "GateRuntimeState",
        logger: "Logger",
        task_runner: "TaskRunner",
        leadership_tracker: "JobLeadershipTracker",
        get_node_id: Callable,
        get_node_addr: Callable,
        send_tcp: Callable,
        get_active_peers: Callable,
        get_cluster_size: Callable[[], int],
        peer_rpc_timeout_seconds: float,
    ) -> None:
        self._state: "GateRuntimeState" = state
        self._logger: "Logger" = logger
        self._task_runner: "TaskRunner" = task_runner
        self._leadership_tracker: "JobLeadershipTracker" = leadership_tracker
        self._get_node_id: Callable = get_node_id
        self._get_node_addr: Callable = get_node_addr
        self._send_tcp: Callable = send_tcp
        self._get_active_peers: Callable = get_active_peers
        self._get_cluster_size: Callable[[], int] = get_cluster_size
        self._peer_rpc_timeout_seconds = peer_rpc_timeout_seconds

    async def broadcast_leadership(
        self,
        job_id: str,
        target_dc_count: int,
        callback_addr: tuple[str, int] | None = None,
    ) -> None:
        """
        Broadcast job leadership to peer gates.

        Args:
            job_id: Job identifier
            target_dc_count: Number of target datacenters
            callback_addr: Client callback address for leadership transfer
        """
        node_id = self._get_node_id()
        node_addr = self._get_node_addr()
        fence_token = self._leadership_tracker.get_fencing_token(job_id)

        announcement = JobLeadershipAnnouncement(
            job_id=job_id,
            leader_id=node_id.full,
            leader_addr=node_addr,
            term=fence_token,
            workflow_count=target_dc_count,
            fence_token=fence_token,
            target_dc_count=target_dc_count,
            callback_addr=callback_addr,
            origin_gate_addr=node_addr,
        )

        # Send to all active peers
        peers = self._get_active_peers()
        for peer_addr in peers:
            self._task_runner.run(
                self._send_leadership_announcement,
                peer_addr,
                announcement,
            )

        if callback_addr:
            transfer = GateJobLeaderTransfer(
                job_id=job_id,
                new_gate_id=node_id.full,
                new_gate_addr=node_addr,
                fence_token=fence_token,
            )
            await self._send_leadership_transfer_to_client(callback_addr, transfer)

    async def _send_leadership_announcement(
        self,
        peer_addr: tuple[str, int],
        announcement: JobLeadershipAnnouncement,
    ) -> None:
        try:
            await self._send_tcp(
                peer_addr,
                "job_leadership_announcement",
                announcement.dump(),
                timeout=self._peer_rpc_timeout_seconds,
            )
        except Exception as error:
            self._task_runner.run(
                self._logger.log,
                ServerDebug(
                    message=f"Failed to send leadership announcement to {peer_addr}: {error}",
                    node_host=self._get_node_addr()[0],
                    node_port=self._get_node_addr()[1],
                    node_id=self._get_node_id().short,
                ),
            )

    async def _send_leadership_transfer_to_client(
        self,
        callback_addr: tuple[str, int],
        transfer: GateJobLeaderTransfer,
    ) -> None:
        try:
            await self._send_tcp(
                callback_addr,
                "receive_gate_job_leader_transfer",
                transfer.dump(),
                timeout=self._peer_rpc_timeout_seconds,
            )
        except Exception as error:
            await self._logger.log(
                ServerWarning(
                    message=(
                        f"Failed to deliver gate leader transfer for job {transfer.job_id} "
                        f"to client {callback_addr}: {error}"
                    ),
                    node_host=self._get_node_addr()[0],
                    node_port=self._get_node_addr()[1],
                    node_id=self._get_node_id().short,
                )
            )

    def get_quorum_size(self) -> int:
        """Majority of the gate cluster (the server's one cluster-size
        definition, read live)."""
        return (self._get_cluster_size() // 2) + 1

    def has_quorum(self, gate_state_value: str) -> bool:
        if gate_state_value != "active":
            return False
        active_count = self._state.get_active_peer_count() + 1
        return active_count >= self.get_quorum_size()


__all__ = ["GateLeadershipCoordinator"]
