"""
Gate leadership coordination module.

Coordinates job leadership, lease management, and peer gate coordination.
"""

import asyncio
from typing import TYPE_CHECKING, Callable

from hyperscale.distributed.models import (
    GateJobLeaderTransfer,
    JobLeadershipAnnouncement,
    JobLeadershipAck,
    JobLeaderGateTransfer,
    JobLeaderGateTransferAck,
)

if TYPE_CHECKING:
    from hyperscale.distributed.nodes.gate.state import GateRuntimeState
    from hyperscale.distributed.jobs import JobLeadershipTracker
    from hyperscale.logging import Logger
    from hyperscale.distributed.taskex import TaskRunner


class GateLeadershipCoordinator:
    """
    Coordinates job leadership across peer gates.

    Responsibilities:
    - Track job leadership with fencing tokens
    - Handle leadership announcements
    - Coordinate leadership transfers
    - Manage orphaned jobs
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
    ) -> None:
        self._state: "GateRuntimeState" = state
        self._logger: "Logger" = logger
        self._task_runner: "TaskRunner" = task_runner
        self._leadership_tracker: "JobLeadershipTracker" = leadership_tracker
        self._get_node_id: Callable = get_node_id
        self._get_node_addr: Callable = get_node_addr
        self._send_tcp: Callable = send_tcp
        self._get_active_peers: Callable = get_active_peers

    def is_job_leader(self, job_id: str) -> bool:
        """
        Check if this gate is the leader for a job.

        Args:
            job_id: Job identifier

        Returns:
            True if this gate is the leader
        """
        return self._leadership_tracker.is_leader(job_id)

    def assume_leadership(self, job_id: str, target_dc_count: int) -> None:
        """
        Assume leadership for a job.

        Args:
            job_id: Job identifier
            target_dc_count: Number of target datacenters
        """
        self._leadership_tracker.assume_leadership(
            job_id=job_id,
            metadata=target_dc_count,
        )

    async def broadcast_leadership(
        self,
        job_id: str,
        target_dc_count: int,
    ) -> None:
        """
        Broadcast job leadership to peer gates.

        Args:
            job_id: Job identifier
            target_dc_count: Number of target datacenters
        """
        node_id = self._get_node_id()
        node_addr = self._get_node_addr()
        fence_token = self._leadership_tracker.get_fence_token(job_id)

        announcement = JobLeadershipAnnouncement(
            job_id=job_id,
            leader_id=node_id.full,
            leader_addr=node_addr,
            fence_token=fence_token,
            target_dc_count=target_dc_count,
        )

        # Send to all active peers
        peers = self._get_active_peers()
        for peer_addr in peers:
            self._task_runner.run(
                self._send_leadership_announcement,
                peer_addr,
                announcement,
            )

    async def _send_leadership_announcement(
        self,
        peer_addr: tuple[str, int],
        announcement: JobLeadershipAnnouncement,
    ) -> None:
        """Send leadership announcement to a peer gate."""
        try:
            await self._send_tcp(
                peer_addr,
                "job_leadership_announcement",
                announcement.dump(),
                timeout=5.0,
            )
        except Exception:
            pass  # Best effort

    def handle_leadership_announcement(
        self,
        job_id: str,
        leader_id: str,
        leader_addr: tuple[str, int],
        fence_token: int,
        target_dc_count: int,
    ) -> JobLeadershipAck:
        """
        Handle leadership announcement from peer gate.

        Args:
            job_id: Job identifier
            leader_id: Leader gate ID
            leader_addr: Leader gate address
            fence_token: Fencing token for ordering
            target_dc_count: Number of target datacenters

        Returns:
            Acknowledgment
        """
        # Check if we already have leadership with higher fence token
        current_token = self._leadership_tracker.get_fence_token(job_id)
        node_id = self._get_node_id()
        if current_token and current_token >= fence_token:
            return JobLeadershipAck(
                job_id=job_id,
                accepted=False,
                responder_id=node_id.full,
            )

        # Accept the leadership announcement
        self._leadership_tracker.record_external_leader(
            job_id=job_id,
            leader_id=leader_id,
            leader_addr=leader_addr,
            fence_token=fence_token,
            metadata=target_dc_count,
        )

        return JobLeadershipAck(
            job_id=job_id,
            accepted=True,
            responder_id=node_id.full,
        )

    async def transfer_leadership(
        self,
        job_id: str,
        new_leader_id: str,
        new_leader_addr: tuple[str, int],
        reason: str = "requested",
    ) -> bool:
        """
        Transfer job leadership to another gate.

        Args:
            job_id: Job identifier
            new_leader_id: New leader gate ID
            new_leader_addr: New leader gate address
            reason: Transfer reason

        Returns:
            True if transfer succeeded
        """
        if not self.is_job_leader(job_id):
            return False

        fence_token = self._leadership_tracker.get_fence_token(job_id)
        new_token = fence_token + 1

        transfer = JobLeaderGateTransfer(
            job_id=job_id,
            new_gate_id=new_leader_id,
            new_gate_addr=new_leader_addr,
            fence_token=new_token,
            old_gate_id=self._get_node_id().full,
        )

        try:
            response, _ = await self._send_tcp(
                new_leader_addr,
                "job_leader_gate_transfer",
                transfer.dump(),
                timeout=10.0,
            )

            if response and not isinstance(response, Exception):
                ack = JobLeaderGateTransferAck.load(response)
                if ack.accepted:
                    # Relinquish leadership
                    self._leadership_tracker.relinquish(job_id)
                    return True

            return False

        except Exception:
            return False

    def handle_leadership_transfer(
        self,
        job_id: str,
        old_leader_id: str,
        new_leader_id: str,
        fence_token: int,
        reason: str,
    ) -> JobLeaderGateTransferAck:
        """
        Handle incoming leadership transfer request.

        Args:
            job_id: Job identifier
            old_leader_id: Previous leader gate ID
            new_leader_id: New leader gate ID (should be us)
            fence_token: New fence token
            reason: Transfer reason

        Returns:
            Transfer acknowledgment
        """
        my_id = self._get_node_id().full
        if new_leader_id != my_id:
            return JobLeaderGateTransferAck(
                job_id=job_id,
                manager_id=my_id,
                accepted=False,
            )

        # Accept the transfer
        self._leadership_tracker.assume_leadership(
            job_id=job_id,
            metadata=0,  # Will be updated from job state
            fence_token=fence_token,
        )

        return JobLeaderGateTransferAck(
            job_id=job_id,
            manager_id=my_id,
            accepted=True,
        )

    def get_job_leader(self, job_id: str) -> tuple[str, tuple[str, int]] | None:
        """
        Get the leader for a job.

        Args:
            job_id: Job identifier

        Returns:
            (leader_id, leader_addr) or None if not known
        """
        return self._leadership_tracker.get_leader(job_id)

    def mark_job_orphaned(self, job_id: str) -> None:
        """
        Mark a job as orphaned (leader dead).

        Args:
            job_id: Job identifier
        """
        import time

        self._state.mark_job_orphaned(job_id, time.monotonic())

    def clear_orphaned_job(self, job_id: str) -> None:
        """
        Clear orphaned status for a job.

        Args:
            job_id: Job identifier
        """
        self._state.clear_orphaned_job(job_id)

    def get_quorum_size(self) -> int:
        active_peer_count = self._state.get_active_peer_count()
        total_gates = active_peer_count + 1
        return (total_gates // 2) + 1

    def has_quorum(self, gate_state_value: str) -> bool:
        if gate_state_value != "active":
            return False
        active_count = self._state.get_active_peer_count() + 1
        return active_count >= self.get_quorum_size()


__all__ = ["GateLeadershipCoordinator"]
