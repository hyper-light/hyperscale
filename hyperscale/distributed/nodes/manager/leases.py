"""
Manager leases module for fencing tokens and ownership.

Provides at-most-once semantics through fencing tokens and
job leadership tracking (Context Consistency Protocol).
"""

import time
from typing import TYPE_CHECKING

from hyperscale.logging.hyperscale_logging_models import ServerDebug, ServerWarning

if TYPE_CHECKING:
    from hyperscale.distributed.nodes.manager.state import ManagerState
    from hyperscale.distributed.nodes.manager.models.manager_config import ManagerConfig
    from hyperscale.distributed.taskex import TaskRunner
    from hyperscale.logging import Logger


class ManagerLeaseCoordinator:
    """
    Coordinates job leadership and fencing tokens.

    Implements Context Consistency Protocol:
    - Job leader tracking (one manager per job)
    - Fencing tokens for at-most-once semantics
    - Layer versioning for dependency ordering
    """

    def __init__(
        self,
        state: "ManagerState",
        config: "ManagerConfig",
        logger: "Logger",
        node_id: str,
        task_runner: "TaskRunner",
    ) -> None:
        self._state: "ManagerState" = state
        self._config: "ManagerConfig" = config
        self._logger: "Logger" = logger
        self._node_id: str = node_id
        self._task_runner: "TaskRunner" = task_runner

    def is_job_leader(self, job_id: str) -> bool:
        """
        Check if this manager is leader for a job.

        Args:
            job_id: Job ID to check

        Returns:
            True if this manager is the job leader
        """
        return self._state._job_leaders.get(job_id) == self._node_id

    def get_job_leader(self, job_id: str) -> str | None:
        """
        Get the leader node ID for a job.

        Args:
            job_id: Job ID

        Returns:
            Leader node ID or None if not known
        """
        return self._state._job_leaders.get(job_id)

    def get_job_leader_addr(self, job_id: str) -> tuple[str, int] | None:
        """
        Get the leader address for a job.

        Args:
            job_id: Job ID

        Returns:
            Leader (host, port) or None if not known
        """
        return self._state._job_leader_addrs.get(job_id)

    async def claim_job_leadership(
        self,
        job_id: str,
        tcp_addr: tuple[str, int],
        force_takeover: bool = False,
    ) -> bool:
        """
        Claim leadership for a job.

        Only succeeds if no current leader, we are the leader, or force_takeover is True.

        Args:
            job_id: Job ID to claim
            tcp_addr: This manager's TCP address
            force_takeover: If True, forcibly take over from failed leader (increments fencing token)

        Returns:
            True if leadership claimed successfully
        """
        current_leader = self._state._job_leaders.get(job_id)

        if not self._can_claim_job(current_leader, force_takeover):
            return False

        current_token = self._state._job_fencing_tokens.get(job_id, 0)
        next_token = self._next_claim_fence_token(current_token, force_takeover)
        self._state.apply_job_leadership(
            job_id=job_id,
            leader_id=self._node_id,
            leader_addr=tcp_addr,
            fencing_token=next_token,
        )

        action = "Took over" if force_takeover else "Claimed"
        await self._logger.log(
            ServerDebug(
                message=(
                    f"{action} leadership for job {job_id[:8]}... "
                    f"(fence={self._state._job_fencing_tokens.get(job_id, 0)})"
                ),
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            ),
        )
        return True

    def _can_claim_job(self, current_leader: str | None, force_takeover: bool) -> bool:
        """No current leader, we already lead, or a forced takeover."""
        return current_leader is None or current_leader == self._node_id or force_takeover

    @staticmethod
    def _next_claim_fence_token(current_token: int, force_takeover: bool) -> int:
        """A takeover bumps the fence token; a plain claim keeps it (at least 1)."""
        return current_token + 1 if force_takeover else max(1, current_token)

    def apply_job_leadership(
        self,
        job_id: str,
        leader_id: str,
        leader_addr: tuple[str, int],
        fencing_token: int,
    ) -> bool:
        """
        Apply a leadership claim that already passed its authoritative protocol.

        This is the common ingress for Raft apply, state sync, and leadership
        announcements. It keeps ``ManagerState`` as the manager-visible source
        of truth while preserving fencing-token ordering.
        """
        return self._state.apply_job_leadership(
            job_id=job_id,
            leader_id=leader_id,
            leader_addr=leader_addr,
            fencing_token=fencing_token,
        )

    def get_fence_token(self, job_id: str) -> int:
        """
        Get current fencing token for a job.

        Args:
            job_id: Job ID

        Returns:
            Current fencing token (0 if not set)
        """
        return self._state._job_fencing_tokens.get(job_id, 0)

    async def increment_fence_token(self, job_id: str) -> int:
        async with self._state._get_counter_lock():
            current = self._state._job_fencing_tokens.get(job_id, 0)
            new_value = current + 1
            self._state._job_fencing_tokens[job_id] = new_value
            return new_value

    def update_fence_token_if_higher(self, job_id: str, new_token: int) -> bool:
        """
        Update fencing token only if new value is higher than current.

        Used during state sync to accept newer tokens from peers.

        Args:
            job_id: Job ID
            new_token: Proposed new token value

        Returns:
            True if token was updated, False if current token is >= new_token
        """
        current = self._state._job_fencing_tokens.get(job_id, 0)
        if new_token > current:
            self._state._job_fencing_tokens[job_id] = new_token
            return True
        return False

    def validate_fence_token(self, job_id: str, token: int) -> bool:
        """
        Validate a fencing token is current.

        Args:
            job_id: Job ID
            token: Token to validate

        Returns:
            True if token is valid (>= current)
        """
        current = self._state._job_fencing_tokens.get(job_id, 0)
        return token >= current

    def get_global_fence_token(self) -> int:
        """
        Get the global (non-job-specific) fence token.

        Returns:
            Current global fence token
        """
        return self._state._fence_token

    async def increment_global_fence_token(self) -> int:
        return await self._state.increment_fence_token()

    def get_led_job_ids(self) -> list[str]:
        """
        Get list of job IDs this manager leads.

        Returns:
            List of job IDs where this manager is leader
        """
        return [
            job_id
            for job_id, leader_id in self._state._job_leaders.items()
            if leader_id == self._node_id
        ]

    def clear_job_leases(self, job_id: str) -> None:
        """
        Clear all lease-related state for a job.

        Args:
            job_id: Job ID to clear
        """
        self._state._job_leaders.pop(job_id, None)
        self._state._job_leader_addrs.pop(job_id, None)
        self._state._job_fencing_tokens.pop(job_id, None)
