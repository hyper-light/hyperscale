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
    from hyperscale.distributed.nodes.manager.config import ManagerConfig
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
        task_runner,
    ) -> None:
        self._state = state
        self._config = config
        self._logger = logger
        self._node_id = node_id
        self._task_runner = task_runner

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

    def claim_job_leadership(
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

        can_claim = (
            current_leader is None or current_leader == self._node_id or force_takeover
        )

        if can_claim:
            self._state._job_leaders[job_id] = self._node_id
            self._state._job_leader_addrs[job_id] = tcp_addr

            if job_id not in self._state._job_fencing_tokens:
                self._state._job_fencing_tokens[job_id] = 1
                self._state._job_layer_version[job_id] = 1
            elif force_takeover:
                self._state._job_fencing_tokens[job_id] += 1

            action = "Took over" if force_takeover else "Claimed"
            self._task_runner.run(
                self._logger.log,
                ServerDebug(
                    message=f"{action} leadership for job {job_id[:8]}... (fence={self._state._job_fencing_tokens.get(job_id, 0)})",
                    node_host=self._config.host,
                    node_port=self._config.tcp_port,
                    node_id=self._node_id,
                ),
            )
            return True

        return False

    def release_job_leadership(self, job_id: str) -> None:
        """
        Release leadership for a job.

        Args:
            job_id: Job ID to release
        """
        if self._state._job_leaders.get(job_id) == self._node_id:
            self._state._job_leaders.pop(job_id, None)
            self._state._job_leader_addrs.pop(job_id, None)

            self._task_runner.run(
                self._logger.log,
                ServerDebug(
                    message=f"Released leadership for job {job_id[:8]}...",
                    node_host=self._config.host,
                    node_port=self._config.tcp_port,
                    node_id=self._node_id,
                ),
            )

    def transfer_job_leadership(
        self,
        job_id: str,
        new_leader_id: str,
        new_leader_addr: tuple[str, int],
    ) -> bool:
        """
        Transfer job leadership to another manager.

        Only succeeds if we are the current leader.

        Args:
            job_id: Job ID to transfer
            new_leader_id: New leader node ID
            new_leader_addr: New leader TCP address

        Returns:
            True if transfer successful
        """
        if self._state._job_leaders.get(job_id) != self._node_id:
            return False

        self._state._job_leaders[job_id] = new_leader_id
        self._state._job_leader_addrs[job_id] = new_leader_addr

        self._task_runner.run(
            self._logger.log,
            ServerDebug(
                message=f"Transferred leadership for job {job_id[:8]}... to {new_leader_id[:8]}...",
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            ),
        )
        return True

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

    def set_fence_token(self, job_id: str, value: int) -> None:
        """
        Set fencing token for a job to a specific value.

        Used during job initialization or explicit token assignment.

        Args:
            job_id: Job ID
            value: Token value to set
        """
        self._state._job_fencing_tokens[job_id] = value

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

    def get_layer_version(self, job_id: str) -> int:
        """
        Get current layer version for a job.

        Args:
            job_id: Job ID

        Returns:
            Current layer version (0 if not set)
        """
        return self._state._job_layer_version.get(job_id, 0)

    def increment_layer_version(self, job_id: str) -> int:
        """
        Increment and return layer version for a job.

        Used when completing a workflow layer to advance to next.

        Args:
            job_id: Job ID

        Returns:
            New layer version value
        """
        current = self._state._job_layer_version.get(job_id, 0)
        new_value = current + 1
        self._state._job_layer_version[job_id] = new_value
        return new_value

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
        self._state._job_layer_version.pop(job_id, None)
        self._state._job_contexts.pop(job_id, None)
