"""
Leadership tracking for HyperscaleClient.

Handles gate/manager leader tracking, fence token validation, and orphan detection.
Implements AD-16 (Leadership Transfer) semantics.
"""

import time

from hyperscale.distributed_rewrite.models import (
    GateLeaderInfo,
    ManagerLeaderInfo,
    OrphanedJobInfo,
)
from hyperscale.distributed_rewrite.nodes.client.state import ClientState
from hyperscale.logging import Logger


class ClientLeadershipTracker:
    """
    Manages leadership tracking for jobs (AD-16).

    Tracks gate and manager leaders per job, validates fence tokens
    for leadership transfers, and detects orphaned jobs.

    Leadership transfer flow:
    1. New leader sends transfer notification with fence token
    2. Client validates fence token is monotonically increasing
    3. Client updates leader info and clears orphan status
    4. Client uses new leader for future requests
    """

    def __init__(self, state: ClientState, logger: Logger) -> None:
        self._state = state
        self._logger = logger

    def validate_gate_fence_token(
        self, job_id: str, new_fence_token: int
    ) -> tuple[bool, str]:
        """
        Validate a gate transfer's fence token (AD-16).

        Fence tokens must be monotonically increasing to prevent
        accepting stale leadership transfers.

        Args:
            job_id: Job identifier
            new_fence_token: Fence token from new leader

        Returns:
            (is_valid, rejection_reason) tuple
        """
        current_leader = self._state._gate_job_leaders.get(job_id)
        if current_leader and new_fence_token <= current_leader.fence_token:
            return (
                False,
                f"Stale fence token: received {new_fence_token}, current {current_leader.fence_token}",
            )
        return (True, "")

    def validate_manager_fence_token(
        self,
        job_id: str,
        datacenter_id: str,
        new_fence_token: int,
    ) -> tuple[bool, str]:
        """
        Validate a manager transfer's fence token (AD-16).

        Fence tokens must be monotonically increasing per (job_id, datacenter_id).

        Args:
            job_id: Job identifier
            datacenter_id: Datacenter identifier
            new_fence_token: Fence token from new leader

        Returns:
            (is_valid, rejection_reason) tuple
        """
        key = (job_id, datacenter_id)
        current_leader = self._state._manager_job_leaders.get(key)
        if current_leader and new_fence_token <= current_leader.fence_token:
            return (
                False,
                f"Stale fence token: received {new_fence_token}, current {current_leader.fence_token}",
            )
        return (True, "")

    def update_gate_leader(
        self,
        job_id: str,
        gate_addr: tuple[str, int],
        fence_token: int,
    ) -> None:
        """
        Update gate job leader tracking.

        Stores the new leader info and clears orphan status if present.

        Args:
            job_id: Job identifier
            gate_addr: New gate leader (host, port)
            fence_token: Fence token from transfer
        """
        self._state._gate_job_leaders[job_id] = GateLeaderInfo(
            gate_addr=gate_addr,
            fence_token=fence_token,
            last_updated=time.monotonic(),
        )
        # Clear orphan status if present
        self._state.clear_job_orphaned(job_id)

    def update_manager_leader(
        self,
        job_id: str,
        datacenter_id: str,
        manager_addr: tuple[str, int],
        fence_token: int,
    ) -> None:
        """
        Update manager job leader tracking.

        Stores the new leader info keyed by (job_id, datacenter_id).

        Args:
            job_id: Job identifier
            datacenter_id: Datacenter identifier
            manager_addr: New manager leader (host, port)
            fence_token: Fence token from transfer
        """
        key = (job_id, datacenter_id)
        self._state._manager_job_leaders[key] = ManagerLeaderInfo(
            manager_addr=manager_addr,
            fence_token=fence_token,
            datacenter_id=datacenter_id,
            last_updated=time.monotonic(),
        )

    def mark_job_orphaned(
        self,
        job_id: str,
        last_known_gate: tuple[str, int] | None,
        last_known_manager: tuple[str, int] | None,
        datacenter_id: str = "",
    ) -> None:
        """
        Mark a job as orphaned.

        Called when we lose contact with the job's leader and cannot
        determine the current leader.

        Args:
            job_id: Job identifier
            last_known_gate: Last known gate address (if any)
            last_known_manager: Last known manager address (if any)
            datacenter_id: Datacenter identifier (if known)
        """
        orphan_info = OrphanedJobInfo(
            job_id=job_id,
            orphan_timestamp=time.monotonic(),
            last_known_gate=last_known_gate,
            last_known_manager=last_known_manager,
            datacenter_id=datacenter_id,
        )
        self._state.mark_job_orphaned(job_id, orphan_info)

    def clear_job_orphaned(self, job_id: str) -> None:
        """
        Clear orphaned status for a job.

        Called when we re-establish contact with the job's leader.

        Args:
            job_id: Job identifier
        """
        self._state.clear_job_orphaned(job_id)

    def is_job_orphaned(self, job_id: str) -> bool:
        """
        Check if a job is currently in orphan state.

        Args:
            job_id: Job identifier

        Returns:
            True if job is orphaned
        """
        return self._state.is_job_orphaned(job_id)

    def get_current_gate_leader(self, job_id: str) -> tuple[str, int] | None:
        """
        Get the current gate leader address for a job.

        Args:
            job_id: Job identifier

        Returns:
            Gate (host, port) or None if no leader tracked
        """
        leader_info = self._state._gate_job_leaders.get(job_id)
        if leader_info:
            return leader_info.gate_addr
        return None

    def get_current_manager_leader(
        self,
        job_id: str,
        datacenter_id: str,
    ) -> tuple[str, int] | None:
        """
        Get the current manager leader address for a job in a datacenter.

        Args:
            job_id: Job identifier
            datacenter_id: Datacenter identifier

        Returns:
            Manager (host, port) or None if no leader tracked
        """
        key = (job_id, datacenter_id)
        leader_info = self._state._manager_job_leaders.get(key)
        if leader_info:
            return leader_info.manager_addr
        return None

    def get_leadership_metrics(self) -> dict[str, int]:
        """
        Get leadership transfer and orphan tracking metrics.

        Returns:
            Dict with transfer counts, rerouted requests, failures, orphan counts
        """
        return self._state.get_leadership_metrics()

    async def orphan_check_loop(
        self,
        grace_period_seconds: float,
        check_interval_seconds: float,
    ) -> None:
        """
        Background task for orphan detection (placeholder).

        Periodically checks for jobs that haven't received leader updates
        within the grace period and marks them as orphaned.

        Args:
            grace_period_seconds: Time without update before marking orphaned
            check_interval_seconds: How often to check for orphans

        Note: Full implementation would require async loop integration.
        Currently a placeholder for future orphan detection logic.
        """
        # Placeholder for background orphan detection
        # In full implementation, would:
        # 1. Loop with asyncio.sleep(check_interval_seconds)
        # 2. Check leader last_updated timestamps
        # 3. Mark jobs as orphaned if grace_period exceeded
        # 4. Log orphan detections
        pass
