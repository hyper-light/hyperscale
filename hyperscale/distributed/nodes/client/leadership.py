"""
Leadership tracking for HyperscaleClient.

Handles gate/manager leader tracking and fence token validation.
Implements AD-16 (Leadership Transfer) semantics.
"""

from hyperscale.distributed.models import (
    GateLeaderInfo,
    ManagerLeaderInfo,
)
from hyperscale.distributed.nodes.client.state import ClientState

from hyperscale.distributed.runtime import Clock, RealClock


_DEFAULT_CLOCK: Clock = RealClock()


class ClientLeadershipTracker:
    """
    Manages leadership tracking for jobs (AD-16).

    Tracks gate and manager leaders per job and validates fence tokens
    for leadership transfers.

    Leadership transfer flow:
    1. New leader sends transfer notification with fence token
    2. Client validates fence token is monotonically increasing
    3. Client updates leader info
    4. Client uses new leader for future requests
    """

    def __init__(self, state: ClientState) -> None:
        self._state = state

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

        Stores the new leader info.

        Args:
            job_id: Job identifier
            gate_addr: New gate leader (host, port)
            fence_token: Fence token from transfer
        """
        self._state._gate_job_leaders[job_id] = GateLeaderInfo(
            gate_addr=gate_addr,
            fence_token=fence_token,
            last_updated=_DEFAULT_CLOCK.monotonic(),
        )

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
            last_updated=_DEFAULT_CLOCK.monotonic(),
        )
