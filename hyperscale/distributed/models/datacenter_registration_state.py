"""Wire model ``DatacenterRegistrationState`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from typing import TYPE_CHECKING
from dataclasses import dataclass, field
from .datacenter_registration_status import DatacenterRegistrationStatus
from .manager_registration_state import ManagerRegistrationState

if TYPE_CHECKING:
    from .workflow_result_push import WorkflowResultPush


@dataclass(slots=True)
class DatacenterRegistrationState:
    """
    Per-datacenter registration state tracked by a Gate.

    Tracks which managers have registered and provides registration status
    based on quorum requirements. Health classification only applies once
    the datacenter is READY.
    """

    dc_id: str  # Datacenter identifier
    configured_managers: list[tuple[str, int]]  # Manager addrs from config

    # Per-manager tracking
    manager_states: dict[tuple[str, int], ManagerRegistrationState] = field(
        default_factory=dict
    )

    # Timing
    first_heartbeat_at: float = 0.0  # When first manager registered (monotonic)
    last_heartbeat_at: float = 0.0  # Most recent heartbeat from any manager (monotonic)

    def get_registration_status(
        self, now: float, staleness_multiplier: float = 3.0
    ) -> DatacenterRegistrationStatus:
        """
        Compute current registration status based on manager heartbeats.

        Uses quorum (majority) of configured managers as the threshold
        for READY status.
        """
        configured_count = len(self.configured_managers)
        if configured_count == 0:
            return DatacenterRegistrationStatus.UNAVAILABLE

        # Count non-stale registered managers
        active_count = sum(
            1
            for state in self.manager_states.values()
            if state.is_registered and not state.is_stale(now, staleness_multiplier)
        )

        quorum = configured_count // 2 + 1

        if active_count == 0:
            if self.first_heartbeat_at == 0:
                # Never received any heartbeats
                return DatacenterRegistrationStatus.AWAITING_INITIAL
            else:
                # Had heartbeats before but all are now stale/lost
                return DatacenterRegistrationStatus.UNAVAILABLE
        elif active_count < quorum:
            if self.first_heartbeat_at == 0 or self._was_ever_ready():
                # Was ready before, now below quorum
                return DatacenterRegistrationStatus.PARTIAL
            else:
                # Still coming up, not yet at quorum
                return DatacenterRegistrationStatus.INITIALIZING
        else:
            # At or above quorum
            return DatacenterRegistrationStatus.READY

    def _was_ever_ready(self) -> bool:
        """Check if this DC ever had quorum (any manager with heartbeat_count > 1)."""
        # If any manager has received multiple heartbeats, we were likely ready before
        return any(state.heartbeat_count > 1 for state in self.manager_states.values())

    def get_active_manager_count(
        self, now: float, staleness_multiplier: float = 3.0
    ) -> int:
        """Get count of non-stale registered managers."""
        return sum(
            1
            for state in self.manager_states.values()
            if state.is_registered and not state.is_stale(now, staleness_multiplier)
        )

    def record_heartbeat(
        self,
        manager_addr: tuple[str, int],
        node_id: str,
        generation: int,
        now: float,
        term: int = 0,
        is_leader: bool = False,
    ) -> bool:
        """
        Record a heartbeat from a manager in this datacenter.

        Returns True if this is a new manager or a manager restart (new generation).
        """
        if manager_addr not in self.manager_states:
            self.manager_states[manager_addr] = ManagerRegistrationState(
                manager_addr=manager_addr,
            )

        is_new = self.manager_states[manager_addr].record_heartbeat(
            now, node_id, generation, term=term, is_leader=is_leader
        )

        # Update DC-level timing
        if self.first_heartbeat_at == 0:
            self.first_heartbeat_at = now
        self.last_heartbeat_at = now

        return is_new

    def get_known_leader_manager_term(self) -> int:
        """Highest term observed from a manager advertising leadership.

        Used by the gate to validate ``WorkflowResultPush.manager_fence_token``
        on manager-originated data-plane results. Follower/candidate
        heartbeats can carry newer terms before a stable leader is
        known; those must not fence valid data-plane results. Therefore
        only current leader heartbeats contribute to this value.
        """
        return max(
            (
                state.latest_term
                for state in self.manager_states.values()
                if state.is_leader
            ),
            default=0,
        )
