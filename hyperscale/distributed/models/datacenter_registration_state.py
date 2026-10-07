"""Wire model ``DatacenterRegistrationState`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from typing import TYPE_CHECKING
from dataclasses import dataclass, field
from operator import attrgetter
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
        active_count = self.get_active_manager_count(now, staleness_multiplier)

        quorum = configured_count // 2 + 1

        return self._status_from_active_count(active_count, quorum)

    def _status_from_active_count(self, active_count: int, quorum: int) -> DatacenterRegistrationStatus:
        """Classify by active managers against the quorum (quorum >= 1, so
        the READY check never shadows the zero-active case)."""
        if active_count >= quorum:
            # At or above quorum
            return DatacenterRegistrationStatus.READY

        if active_count == 0:
            return self._status_without_active_managers()

        return self._status_below_quorum()

    def _status_without_active_managers(self) -> DatacenterRegistrationStatus:
        """Status when no registered manager is fresh."""
        if self.first_heartbeat_at == 0:
            # Never received any heartbeats
            return DatacenterRegistrationStatus.AWAITING_INITIAL

        # Had heartbeats before but all are now stale/lost
        return DatacenterRegistrationStatus.UNAVAILABLE

    def _status_below_quorum(self) -> DatacenterRegistrationStatus:
        """Status when some, but fewer than a quorum of, managers are fresh."""
        if self.first_heartbeat_at == 0 or self._was_ever_ready():
            # Was ready before, now below quorum
            return DatacenterRegistrationStatus.PARTIAL

        # Still coming up, not yet at quorum
        return DatacenterRegistrationStatus.INITIALIZING

    def _was_ever_ready(self) -> bool:
        """Check if this DC ever had quorum (any manager with heartbeat_count > 1)."""
        # If any manager has received multiple heartbeats, we were likely ready before
        return any(state.heartbeat_count > 1 for state in self.manager_states.values())

    def get_active_manager_count(
        self, now: float, staleness_multiplier: float = 3.0
    ) -> int:
        """Get count of non-stale registered managers."""
        registered_states = filter(attrgetter("is_registered"), self.manager_states.values())
        return sum(1 for state in registered_states if not state.is_stale(now, staleness_multiplier))

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
