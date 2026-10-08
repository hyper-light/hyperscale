"""Wire model ``ManagerRegistrationState`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from typing import TYPE_CHECKING
from dataclasses import dataclass

if TYPE_CHECKING:
    from .manager_heartbeat import ManagerHeartbeat


@dataclass(slots=True)
class ManagerRegistrationState:
    """
    Per-manager registration state tracked by a Gate.

    Tracks when each manager registered and heartbeat patterns for
    adaptive staleness detection. Generation IDs handle manager restarts.
    """

    manager_addr: tuple[str, int]  # (host, tcp_port)
    node_id: str | None = None  # Manager's node_id (from first heartbeat)
    generation: int = 0  # Increments on manager restart (from heartbeat)

    # Manager-leadership term carried in the heartbeat
    # (``ManagerHeartbeat.term``). Monotonically non-decreasing within
    # a manager generation; reset when a fresh generation is observed.
    # Used by the gate to validate manager-originated data-plane
    # results against the latest known leader manager in this DC.
    latest_term: int = 0
    # ``True`` on the most recent heartbeat. Surfaces "is this
    # manager the DC leader right now" without re-grepping heartbeats.
    is_leader: bool = False

    # Timing
    first_seen_at: float = 0.0  # monotonic time of first heartbeat
    last_heartbeat_at: float = 0.0  # monotonic time of most recent heartbeat

    # Heartbeat interval tracking (for adaptive staleness)
    heartbeat_count: int = 0  # Total heartbeats received
    avg_heartbeat_interval: float = 5.0  # Running average interval (seconds)

    @property
    def is_registered(self) -> bool:
        """Manager has sent at least one heartbeat."""
        return self.first_seen_at > 0

    def is_stale(self, now: float, staleness_multiplier: float = 3.0) -> bool:
        """
        Check if manager is stale based on adaptive interval.

        A manager is stale if no heartbeat received for staleness_multiplier
        times the average heartbeat interval.
        """
        if not self.is_registered:
            return False
        expected_interval = max(self.avg_heartbeat_interval, 1.0)
        return (now - self.last_heartbeat_at) > (
            staleness_multiplier * expected_interval
        )

    def record_heartbeat(
        self,
        now: float,
        node_id: str,
        generation: int,
        term: int = 0,
        is_leader: bool = False,
    ) -> bool:
        """
        Record a heartbeat from this manager.

        ``term`` is the manager's current leadership term
        (``ManagerHeartbeat.term``). It is stored monotonically within
        one process generation, and reset on a new generation so a
        restarted manager is not fenced by stale term state from its
        previous incarnation. ``is_leader`` is updated only for a
        heartbeat at least as fresh as ``latest_term``.

        Returns True if this is a new generation (manager restarted).
        """
        is_new_generation = generation > self.generation

        if is_new_generation or not self.is_registered:
            # New registration or restart - reset state
            self.node_id = node_id
            self.generation = generation
            self.first_seen_at = now
            self.heartbeat_count = 1
            self.avg_heartbeat_interval = 5.0  # Reset to default
            self.latest_term = term
            self.is_leader = is_leader
        else:
            # Update running average of heartbeat interval
            if self.last_heartbeat_at > 0:
                interval = now - self.last_heartbeat_at
                # Exponential moving average (alpha = 0.2)
                self.avg_heartbeat_interval = (
                    0.8 * self.avg_heartbeat_interval + 0.2 * interval
                )
            self.heartbeat_count += 1

            if term > self.latest_term:
                self.latest_term = term
                self.is_leader = is_leader
            elif term == self.latest_term:
                self.is_leader = is_leader

        self.last_heartbeat_at = now
        return is_new_generation
