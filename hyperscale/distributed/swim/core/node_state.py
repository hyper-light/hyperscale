"""
Node state tracking for SWIM membership.
"""

from dataclasses import dataclass
from .types import Status


@dataclass(slots=True)
class NodeState:
    """
    Tracks the state of a known node in the SWIM membership.

    Includes status, incarnation number, and timing information
    for the suspicion subprotocol.

    Uses __slots__ for memory efficiency since many instances are created.
    """
    status: Status = b'OK'
    incarnation: int = 0
    last_update_time: float = 0.0

    @property
    def last_seen(self) -> float:
        """Alias for last_update_time for backward compatibility."""
        return self.last_update_time

    def update(self, new_status: Status, new_incarnation: int, timestamp: float) -> bool:
        """
        Update node state if the new information is fresher.
        Returns True if the state was updated, False if ignored.

        Per SWIM protocol + AD-35:
        - Higher incarnation always wins
        - Same incarnation: DEAD > SUSPECT > OK > UNCONFIRMED
        - UNCONFIRMED cannot transition to SUSPECT (AD-35 Task 12.3.4)
        - Lower incarnation is always ignored
        """
        if new_incarnation > self.incarnation:
            self.status = new_status
            self.incarnation = new_incarnation
            self.last_update_time = timestamp
            return True
        elif new_incarnation == self.incarnation:
            # Same incarnation - apply status priority
            # AD-35: UNCONFIRMED has lowest priority, cannot go to SUSPECT
            status_priority = {
                b'UNCONFIRMED': -1,  # Lowest priority (AD-35 Task 12.3.1)
                b'OK': 0,
                b'JOIN': 0,
                b'SUSPECT': 1,
                b'DEAD': 2
            }

            # AD-35 Task 12.3.4: Prevent UNCONFIRMED → SUSPECT transitions
            if self.status == b'UNCONFIRMED' and new_status == b'SUSPECT':
                return False  # Ignore suspect messages for unconfirmed peers

            if status_priority.get(new_status, 0) > status_priority.get(self.status, 0):
                self.status = new_status
                self.last_update_time = timestamp
                return True
        return False

