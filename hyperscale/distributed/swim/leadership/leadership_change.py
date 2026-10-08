"""``LeadershipChange`` -- pickled under the namespace
``hyperscale.distributed.swim.leadership.flapping_detector`` (see that module)."""

from dataclasses import dataclass

from .flapping_detector_shared import _DEFAULT_CLOCK


@dataclass(slots=True)
class LeadershipChange:
    """
    Record of a leadership change event.
    
    Uses __slots__ for memory efficiency since many instances may be created
    during flapping episodes.
    """
    timestamp: float
    old_leader: tuple[str, int] | None
    new_leader: tuple[str, int] | None
    term: int
    reason: str  # e.g., 'election', 'stepdown', 'timeout', 'conflict'
    
    def __post_init__(self):
        if self.timestamp == 0:
            self.timestamp = _DEFAULT_CLOCK.monotonic()
