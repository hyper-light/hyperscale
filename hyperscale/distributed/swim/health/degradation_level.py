"""``DegradationLevel`` -- pickled under the namespace
``hyperscale.distributed.swim.health.graceful_degradation`` (see that module)."""

from enum import Enum


class DegradationLevel(Enum):
    """Levels of graceful degradation."""
    NORMAL = 0       # Normal operation
    LIGHT = 1        # Minor load shedding
    MODERATE = 2     # Significant load shedding
    HEAVY = 3        # Major load shedding
    CRITICAL = 4     # Emergency mode - minimal operation
