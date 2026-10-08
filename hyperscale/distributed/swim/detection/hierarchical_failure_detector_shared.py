"""Definitions shared by the classes of
``hyperscale.distributed.swim.detection.hierarchical_failure_detector`` (see that module)."""

from hyperscale.distributed.runtime import Clock, RealClock

_DEFAULT_CLOCK: Clock = RealClock()

# Type aliases
NodeAddress = tuple[str, int]

JobId = str
