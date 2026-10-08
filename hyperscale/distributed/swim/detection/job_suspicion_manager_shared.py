"""Definitions shared by the classes of
``hyperscale.distributed.swim.detection.job_suspicion_manager`` (see that module)."""

from hyperscale.distributed.runtime import Clock, RealClock

_DEFAULT_CLOCK: Clock = RealClock()

# Type aliases
NodeAddress = tuple[str, int]

JobId = str
