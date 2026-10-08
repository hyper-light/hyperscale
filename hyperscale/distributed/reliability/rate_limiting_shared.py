"""Definitions shared by the classes of
``hyperscale.distributed.reliability.rate_limiting`` (see that module)."""

from hyperscale.distributed.runtime import Clock, RealClock

_DEFAULT_CLOCK: Clock = RealClock()
