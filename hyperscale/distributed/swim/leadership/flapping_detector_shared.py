"""Definitions shared by the classes of
``hyperscale.distributed.swim.leadership.flapping_detector`` (see that module)."""

from hyperscale.distributed.runtime import Clock, RealClock

_DEFAULT_CLOCK: Clock = RealClock()
