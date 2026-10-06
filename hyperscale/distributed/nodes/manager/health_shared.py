"""Definitions shared by the classes of
``hyperscale.distributed.nodes.manager.health`` (see that module)."""

from hyperscale.distributed.runtime import Clock, RealClock

_DEFAULT_CLOCK: Clock = RealClock()
