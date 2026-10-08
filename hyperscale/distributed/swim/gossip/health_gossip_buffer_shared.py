"""Definitions shared by the classes of
``hyperscale.distributed.swim.gossip.health_gossip_buffer`` (see that module)."""

from hyperscale.distributed.runtime import Clock, RealClock

_DEFAULT_CLOCK: Clock = RealClock()
