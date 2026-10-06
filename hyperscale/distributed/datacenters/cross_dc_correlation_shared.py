"""Definitions shared by the classes of
``hyperscale.distributed.datacenters.cross_dc_correlation`` (see that module)."""

from hyperscale.distributed.runtime import Clock, RealClock

_DEFAULT_CLOCK: Clock = RealClock()
