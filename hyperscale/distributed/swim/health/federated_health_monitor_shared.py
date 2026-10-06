"""Definitions shared by the classes of
``hyperscale.distributed.swim.health.federated_health_monitor`` (see that module)."""

from hyperscale.distributed.runtime import Clock, RealClock

_DEFAULT_CLOCK: Clock = RealClock()
