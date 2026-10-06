"""Definitions shared by the classes of
``hyperscale.distributed.swim.core.state_embedder`` (see that module)."""

from hyperscale.distributed.runtime import Clock, RealClock

_DEFAULT_CLOCK: Clock = RealClock()

# Maximum size for probe RTT cache to prevent unbounded memory growth
_PROBE_RTT_CACHE_MAX_SIZE = 100
