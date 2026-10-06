"""Definitions shared by the classes of
``hyperscale.distributed.leases.job_lease`` (see that module)."""

from __future__ import annotations

from hyperscale.distributed.runtime import Clock, RealClock

_DEFAULT_CLOCK: Clock = RealClock()
