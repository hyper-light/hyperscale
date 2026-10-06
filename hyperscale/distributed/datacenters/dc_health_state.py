"""``DCHealthState`` -- pickled under the namespace
``hyperscale.distributed.datacenters.cross_dc_correlation`` (see that module)."""

from enum import Enum


class DCHealthState(Enum):
    """Per-DC health state with hysteresis."""

    HEALTHY = "healthy"  # DC is operating normally
    DEGRADED = "degraded"  # DC has some issues but not failing
    FAILING = "failing"  # DC is actively failing (not yet confirmed)
    FAILED = "failed"  # DC failure confirmed (sustained)
    RECOVERING = "recovering"  # DC showing signs of recovery
    FLAPPING = "flapping"  # DC is oscillating rapidly
