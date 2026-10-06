"""``LocalityTier`` -- pickled under the namespace
``hyperscale.distributed.discovery.models.locality_info`` (see that module)."""

from enum import IntEnum


class LocalityTier(IntEnum):
    """
    Locality tiers for peer preference.

    Lower values are preferred. SAME_DC is most preferred,
    GLOBAL is least preferred (fallback).
    """
    SAME_DC = 0      # Same datacenter (lowest latency, ~1-2ms)
    SAME_REGION = 1  # Same region, different DC (~10-50ms)
    GLOBAL = 2       # Different region (~50-200ms+)
