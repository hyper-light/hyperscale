"""Wire model ``UpdateTier`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from enum import Enum


class UpdateTier(str, Enum):
    """
    Tiered update strategy for cross-DC stat synchronization.

    Not all stats need real-time updates. This enum defines the
    urgency/frequency tier for different types of updates.

    See AD-15 in docs/architecture.md for design rationale.
    """

    IMMEDIATE = "immediate"  # Event-driven, TCP push - completion, failure, critical
    PERIODIC = "periodic"  # Every 1-5s, TCP batch - progress, aggregate rates
    ON_DEMAND = "on_demand"  # Client request, TCP pull - step stats, historical
