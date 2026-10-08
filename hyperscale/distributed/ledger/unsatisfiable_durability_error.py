"""
Raised when a caller asks the ledger for a durability level the
deployment is not configured to reach.
"""

from __future__ import annotations

from .durability_level import DurabilityLevel


class UnsatisfiableDurabilityError(ValueError):
    """A durability request no amount of retrying can satisfy.

    Replication is wired at construction time, so a REGIONAL or GLOBAL
    request against a node with no matching replicator is a
    configuration error, not a runtime condition -- there is no state
    the caller could wait for that would make it succeed.

    It is raised BEFORE the WAL append rather than reported afterwards
    because the append is the damage. The ledger writes the entry
    first and only updates in-memory state once the commit succeeds,
    so an unsatisfiable request that got as far as appending would
    leave a durable entry describing a job the live node never
    tracked: reads say the job does not exist, and a restart replays
    the entry and says it does. Refusing up front is what keeps live
    and recovered state the same.
    """

    __slots__ = ("requested", "achievable")

    def __init__(
        self, requested: DurabilityLevel, achievable: DurabilityLevel
    ) -> None:
        self.requested = requested
        self.achievable = achievable
        super().__init__(
            f"{requested.value} durability requested but this node can only "
            f"reach {achievable.value}; no replicator is configured for the "
            "levels above it, so nothing was written"
        )
