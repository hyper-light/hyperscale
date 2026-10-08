"""Wire model ``GateState`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from enum import Enum


class GateState(str, Enum):
    """
    State of a gate node in the cluster.

    New Gate Join Process:
    1. Gate joins SWIM cluster → State = SYNCING
    2. SYNCING gates are NOT counted in quorum
    3. Request state sync from leader (if not leader)
    4. Apply state snapshot
    5. State = ACTIVE → now counted in quorum

    This prevents new/recovering gates from affecting quorum
    until they have synchronized state from the cluster.
    """

    SYNCING = "syncing"  # Joined cluster, syncing state (not in quorum)
    ACTIVE = "active"  # Fully operational (counted in quorum)
    DRAINING = "draining"  # Not accepting new work, draining existing
