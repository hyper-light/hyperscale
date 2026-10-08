"""Wire model ``DatacenterRegistrationStatus`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from enum import Enum


class DatacenterRegistrationStatus(str, Enum):
    """
    Registration status for a datacenter (distinct from health).

    Registration tracks whether managers have announced themselves to the gate.
    Health classification only applies to READY datacenters.

    State machine:
      AWAITING_INITIAL → (first heartbeat) → INITIALIZING
      INITIALIZING → (quorum heartbeats) → READY
      INITIALIZING → (grace period, no quorum) → UNAVAILABLE
      READY → (heartbeats continue) → READY
      READY → (heartbeats stop, < quorum) → PARTIAL
      READY → (all heartbeats stop) → UNAVAILABLE
    """

    AWAITING_INITIAL = "awaiting_initial"  # Configured but no heartbeats received yet
    INITIALIZING = "initializing"  # Some managers registered, waiting for quorum
    READY = "ready"  # Quorum of managers registered, health classification applies
    PARTIAL = "partial"  # Was ready, now below quorum (degraded but not lost)
    UNAVAILABLE = "unavailable"  # Was ready, lost all heartbeats (need recovery)
