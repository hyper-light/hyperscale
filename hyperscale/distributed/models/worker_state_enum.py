"""Wire model ``WorkerState`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from enum import Enum


class WorkerState(str, Enum):
    """State of a worker node."""

    HEALTHY = "healthy"  # Normal operation
    DEGRADED = "degraded"  # High load, accepting with backpressure
    DRAINING = "draining"  # Not accepting new work
    OFFLINE = "offline"  # Not responding
