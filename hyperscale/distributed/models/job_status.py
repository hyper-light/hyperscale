"""Wire model ``JobStatus`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from enum import Enum


class JobStatus(str, Enum):
    """Status of a distributed job."""

    SUBMITTED = "submitted"  # Job received, not yet dispatched
    QUEUED = "queued"  # Queued for execution
    DISPATCHING = "dispatching"  # Being dispatched to workers
    RUNNING = "running"  # Active execution
    COMPLETING = "completing"  # Wrapping up, gathering results
    COMPLETED = "completed"  # Successfully finished
    FAILED = "failed"  # Failed (may be retried)
    CANCELLED = "cancelled"  # User cancelled
    TIMEOUT = "timeout"  # Exceeded time limit
    UNKNOWN = "unknown"  # Not known to this node (status queries only)
