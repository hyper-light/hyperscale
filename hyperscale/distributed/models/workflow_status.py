"""Wire model ``WorkflowStatus`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from enum import Enum


class WorkflowStatus(str, Enum):
    """Status of a single workflow within a job."""

    PENDING = "pending"  # Not yet started
    ASSIGNED = "assigned"  # Assigned/dispatched to worker(s)
    RUNNING = "running"  # Executing
    COMPLETED = "completed"  # Finished successfully
    FAILED = "failed"  # Failed
    CANCELLED = "cancelled"  # Cancelled
    AGGREGATED = "aggregated"  # Results successfully aggregated (internal)
    AGGREGATION_FAILED = "aggregation_failed"  # Aggregation failed (internal)
