"""Wire model ``WorkflowStatusInfo`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from typing import TYPE_CHECKING
from dataclasses import dataclass, field
from .message import Message

if TYPE_CHECKING:
    from .workflow_query_response import WorkflowQueryResponse


@dataclass(slots=True, kw_only=True)
class WorkflowStatusInfo(Message):
    """
    Status information for a single workflow.

    Returned as part of WorkflowQueryResponse.
    """

    workflow_name: str  # Workflow class name
    workflow_id: str  # Unique workflow instance ID
    job_id: str  # Parent job ID
    status: str  # WorkflowStatus value
    # Provisioning info
    provisioned_cores: int = 0  # Cores allocated to this workflow
    vus: int = 0  # Virtual users (from workflow config)
    # Progress info
    completed_count: int = 0  # Actions completed
    failed_count: int = 0  # Actions failed
    rate_per_second: float = 0.0  # Current execution rate
    elapsed_seconds: float = 0.0  # Time since start
    # Queue info
    is_enqueued: bool = False  # True if waiting for cores
    queue_position: int = 0  # Position in queue (0 if not queued)
    # Worker assignment
    assigned_workers: list[str] = field(default_factory=list)  # Worker IDs
