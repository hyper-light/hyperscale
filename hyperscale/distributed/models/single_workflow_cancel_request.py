"""Wire model ``SingleWorkflowCancelRequest`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True)
class SingleWorkflowCancelRequest(Message):
    """
    Request to cancel a specific workflow (Section 6).

    Can be sent from:
    - Client -> Gate (cross-DC workflow cancellation)
    - Gate -> Manager (DC-specific workflow cancellation)
    - Client -> Manager (direct DC workflow cancellation)

    If cancel_dependents is True, all workflows that depend on this one
    will also be cancelled recursively.
    """

    job_id: str  # Parent job ID
    workflow_id: str  # Specific workflow to cancel
    request_id: str  # Unique request ID for tracking/dedup
    requester_id: str  # Who requested cancellation
    timestamp: float  # When request was made
    cancel_dependents: bool = True  # Also cancel dependent workflows
    origin_gate_addr: tuple[str, int] | None = None  # For result push
    origin_client_addr: tuple[str, int] | None = None  # For direct client push
