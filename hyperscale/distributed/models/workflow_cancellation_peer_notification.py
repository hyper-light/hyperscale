"""Wire model ``WorkflowCancellationPeerNotification`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass, field
from .message import Message


@dataclass(slots=True)
class WorkflowCancellationPeerNotification(Message):
    """
    Peer notification for workflow cancellation (Section 6).

    Sent from manager-to-manager or gate-to-gate to synchronize
    cancellation state across the cluster. Ensures all peers mark
    the workflow (and dependents) as cancelled to prevent resurrection.
    """

    job_id: str  # Parent job ID
    workflow_id: str  # Primary workflow cancelled
    request_id: str  # Original request ID
    origin_node_id: str  # Node that initiated cancellation
    cancelled_workflows: list[str] = field(
        default_factory=list
    )  # All cancelled (incl deps)
    timestamp: float = 0.0  # When cancellation occurred
