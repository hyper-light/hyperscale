"""Wire model ``JobLeaderWorkerTransferAck`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass, field
from .message import Message


@dataclass(slots=True)
class JobLeaderWorkerTransferAck(Message):
    """
    Acknowledgment of job leader worker transfer notification (Section 8.4).

    Sent from worker to new job leader manager after processing transfer.
    Contains workflow state information so the new leader can verify all workers acknowledged.
    """

    job_id: str  # Job being acknowledged
    worker_id: str  # Node ID of responding worker
    workflows_updated: int  # Number of workflow routings updated
    accepted: bool = True  # Whether transfer was applied
    rejection_reason: str = ""  # Reason if rejected (8.2)
    fence_token_received: int = 0  # The fence token from the transfer (8.4)
    workflow_states: dict[str, str] = field(
        default_factory=dict
    )  # workflow_id -> status (8.4)
