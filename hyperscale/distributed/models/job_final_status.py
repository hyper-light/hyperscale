"""Wire model ``JobFinalStatus`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True)
class JobFinalStatus(Message):
    """
    Manager → Gate: Final job status for cleanup (AD-34 lifecycle management).

    Sent when job reaches terminal state (completed/failed/cancelled/timed out).
    Gate uses this to clean up timeout tracking for the job.

    When all DCs report terminal status, gate removes job from tracking to
    prevent memory leaks.
    """

    job_id: str
    datacenter: str
    manager_id: str
    status: str  # JobStatus.COMPLETED/FAILED/CANCELLED/TIMEOUT value
    timestamp: float
    fence_token: int
