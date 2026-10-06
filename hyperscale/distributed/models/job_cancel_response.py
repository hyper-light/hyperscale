"""Wire model ``JobCancelResponse`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from typing import TYPE_CHECKING
from dataclasses import dataclass
from .message import Message

if TYPE_CHECKING:
    from .job_ack import JobAck


@dataclass(slots=True)
class JobCancelResponse(Message):
    """
    Response to a job cancellation request (AD-20).

    Returned by:
    - Gate: Aggregated result from all DCs
    - Manager: DC-local result
    - Worker: Workflow-level result

    Leader redirection:
    A manager that is NOT the leader for ``job_id`` populates
    ``leader_addr`` with the current leader's TCP address and sets
    ``success=False``. The client's cancel path follows the redirect
    analogously to ``JobAck`` on submission. Without this, a
    non-leader silently completes 0 workflows and reports success
    while the actual cancel never happens.
    """

    job_id: str  # Job that was cancelled
    success: bool  # Whether cancellation succeeded
    cancelled_workflow_count: int = 0  # Number of workflows cancelled
    already_cancelled: bool = False  # True if job was already cancelled
    already_completed: bool = False  # True if job was already completed
    error: str | None = None  # Error message if failed
    leader_addr: tuple[str, int] | None = None  # Leader address for redirect
    # The answering manager holds no record of the job.
    job_not_found: bool = False
