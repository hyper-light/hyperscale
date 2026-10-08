"""Wire model ``JobLeaderWorkerTransfer`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True)
class JobLeaderWorkerTransfer(Message):
    """
    Notification to workers that job leadership has transferred (AD-31).

    Sent from the new job leader manager to workers with active workflows
    for the job. Workers update their _workflow_job_leader mapping to route
    progress updates to the new manager.

    Flow:
    - Manager-A (job leader) fails
    - Manager-B takes over job leadership
    - Manager-B sends JobLeaderWorkerTransfer to workers with active sub-workflows
    - Workers update _workflow_job_leader for affected workflows
    """

    job_id: str  # Job whose leadership transferred
    workflow_ids: list[str]  # Workflow IDs affected (worker's active workflows)
    new_manager_id: str  # Node ID of new job leader manager
    new_manager_addr: tuple[str, int]  # TCP address of new leader manager
    fence_token: int  # Fencing token for consistency
    old_manager_id: str | None = None  # Node ID of old leader manager (if known)
