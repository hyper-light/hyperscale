"""Wire model ``JobLeaderManagerTransfer`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True)
class JobLeaderManagerTransfer(Message):
    """
    Notification that job leadership has transferred to a new manager (AD-31).

    Sent from the new job leader manager to the origin gate when manager
    failure triggers job ownership transfer within a datacenter. Gate updates
    its _job_dc_managers mapping to route requests to the new leader manager.

    Flow:
    - Manager-A (job leader in DC) fails
    - Manager-B (cluster leader) takes over job leadership
    - Manager-B sends JobLeaderManagerTransfer to origin gate
    - Gate updates _job_dc_managers[job_id][dc_id] = Manager-B address
    """

    job_id: str  # Job being transferred
    datacenter_id: str  # DC where leadership changed
    new_manager_id: str  # Node ID of new job leader manager
    new_manager_addr: tuple[str, int]  # TCP address of new leader manager
    fence_token: int  # Incremented fence token for consistency
    old_manager_id: str | None = None  # Node ID of old leader manager (if known)
