"""Wire model ``RegisterCallback`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True)
class RegisterCallback(Message):
    """
    Client request to register for push notifications for a job.

    Used for client reconnection after disconnect. Client sends this
    to the job owner gate/manager to re-subscribe to status updates.

    Part of Client Reconnection (Component 5):
    1. Client disconnects from Gate-A
    2. Client reconnects and sends RegisterCallback(job_id=X)
    3. Gate/Manager adds callback_addr to job's notification list
    4. Client receives remaining status updates
    """

    job_id: str  # Job to register callback for
    callback_addr: tuple[str, int]  # Client's TCP address for push notifications
    last_sequence: int = 0
