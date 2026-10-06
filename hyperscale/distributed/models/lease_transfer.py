"""Wire model ``LeaseTransfer`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True)
class LeaseTransfer(Message):
    """
    Transfer a lease to another gate (during scaling).
    """

    job_id: str  # Job identifier
    datacenter: str  # Datacenter
    from_gate: str  # Current holder
    to_gate: str  # New holder
    new_fence_token: int  # New fencing token
    version: int  # Transfer version
