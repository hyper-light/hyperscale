"""Wire model ``DatacenterLease`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True)
class DatacenterLease(Message):
    """
    Lease for job execution in a datacenter.

    Used by gates for at-most-once semantics across DCs.
    """

    job_id: str  # Job identifier
    datacenter: str  # Datacenter holding lease
    lease_holder: str  # Gate node_id holding lease
    fence_token: int  # Fencing token
    expires_at: float  # Monotonic expiration time
    version: int  # Lease version
