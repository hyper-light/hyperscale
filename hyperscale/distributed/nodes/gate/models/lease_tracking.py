"""``LeaseTracking`` -- pickled under the namespace
``hyperscale.distributed.nodes.gate.models.lease_state`` (see that module)."""

from dataclasses import dataclass
from hyperscale.distributed.models import DatacenterLease


@dataclass(slots=True)
class LeaseTracking:
    """Tracks a single lease state."""

    job_id: str
    datacenter_id: str
    lease: DatacenterLease
    fence_token: int
