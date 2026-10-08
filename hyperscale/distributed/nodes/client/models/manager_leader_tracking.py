"""``ManagerLeaderTracking`` -- pickled under the namespace
``hyperscale.distributed.nodes.client.models.leader_tracking`` (see that module)."""

from dataclasses import dataclass
from hyperscale.distributed.models import ManagerLeaderInfo


@dataclass(slots=True)
class ManagerLeaderTracking:
    """Tracks manager leader for a job+datacenter."""

    job_id: str
    datacenter_id: str
    leader_info: ManagerLeaderInfo
    last_updated: float
