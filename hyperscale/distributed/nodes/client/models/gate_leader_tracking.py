"""``GateLeaderTracking`` -- pickled under the namespace
``hyperscale.distributed.nodes.client.models.leader_tracking`` (see that module)."""

from dataclasses import dataclass
from hyperscale.distributed.models import GateLeaderInfo


@dataclass(slots=True)
class GateLeaderTracking:
    """Tracks gate leader for a job."""

    job_id: str
    leader_info: GateLeaderInfo
    last_updated: float
