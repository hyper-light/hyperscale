"""``CachedManagerInfo`` -- pickled under the namespace
``hyperscale.distributed.datacenters.datacenter_health_manager`` (see that module)."""

from dataclasses import dataclass
from hyperscale.distributed.models import ManagerHeartbeat


@dataclass(slots=True)
class CachedManagerInfo:
    """Cached information about a manager for health tracking."""

    heartbeat: ManagerHeartbeat
    last_seen: float
    is_alive: bool = True
