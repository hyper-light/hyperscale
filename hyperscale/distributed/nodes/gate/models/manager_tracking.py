"""``ManagerTracking`` -- pickled under the namespace
``hyperscale.distributed.nodes.gate.models.dc_health_state`` (see that module)."""

from dataclasses import dataclass
from hyperscale.distributed.models import ManagerHeartbeat
from hyperscale.distributed.health import ManagerHealthState
from hyperscale.distributed.reliability import BackpressureLevel


@dataclass(slots=True)
class ManagerTracking:
    """Tracks a single manager's state."""

    address: tuple[str, int]
    datacenter_id: str
    last_heartbeat: ManagerHeartbeat | None = None
    last_status_time: float = 0.0
    health_state: ManagerHealthState | None = None
    backpressure_level: BackpressureLevel = BackpressureLevel.NONE
