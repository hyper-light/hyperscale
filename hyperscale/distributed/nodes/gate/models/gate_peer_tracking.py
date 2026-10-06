"""``GatePeerTracking`` -- pickled under the namespace
``hyperscale.distributed.nodes.gate.models.gate_peer_state`` (see that module)."""

from dataclasses import dataclass
from hyperscale.distributed.models import GateHeartbeat
from hyperscale.distributed.health import GateHealthState


@dataclass(slots=True)
class GatePeerTracking:
    """Tracks a single gate peer's state."""

    udp_addr: tuple[str, int]
    tcp_addr: tuple[str, int]
    epoch: int = 0
    is_active: bool = False
    heartbeat: GateHeartbeat | None = None
    health_state: GateHealthState | None = None
