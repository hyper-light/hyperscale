"""``CrossClusterProbe`` -- pickled under the namespace
``hyperscale.distributed.swim.health.federated_health_monitor`` (see that module)."""

from dataclasses import dataclass
from hyperscale.distributed.models import Message


@dataclass(slots=True)
class CrossClusterProbe(Message):
    """
    Cross-cluster health probe (xprobe).

    Sent from gates to DC leader managers to check health.
    Minimal format - no gossip, just identity.
    """

    source_cluster_id: str  # Gate cluster ID
    source_node_id: str  # Sending gate's node ID
    source_addr: tuple[str, int]  # For response routing
