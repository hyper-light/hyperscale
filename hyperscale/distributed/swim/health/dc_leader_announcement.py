"""``DCLeaderAnnouncement`` -- pickled under the namespace
``hyperscale.distributed.swim.health.federated_health_monitor`` (see that module)."""

from dataclasses import dataclass, field
from hyperscale.distributed.models import Message

from .federated_health_monitor_shared import _DEFAULT_CLOCK


@dataclass(slots=True)
class DCLeaderAnnouncement(Message):
    """
    Announcement when a manager becomes DC leader.

    Sent via TCP to notify gates of leadership changes.
    """

    datacenter: str
    leader_node_id: str
    leader_tcp_addr: tuple[str, int]
    leader_udp_addr: tuple[str, int]
    term: int
    timestamp: float = field(default_factory=lambda: _DEFAULT_CLOCK.time())
