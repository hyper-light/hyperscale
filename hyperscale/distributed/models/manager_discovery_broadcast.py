"""Wire model ``ManagerDiscoveryBroadcast`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True, kw_only=True)
class ManagerDiscoveryBroadcast(Message):
    """
    Broadcast from one gate to another about a newly discovered manager.

    Used for cross-gate synchronization of manager discovery.
    When a manager registers with one gate, that gate broadcasts
    to all peer gates so they can also track the manager.

    Includes manager status so peer gates can also update _datacenter_status.
    """

    datacenter: str  # Manager's datacenter
    manager_tcp_addr: tuple[str, int]  # Manager's TCP address
    manager_udp_addr: tuple[str, int] | None = None  # Manager's UDP address (if known)
    source_gate_id: str = ""  # Gate that received the original registration
    # Manager status info (from registration heartbeat)
    worker_count: int = 0  # Number of workers manager has
    healthy_worker_count: int = 0  # Healthy workers (SWIM responding)
    available_cores: int = 0  # Available cores for job dispatch
    total_cores: int = 0  # Total cores across all workers
