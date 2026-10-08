"""Wire model ``WorkerDiscoveryBroadcast`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True, kw_only=True)
class WorkerDiscoveryBroadcast(Message):
    """
    Broadcast from one manager to another about a newly discovered worker.

    Used for cross-manager synchronization of worker discovery.
    When a worker registers with one manager, that manager broadcasts
    to all peer managers so they can also track the worker.
    """

    worker_id: str  # Worker's node_id
    worker_tcp_addr: tuple[str, int]  # Worker's TCP address
    worker_udp_addr: tuple[str, int]  # Worker's UDP address
    datacenter: str  # Worker's datacenter
    available_cores: int  # Worker's available cores
    source_manager_id: str = ""  # Manager that received the original registration
