"""Wire model ``NodeInfo`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True)
class NodeInfo(Message):
    """
    Identity information for any node in the cluster.

    Used for registration, heartbeats, and state sync.
    """

    node_id: str  # Unique node identifier
    role: str  # NodeRole value
    host: str  # Network host
    port: int  # TCP port
    datacenter: str  # Datacenter identifier
    version: int = 0  # State version (Lamport clock)
    udp_port: int = 0  # UDP port for SWIM (defaults to 0, derived from port if not set)
