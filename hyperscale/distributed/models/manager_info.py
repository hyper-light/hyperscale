"""Wire model ``ManagerInfo`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True)
class ManagerInfo(Message):
    """
    Manager identity and address information for worker discovery.

    Workers use this to maintain a list of known managers for
    redundant communication and failover.
    """

    node_id: str  # Manager's unique identifier
    tcp_host: str  # TCP host for data operations
    tcp_port: int  # TCP port for data operations
    udp_host: str  # UDP host for SWIM healthchecks
    udp_port: int  # UDP port for SWIM healthchecks
    datacenter: str  # Datacenter identifier
    is_leader: bool = False  # Whether this manager is the current leader
