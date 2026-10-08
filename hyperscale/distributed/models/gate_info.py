"""Wire model ``GateInfo`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True)
class GateInfo(Message):
    """
    Gate identity and address information for manager discovery.

    Managers use this to maintain a list of known gates for
    redundant communication and failover.
    """

    node_id: str  # Gate's unique identifier
    tcp_host: str  # TCP host for data operations
    tcp_port: int  # TCP port for data operations
    udp_host: str  # UDP host for SWIM healthchecks
    udp_port: int  # UDP port for SWIM healthchecks
    datacenter: str  # Datacenter identifier (gate's home DC)
    is_leader: bool = False  # Whether this gate is the current leader
