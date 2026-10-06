"""Wire model ``GateRegistrationRequest`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True, kw_only=True)
class GateRegistrationRequest(Message):
    """
    Registration request from gate to manager.

    Gates register with all managers at startup (symmetric to managers
    registering with all gates). This ensures managers know about all
    gates for proper routing and health tracking.

    Protocol Version (AD-25):
    - protocol_version_major/minor: For version compatibility checks
    - capabilities: Comma-separated list of supported features

    Cluster Isolation (AD-28 Issue 2):
    - cluster_id: Cluster identifier for isolation validation
    - environment_id: Environment identifier for isolation validation
    """

    node_id: str  # Gate's unique identifier
    tcp_host: str  # Gate's TCP host
    tcp_port: int  # Gate's TCP port
    udp_host: str  # Gate's UDP host
    udp_port: int  # Gate's UDP port
    is_leader: bool  # Whether this gate is the leader
    term: int  # Current leadership term
    state: str  # GateState value
    datacenter: str = "global"  # Gate's home datacenter
    cluster_id: str = "hyperscale"  # Cluster identifier for isolation
    environment_id: str = "default"  # Environment identifier for isolation
    active_jobs: int = 0  # Number of active jobs
    manager_count: int = 0  # Number of known managers
    # Protocol version fields (AD-25)
    protocol_version_major: int = 1
    protocol_version_minor: int = 0
    capabilities: str = ""  # Comma-separated feature list
