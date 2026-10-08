"""Wire model ``WorkerRegistration`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message
from .node_info import NodeInfo


@dataclass(slots=True)
class WorkerRegistration(Message):
    """
    Worker registration message sent to managers.

    Contains worker identity and capacity information.

    Protocol Version (AD-25):
    - protocol_version_major/minor: For version compatibility checks
    - capabilities: Comma-separated list of supported features

    Cluster Isolation (AD-28 Issue 2):
    - cluster_id: Cluster identifier for isolation validation
    - environment_id: Environment identifier for isolation validation
    """

    node: NodeInfo  # Worker identity
    total_cores: int  # Total CPU cores available
    available_cores: int  # Currently free cores
    memory_mb: int  # Total memory in MB
    available_memory_mb: int = 0  # Currently free memory
    cluster_id: str = ""  # Cluster identifier for isolation
    environment_id: str = ""  # Environment identifier for isolation
    # Protocol version fields (AD-25) - defaults for backwards compatibility
    protocol_version_major: int = 1
    protocol_version_minor: int = 0
    capabilities: str = ""  # Comma-separated feature list
