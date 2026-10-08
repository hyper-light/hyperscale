"""Wire model ``ManagerPeerRegistration`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message
from .manager_info import ManagerInfo


@dataclass(slots=True, kw_only=True)
class ManagerPeerRegistration(Message):
    """
    Registration request from one manager to another peer manager.

    When a manager discovers a new peer (via SWIM or seed list),
    it sends this registration to establish the bidirectional relationship.

    Protocol Version (AD-25):
    - protocol_version_major/minor: For version compatibility checks
    - capabilities: Comma-separated list of supported features

    Cluster Isolation (AD-28 Issue 2):
    - cluster_id: Cluster identifier for isolation validation
    - environment_id: Environment identifier for isolation validation
    """

    node: ManagerInfo  # Registering manager's info
    term: int  # Current leadership term
    is_leader: bool  # Whether registering manager is leader
    cluster_id: str = "hyperscale"  # Cluster identifier for isolation
    environment_id: str = "default"  # Environment identifier for isolation
    # Protocol version fields (AD-25) - defaults for backwards compatibility
    protocol_version_major: int = 1
    protocol_version_minor: int = 0
    capabilities: str = ""  # Comma-separated feature list
