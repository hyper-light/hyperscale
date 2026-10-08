"""Wire model ``ManagerPeerRegistrationResponse`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message
from .manager_info import ManagerInfo


@dataclass(slots=True, kw_only=True)
class ManagerPeerRegistrationResponse(Message):
    """
    Registration acknowledgment from manager to peer manager.

    Contains list of all known peer managers so the registering
    manager can discover the full cluster topology.

    Protocol Version (AD-25):
    - protocol_version_major/minor: For version compatibility checks
    - capabilities: Comma-separated list of supported features
    """

    accepted: bool  # Whether registration was accepted
    manager_id: str  # Responding manager's node_id
    is_leader: bool  # Whether responding manager is leader
    term: int  # Responding manager's term
    known_peers: list[ManagerInfo]  # All known peer managers (for discovery)
    manager_info: ManagerInfo | None = None  # Responding manager's full address/identity
    error: str | None = None  # Error message if not accepted
    # Protocol version fields (AD-25) - defaults for backwards compatibility
    protocol_version_major: int = 1
    protocol_version_minor: int = 0
    capabilities: str = ""  # Comma-separated feature list
