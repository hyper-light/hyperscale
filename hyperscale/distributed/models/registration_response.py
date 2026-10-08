"""Wire model ``RegistrationResponse`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message
from .manager_info import ManagerInfo


@dataclass(slots=True, kw_only=True)
class RegistrationResponse(Message):
    """
    Registration acknowledgment from manager to worker.

    Contains list of all known healthy managers so worker can
    establish redundant communication channels.

    Protocol Version (AD-25):
    - protocol_version_major/minor: For version compatibility checks
    - capabilities: Comma-separated negotiated features
    """

    accepted: bool  # Whether registration was accepted
    manager_id: str  # Responding manager's node_id
    healthy_managers: list[ManagerInfo]  # All known healthy managers (including self)
    error: str | None = None  # Error message if not accepted
    # Protocol version fields (AD-25) - defaults for backwards compatibility
    protocol_version_major: int = 1
    protocol_version_minor: int = 0
    capabilities: str = ""  # Comma-separated negotiated features
