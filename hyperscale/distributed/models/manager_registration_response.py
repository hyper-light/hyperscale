"""Wire model ``ManagerRegistrationResponse`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message
from .gate_info import GateInfo


@dataclass(slots=True, kw_only=True)
class ManagerRegistrationResponse(Message):
    """
    Registration acknowledgment from gate to manager.

    Contains list of all known healthy gates so manager can
    establish redundant communication channels.

    Protocol Version (AD-25):
    - protocol_version_major/minor: For version compatibility checks
    - capabilities: Comma-separated negotiated features
    """

    accepted: bool  # Whether registration was accepted
    gate_id: str  # Responding gate's node_id
    healthy_gates: list[GateInfo]  # All known healthy gates (including self)
    error: str | None = None  # Error message if not accepted
    # Protocol version fields (AD-25) - defaults for backwards compatibility
    protocol_version_major: int = 1
    protocol_version_minor: int = 0
    capabilities: str = ""  # Comma-separated negotiated features
