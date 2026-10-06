"""Wire model ``GateStateSyncRequest`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True)
class GateStateSyncRequest(Message):
    """
    Request for gate-to-gate state synchronization.

    Sent when a gate needs to sync state with a peer gate.
    """

    requester_id: str  # Requesting gate node ID
    known_version: int = 0  # Last known state version
