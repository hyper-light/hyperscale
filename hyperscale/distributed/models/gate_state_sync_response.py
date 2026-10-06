"""Wire model ``GateStateSyncResponse`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from typing import TYPE_CHECKING
from dataclasses import dataclass
from .message import Message

if TYPE_CHECKING:
    from .gate_state_snapshot import GateStateSnapshot


@dataclass(slots=True)
class GateStateSyncResponse(Message):
    """
    Response to gate state sync request.
    """

    responder_id: str  # Responding gate node ID
    is_leader: bool  # Whether responder is the SWIM cluster leader
    term: int  # Current leadership term
    state_version: int  # Current state version
    snapshot: "GateStateSnapshot | None" = None  # Full state snapshot
    error: str | None = None  # Error message if sync failed
