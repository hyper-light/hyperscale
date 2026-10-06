"""Wire model ``StateSyncResponse`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from typing import TYPE_CHECKING
from dataclasses import dataclass
from .message import Message

if TYPE_CHECKING:
    from .gate_state_snapshot import GateStateSnapshot
    from .manager_state_snapshot import ManagerStateSnapshot
    from .worker_state_snapshot import WorkerStateSnapshot


@dataclass(slots=True)
class StateSyncResponse(Message):
    """
    Response to state sync request.

    The responder_ready field indicates whether the responder has completed
    its own startup and is ready to serve authoritative state. If False,
    the requester should retry after a delay.
    """

    responder_id: str  # Responding node
    current_version: int  # Current state version
    responder_ready: bool = True  # Whether responder has completed startup
    # One of these will be set based on node type
    worker_state: "WorkerStateSnapshot | None" = None
    manager_state: "ManagerStateSnapshot | None" = None
    gate_state: "GateStateSnapshot | None" = None
