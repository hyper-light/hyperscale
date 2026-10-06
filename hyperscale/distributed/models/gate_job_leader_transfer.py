"""Wire model ``GateJobLeaderTransfer`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True)
class GateJobLeaderTransfer(Message):
    """
    Notification to client that gate job leadership has transferred (Section 9.1.2).

    Sent from new gate leader to client when taking over job leadership.
    """

    job_id: str
    new_gate_id: str
    new_gate_addr: tuple[str, int]
    fence_token: int
    old_gate_id: str | None = None
    old_gate_addr: tuple[str, int] | None = None
