"""Wire model ``JobProgressAck`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message
from .gate_info import GateInfo


@dataclass(slots=True, kw_only=True)
class JobProgressAck(Message):
    """
    Acknowledgment for job progress updates from gates to managers.

    Includes updated gate list so managers can maintain
    accurate view of gate cluster topology and leadership.
    """

    gate_id: str  # Responding gate's node_id
    is_leader: bool  # Whether this gate is leader
    healthy_gates: list[GateInfo]  # Current healthy gates
