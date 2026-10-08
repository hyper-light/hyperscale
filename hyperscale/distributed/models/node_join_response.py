"""Wire model ``NodeJoinResponse`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from typing import TYPE_CHECKING
from dataclasses import dataclass
from .message import Message

if TYPE_CHECKING:
    from .node_join_request import NodeJoinRequest


@dataclass(slots=True)
class NodeJoinResponse(Message):
    """
    Outcome of a ``NodeJoinRequest``.
    """

    accepted: bool  # Whether the join completed
    node_id: str  # Joining node ID
    node_role: str  # Joining node's NodeRole value
    error: str | None = None  # Reason when not accepted
