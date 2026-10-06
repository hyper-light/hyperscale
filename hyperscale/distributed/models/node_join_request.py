"""Wire model ``NodeJoinRequest`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True)
class NodeJoinRequest(Message):
    """
    Operator instruction: join the cluster the target node belongs to.

    Sent by ``hyperscale join`` to the node that should join; the node
    runs its existing registration routine against the target, whose
    register endpoint performs isolation and protocol validation.
    """

    target_host: str  # TCP host of the node to join
    target_port: int  # TCP port of the node to join
