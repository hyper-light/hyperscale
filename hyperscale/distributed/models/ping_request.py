"""Wire model ``PingRequest`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True)
class PingRequest(Message):
    """
    Ping request from client to manager or gate.

    Used for health checking and status retrieval without
    submitting a job. Returns current node state.
    """

    request_id: str  # Unique request identifier
