"""Wire model ``JobUpdateRecord`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True)
class JobUpdateRecord(Message):
    """
    Record of a client update for replay/polling.
    """

    sequence: int
    message_type: str
    payload: bytes
    timestamp: float
