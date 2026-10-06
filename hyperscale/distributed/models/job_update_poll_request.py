"""Wire model ``JobUpdatePollRequest`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True)
class JobUpdatePollRequest(Message):
    """
    Request for job updates since a sequence.
    """

    job_id: str
    last_sequence: int = 0
