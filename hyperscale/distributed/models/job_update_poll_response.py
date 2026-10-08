"""Wire model ``JobUpdatePollResponse`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from typing import TYPE_CHECKING
from dataclasses import dataclass, field
from .message import Message

if TYPE_CHECKING:
    from .job_update_record import JobUpdateRecord


@dataclass(slots=True)
class JobUpdatePollResponse(Message):
    """
    Response containing queued job updates for a client.
    """

    job_id: str
    updates: list["JobUpdateRecord"] = field(default_factory=list)
    latest_sequence: int = 0
    truncated: bool = False
    oldest_sequence: int = 0
