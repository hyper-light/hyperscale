"""Wire model ``WorkflowFinalResultAck`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True, kw_only=True)
class WorkflowFinalResultAck(Message):
    """
    Acknowledgment for workflow final-result delivery.

    Followers return ``leader_addr`` when they cannot apply the result locally.
    Stale and duplicate results are acknowledged so workers do not retry
    obsolete terminal states forever.
    """

    accepted: bool
    manager_id: str = ""
    is_leader: bool = False
    forwarded: bool = False
    duplicate: bool = False
    stale: bool = False
    leader_addr: tuple[str, int] | None = None
    error: str | None = None
    reason: str = ""
