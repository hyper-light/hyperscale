from dataclasses import dataclass, field

from hyperscale.distributed.models.message import Message


@dataclass(slots=True)
class ClusterResizeReply(Message):
    """A membership group's answer to a resize (AD-52 ``ResizeCluster``):
    the cohort its log holds once the change committed (``host:port``
    each), or why it did not commit."""

    applied: bool
    cohort: list[str] = field(default_factory=list)
    leader_member_id: str | None = None
    refusal: str | None = None
