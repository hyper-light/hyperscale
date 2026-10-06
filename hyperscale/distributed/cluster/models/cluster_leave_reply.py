from dataclasses import dataclass

from hyperscale.distributed.models.message import Message


@dataclass(slots=True)
class ClusterLeaveReply(Message):
    """A membership group's answer to a leave (AD-52 section 13): the member
    whose address the group's log released, or why nothing was -- with the
    group's leader when this member is not it, for the caller to ask."""

    released: bool
    released_member_id: str | None = None
    leader_member_id: str | None = None
    refusal: str | None = None
