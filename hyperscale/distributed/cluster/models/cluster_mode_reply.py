from dataclasses import dataclass

from hyperscale.distributed.models.message import Message


@dataclass(slots=True)
class ClusterModeReply(Message):
    """A membership group's answer to a mode change (AD-52 section 13): the
    mode its log holds once the change committed, or why it did not."""

    applied: bool
    mode: str | None = None
    leader_member_id: str | None = None
    refusal: str | None = None
