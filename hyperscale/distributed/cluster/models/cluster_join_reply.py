from dataclasses import dataclass, field

from hyperscale.distributed.models.message import Message


@dataclass(slots=True)
class ClusterJoinReply(Message):
    """A membership group's answer to a join (AD-52).

    Accepted, it names the cluster and the voters its group was founded
    with -- the joiner creates its group with them, as every member did.
    Otherwise ``leader_member_id`` points at the group's leader when this
    member is not it, and ``refusal`` says why the join was refused.
    """

    accepted: bool
    cluster_uuid: str | None = None
    founding_voters: list[str] = field(default_factory=list)
    leader_member_id: str | None = None
    refusal: str | None = None
