from dataclasses import dataclass, field

from hyperscale.distributed.models.message import Message


@dataclass(slots=True)
class ClusterHelloReply(Message):
    """Where a node stands in its cluster's formation (AD-52).

    ``member_id`` is the process answering at the greeted address right
    now. ``formation`` is ``discovering`` (it holds no membership group),
    ``forming`` (it holds a founding that has not committed), ``joining``
    (it was taken into a formed cluster and is catching up) or ``formed``.
    ``cluster_uuid`` and ``founding_voters`` name the group it holds, and
    ``leader_member_id`` the group's leader as far as it knows.
    ``discovering_seen`` is how many discovering founders, itself among
    them, its last formation round heard from (0 before its first round).
    ``refusal`` is set when the greeting was refused (a different founding
    cohort). ``configured_digest`` is the cohort the answering process was
    launched with -- a resize waits until every answering voter was
    relaunched with the cohort the cluster holds.
    """

    member_id: str
    formation: str
    cluster_uuid: str | None = None
    founding_voters: list[str] = field(default_factory=list)
    leader_member_id: str | None = None
    discovering_seen: int = 0
    refusal: str | None = None
    configured_digest: str = ""
