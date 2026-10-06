from dataclasses import dataclass, field

from hyperscale.distributed.models.message import Message


@dataclass(slots=True)
class ClusterStatusReply(Message):
    """A cluster's membership as its leader held it having applied through
    ``read_index`` -- at least the ReadIndex it confirmed, so linearizable:
    no change committed before the request was made is missing from it
    (AD-52 section 11) -- or why it could not be served.
    Addresses are ``host:port``."""

    served: bool
    cluster_uuid: str | None = None
    leader_member_id: str | None = None
    voters: list[str] = field(default_factory=list)
    learners: list[str] = field(default_factory=list)
    cohort: list[str] = field(default_factory=list)
    holders: list[str] = field(default_factory=list)
    mode: str | None = None
    read_index: int = 0
    refusal: str | None = None
