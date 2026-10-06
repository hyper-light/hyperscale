from dataclasses import dataclass, field

from hyperscale.distributed.models.message import Message


@dataclass(slots=True)
class ClusterWatchReply(Message):
    """A member's answer to a watch (AD-52 section 9): the cluster and the
    index the answer reaches (the next watch resumes after it), and either
    a snapshot of the membership at that index (``snapshot``) or the
    changes after the watcher's index, in commit order -- each ``(index,
    kind, detail)``: ``claim``/``release`` (a member id), ``mode``,
    ``resize`` (the cohort -- an address it no longer holds is released
    with it), ``configuration`` (voters and learners).
    ``refusal`` says why there is nothing to watch."""

    served: bool
    cluster_uuid: str | None = None
    applied_index: int = 0
    snapshot: bool = False
    holders: list[str] = field(default_factory=list)
    mode: str | None = None
    cohort: list[str] = field(default_factory=list)
    voters: list[str] = field(default_factory=list)
    learners: list[str] = field(default_factory=list)
    events: list[tuple[int, str, str]] = field(default_factory=list)
    refusal: str | None = None
