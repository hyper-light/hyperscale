from dataclasses import dataclass

MemberAddress = tuple[str, int]


@dataclass(slots=True, frozen=True)
class ClusterView:
    """A cluster's membership as of one applied index: who holds each
    address of the cohort, the cohort, the Raft configuration's voters and
    learners, and the cluster's mode."""

    cluster_uuid: str | None
    applied_index: int
    holders: dict[MemberAddress, str]
    cohort: frozenset[MemberAddress]
    voters: frozenset[str]
    learners: frozenset[str]
    mode: str | None


EMPTY_CLUSTER_VIEW = ClusterView(
    cluster_uuid=None,
    applied_index=0,
    holders={},
    cohort=frozenset(),
    voters=frozenset(),
    learners=frozenset(),
    mode=None,
)
