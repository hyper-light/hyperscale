"""Text formatting shared by the node dashboards' readers."""

from collections import Counter

from hyperscale.distributed.cluster.cluster_membership import ClusterMembership
from hyperscale.distributed.cluster.models import ClusterMemberId

# A formed cluster member that is not the leader follows it; before the
# cluster forms, the formation stage itself is the member's role.
CLUSTER_ROLE_BY_FORMATION: dict[str, str] = {"formed": "follower"}


def format_duration(total_seconds: float) -> str:
    """``total_seconds`` as hours, minutes and whole seconds: ``1h02m03s``."""
    minutes, seconds = divmod(int(total_seconds), 60)
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h{minutes:02d}m{seconds:02d}s"


def describe_cluster_role(membership: ClusterMembership) -> str:
    """The node's place in its cluster: standalone (a cohort of itself),
    leader, follower, or the formation stage it has reached."""
    if len(membership.cohort) <= 1:
        return "standalone"

    if membership.is_leader():
        return "leader"

    return CLUSTER_ROLE_BY_FORMATION.get(membership.formation, membership.formation)


def describe_leader(membership: ClusterMembership) -> str:
    """The cluster leader's TCP address as this node knows it, or ``none``
    while no leader is known."""
    if (leader_member_id := membership.leader_member_id) is None:
        return "none"

    leader = ClusterMemberId.parse(leader_member_id)
    return f"{leader.host}:{leader.port}"


def cluster_lines(membership: ClusterMembership, swim_lines: list[str]) -> list[str]:
    """The cluster panel of a manager or gate: its membership role, the
    leader, the voter/learner/cohort counts, and the SWIM view."""
    voter_count = len(membership.voters)
    return [
        f"CLUSTER {describe_cluster_role(membership)}",
        f"leader {describe_leader(membership)}",
        f"voters {voter_count} learners {len(membership.members) - voter_count}",
        f"cohort {len(membership.cohort)} {membership.formation}",
        *swim_lines,
    ]


def in_use_percent(total_cores: int, free_cores: int) -> float:
    """The share of ``total_cores`` not free, in percent (0 with no cores)."""
    if total_cores <= 0:
        return 0.0

    return (total_cores - free_cores) * 100.0 / total_cores


def count_statuses(status_counts: Counter[str], statuses: tuple[str, ...]) -> int:
    """How many of the counted items are in any of ``statuses``."""
    return sum(map(status_counts.__getitem__, statuses))
