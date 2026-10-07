"""Text formatting shared by the node dashboards' readers."""

from collections import Counter

from hyperscale.distributed.cluster.cluster_membership import FORMATION_FORMED, ClusterMembership
from hyperscale.distributed.cluster.models import ClusterMemberId
from hyperscale.ui.components.meter import MeterReading
from hyperscale.ui.components.status_badge import StatusBadgeReading

from .status_tones import joined_label

# A formed cluster member that is not the leader follows it; before the
# cluster forms, the formation stage itself is the member's role.
CLUSTER_ROLE_BY_FORMATION: dict[str, str] = {"formed": "follower"}
# A reading the node has no value for this sample, as the tables show a
# cell they have no value for.
NO_READING = "-"


def format_reading(value: float | None) -> str:
    """A reading as the dashboard shows it: to the tables' precision of
    one decimal, or ``NO_READING`` when there is none."""
    if value is None:
        return NO_READING

    return f"{value:.1f}"


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


def cluster_lines(membership: ClusterMembership, node_lines: list[str]) -> list[str]:
    """The cluster panel of a manager or gate: its membership role, the
    leader, the voter/learner/cohort counts, then ``node_lines`` (the
    node's SWIM view and local health)."""
    voter_count = len(membership.voters)
    return [
        f"CLUSTER {describe_cluster_role(membership)}",
        f"leader {describe_leader(membership)}",
        f"voters {voter_count} learners {len(membership.members) - voter_count}",
        f"cohort {len(membership.cohort)} {membership.formation}",
        *node_lines,
    ]


def in_use_percent(total_cores: int, free_cores: int) -> float:
    """The share of ``total_cores`` not free, in percent (0 with no cores)."""
    if total_cores <= 0:
        return 0.0

    return (total_cores - free_cores) * 100.0 / total_cores


def count_statuses(status_counts: Counter[str], statuses: tuple[str, ...]) -> int:
    """How many of the counted items are in any of ``statuses``."""
    return sum(map(status_counts.__getitem__, statuses))


def format_rate(value: float | None, unit: str) -> str:
    """A rate as a chart's legend reads it: ``59.9/s``, or ``NO_READING``."""
    if value is None:
        return NO_READING

    return f"{value:.1f}{unit}"


def format_milliseconds(value: float | None) -> str:
    """A latency in milliseconds: ``50.0 ms``, or ``NO_READING``."""
    if value is None:
        return NO_READING

    return f"{value:.1f} ms"


def leader_badge(membership: ClusterMembership) -> StatusBadgeReading:
    """The cluster leader as a badge -- in trouble while none is known --
    with the formation stage while the cluster has not formed."""
    stage = [] if membership.formation == FORMATION_FORMED else [membership.formation]
    return StatusBadgeReading(
        joined_label([f"leader {describe_leader(membership)}", *stage]),
        "ok" if membership.leader_member_id is not None else "failing",
    )


def cohort_badge(membership: ClusterMembership) -> StatusBadgeReading:
    """The node's membership as a badge: its role, the voters among the
    cohort and the formation stage -- worth a look until formed."""
    return StatusBadgeReading(
        f"{describe_cluster_role(membership)} {len(membership.voters)}/{len(membership.cohort)} voters",
        "ok" if membership.formation == FORMATION_FORMED else "degraded",
    )


def cores_meter(used_cores: int, total_cores: int) -> MeterReading:
    """Cores in use of all cores, as a meter labelled ``used/total``."""
    return MeterReading(used=used_cores, total=total_cores, label=f"{used_cores}/{total_cores}")
