from dataclasses import dataclass, field

from hyperscale.distributed.models.message import Message


@dataclass(slots=True)
class ClusterMetricsReply(Message):
    """One member's metrics of its cluster's membership (AD-52 section 18):
    where it stands, the membership as it applied it, its Raft group's
    term, indexes and counters (``raft``; per-follower replication lag in
    entries while it leads), and what it saw happen since it started --
    changes applied by kind, foundings it proposed, groups it left,
    operator requests by kind and outcome, watches it holds open now.

    A gate adds its AD-45 route learning per datacenter
    (``route_learning``): the observed latency, the blended estimate
    routing uses, the confidence in the observations and their count; its
    AD-36 routing counters (``routing``, ``GateJobRouter.get_metrics``);
    and its AD-52 section 10 watch of each datacenter's managers
    (``datacenter_watches``): the view's staleness in seconds, whether the
    watch is disconnected (1.0) or not (0.0), and the index it applied."""

    member_id: str
    formation: str
    is_leader: bool
    cluster_uuid: str | None = None
    mode: str | None = None
    cohort_size: int = 0
    voters: int = 0
    learners: int = 0
    holders: int = 0
    raft: dict[str, int] = field(default_factory=dict)
    follower_lag: dict[str, int] = field(default_factory=dict)
    changes_applied: dict[str, int] = field(default_factory=dict)
    foundings_proposed: int = 0
    groups_left: int = 0
    operator_requests: dict[str, int] = field(default_factory=dict)
    open_watches: int = 0
    route_learning: dict[str, dict[str, float]] = field(default_factory=dict)
    routing: dict[str, int] = field(default_factory=dict)
    datacenter_watches: dict[str, dict[str, float]] = field(default_factory=dict)
