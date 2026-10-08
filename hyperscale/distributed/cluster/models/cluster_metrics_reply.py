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
    watch is disconnected (1.0) or not (0.0), and the index it applied.

    AD-44: a manager adds, per job whose retry budget it holds, the retries
    consumed (``retry_budget_consumed``) and those refused for a spent
    budget (``retry_budget_exhausted``); a gate adds its best-effort
    completions by reason (``best_effort_completions``), the completion
    ratio of each best-effort job it still holds
    (``best_effort_completion_ratio``) and its late datacenter results by
    outcome (``best_effort_late_results``: ``logged`` / ``updated``).

    D-68, the telemetry every role answers with in this one schema -- a
    worker too, which holds no membership (``formation`` empty, every
    membership field at its default):

    * ``role``: ``gate``, ``manager`` or ``worker`` (empty from a node
      older than this schema); ``node_state``: its lifecycle state, the
      value its heartbeats carry.
    * ``capacity``: ``total_cores`` and ``available_cores`` (a manager's
      over its healthy workers, plus ``workers`` and ``healthy_workers``).
    * ``workload``: ``active_jobs`` (gate, manager), ``active_workflows``
      and ``pending_workflows`` (manager, worker).
    * ``resources``: a worker's ``cpu_percent`` and ``memory_percent``.
    * ``dispatch_throughput``: a manager's AD-19 dispatches per second,
      ``observed`` and ``expected``; ``dispatch_outcomes``: its dispatch
      sends by outcome (``DispatchOutcome`` values) since it started.
    * ``dispatch_latency``: a manager's AD-42 dispatch round trips per
      worker (``p50_ms``, ``p95_ms``, ``p99_ms``, ``sample_count``) over
      the SLO windows.
    * ``slo``: AD-42 latency SLO per datacenter (``p50_ms``, ``p95_ms``,
      ``p99_ms``, ``sample_count``, ``compliance_score``,
      ``routing_factor``): a manager's own, a gate's view of each
      datacenter (what its health classification and routing read)."""

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
    retry_budget_consumed: dict[str, int] = field(default_factory=dict)
    retry_budget_exhausted: dict[str, int] = field(default_factory=dict)
    best_effort_completions: dict[str, int] = field(default_factory=dict)
    best_effort_completion_ratio: dict[str, float] = field(default_factory=dict)
    best_effort_late_results: dict[str, int] = field(default_factory=dict)
    role: str = ""
    node_state: str = ""
    capacity: dict[str, int] = field(default_factory=dict)
    workload: dict[str, int] = field(default_factory=dict)
    resources: dict[str, float] = field(default_factory=dict)
    dispatch_throughput: dict[str, float] = field(default_factory=dict)
    dispatch_outcomes: dict[str, int] = field(default_factory=dict)
    dispatch_latency: dict[str, dict[str, float]] = field(default_factory=dict)
    slo: dict[str, dict[str, float]] = field(default_factory=dict)
