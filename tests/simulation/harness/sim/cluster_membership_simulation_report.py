from dataclasses import dataclass, field


@dataclass(slots=True)
class ClusterMembershipSimulationReport:
    """What a ``ClusterMembershipSimulation`` run observed: the safety
    violations it recorded, what the faults did, and the cohort's state
    when it ended."""

    violations: list[str] = field(default_factory=list)
    # (cluster uuid, term) -> the member that led it.
    leaders_by_term: dict[tuple[str, int], str] = field(default_factory=dict)
    # (cluster uuid, index) -> (term, command type, command) of every entry
    # any member held committed.
    committed_entries: dict[tuple[str, int], tuple[int, str, bytes]] = field(
        default_factory=dict
    )
    # Every cluster whose founding committed, in the order observed.
    clusters_formed: list[str] = field(default_factory=list)
    # Virtual instant the first founding was observed committed.
    first_formed_at: float | None = None
    crashes: int = 0
    partitions: int = 0
    one_way_partitions: int = 0
    connection_resets: int = 0
    # Participations left (abandoned foundings, stuck clusters left) across
    # every incarnation.
    groups_left: int = 0
    final_cluster_uuids: set[str] = field(default_factory=set)
    final_formations: dict[str, str] = field(default_factory=dict)
    final_leaders: list[str] = field(default_factory=list)
    final_voters: frozenset[str] = frozenset()
    final_learners: frozenset[str] = frozenset()
    final_is_joint: bool = False
    final_commit_indexes: dict[str, int] = field(default_factory=dict)
    live_member_ids: frozenset[str] = frozenset()
    # Departures after faults heal (AD-52 section 13): the member each drain
    # or force-remove released, or the refusal.
    drains: list[tuple[str, bool, str | None]] = field(default_factory=list)
    force_removals: list[tuple[str, bool, str | None]] = field(default_factory=list)
    # Force-removes of a member that still answered: each must be refused.
    live_removal_refusals: list[str | None] = field(default_factory=list)
    # A freeze (AD-52 section 13): each mode change's outcome, the voters a
    # leader held after a member died while frozen, and a drain's refusal.
    mode_changes: list[tuple[str, bool, str | None]] = field(default_factory=list)
    voters_while_frozen: frozenset[str] = frozenset()
    dead_member_while_frozen: str | None = None
    frozen_drain_refusal: str | None = None
    # Resizes (AD-52 ``ResizeCluster``): each resize's outcome, a second
    # resize's refusal before the members were relaunched, and the cohort
    # every live member holds at the end.
    resizes: list[tuple[str, bool, str | None]] = field(default_factory=list)
    early_resize_refusal: str | None = None
    final_cohorts: set[frozenset[tuple[str, int]]] = field(default_factory=set)
    # Linearizable status reads (AD-52 section 11) issued during faults:
    # how many were served, and how many were refused.
    status_reads_served: int = 0
    status_reads_refused: int = 0
    # Served reads a leader answered from its lease, without a round of
    # their own (only when the cluster runs leader leases).
    status_reads_served_by_lease: int = 0
    # A membership watch (AD-52 section 9) followed throughout: replies
    # served, snapshots among them, events applied, and whether the state it
    # rebuilt matched the leader's at the end.
    watch_replies: int = 0
    watch_snapshots: int = 0
    watch_events: int = 0
    watch_matches_final_state: bool = False
    # Each time the watch entered (True) or left (False) disconnected mode
    # (AD-52 section 10), with when.
    watch_connectivity_transitions: list[tuple[float, bool]] = field(default_factory=list)
    # Per live member at the end: the membership changes it counted as
    # applied, and how many snapshots it installed (AD-52 section 18).
    final_changes_applied: dict[str, dict[str, int]] = field(default_factory=dict)
    final_snapshots_installed: dict[str, int] = field(default_factory=dict)
    # Watches still counted open on any member once every member stopped.
    watches_open_after_stop: int = 0
