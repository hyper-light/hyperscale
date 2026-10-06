from dataclasses import dataclass, field


@dataclass(slots=True)
class RaftGroupSimulationReport:
    """What a ``RaftGroupSimulation`` run observed: the safety violations it
    recorded, and the group's state when it ended."""

    violations: list[str] = field(default_factory=list)
    leaders_by_term: dict[int, str] = field(default_factory=dict)
    # Index -> (term, command) of every entry any member applied.
    committed_entries: dict[int, tuple[int, bytes]] = field(default_factory=dict)
    proposals_committed: int = 0
    # The command of every proposal its proposer saw committed.
    acknowledged_commands: list[bytes] = field(default_factory=list)
    # Restarts that resumed from their disk, and crashes of every member.
    resumptions: int = 0
    power_losses: int = 0
    crashes_after_reply: int = 0
    configuration_changes_started: int = 0
    crashes: int = 0
    partitions: int = 0
    final_leaders: list[str] = field(default_factory=list)
    final_commit_indexes: dict[str, int] = field(default_factory=dict)
    final_voters: frozenset[str] = frozenset()
    final_learners: frozenset[str] = frozenset()
    live_member_ids: frozenset[str] = frozenset()
