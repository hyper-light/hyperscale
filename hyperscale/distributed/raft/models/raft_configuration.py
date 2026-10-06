"""
A Raft group's membership configuration (AD-52 sections 6-7).
"""

from __future__ import annotations

import json
from collections.abc import Set
from dataclasses import dataclass, field

# The command type of a log entry that carries a ``RaftConfiguration``.
# Raft consumes these entries itself; they never reach the state machine.
RAFT_CONFIGURATION_COMMAND = "raft_configuration"


@dataclass(slots=True, frozen=True)
class RaftConfiguration:
    """
    Who votes in a Raft group, and who only follows its log.

    ``voters`` elect leaders and form commit quorums. ``learners`` receive
    the log but never vote and never count toward a quorum: a member joins
    as a learner, catches up, and only then is promoted (AD-52 section 7).

    A change of voters passes through a joint configuration (Raft section
    6, AD-52 section 6): ``outgoing_voters`` holds the configuration being
    left, and every election and commit needs a quorum of BOTH voter sets,
    so no minority can complete a change on its own.

    Each member acts on the latest configuration in its log, committed or
    not; a configuration entry takes effect the moment it is appended.

    ``ordered_members`` and ``ordered_all_voters`` are the same members in
    one order, fixed when the configuration is made: a member sends to them
    in that order, so what it does never follows a set's iteration order
    -- which differs from process to process with Python's hash seed.
    """

    voters: frozenset[str]
    learners: frozenset[str] = frozenset()
    outgoing_voters: frozenset[str] | None = None
    ordered_members: tuple[str, ...] = field(init=False, compare=False, repr=False)
    ordered_all_voters: tuple[str, ...] = field(init=False, compare=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "ordered_members", tuple(sorted(self.members)))
        object.__setattr__(self, "ordered_all_voters", tuple(sorted(self.all_voters)))

    @property
    def is_joint(self) -> bool:
        return self.outgoing_voters is not None

    @property
    def all_voters(self) -> frozenset[str]:
        """Every member whose vote counts in some quorum of this configuration."""
        if self.outgoing_voters is None:
            return self.voters
        return self.voters | self.outgoing_voters

    @property
    def members(self) -> frozenset[str]:
        """Every member that receives the log: voters (both sets while
        joint) and learners."""
        return self.all_voters | self.learners

    def has_quorum(self, holders: Set[str], quorum_floor: int) -> bool:
        """Whether ``holders`` form a quorum: in each voter set (both while
        joint), at least a majority of that set and at least
        ``quorum_floor``. The floor keeps a group from committing with fewer
        than a majority of its cohort's configured size (AD-3); a set too
        small to meet it has no quorum at all."""
        if len(self.voters & holders) < max(len(self.voters) // 2 + 1, quorum_floor):
            return False
        return self.outgoing_voters is None or len(self.outgoing_voters & holders) >= max(
            len(self.outgoing_voters) // 2 + 1, quorum_floor
        )

    def dump(self) -> bytes:
        """Deterministic encoding for a log entry's command: identical on
        every member that appends it."""
        return json.dumps(
            {
                "voters": sorted(self.voters),
                "learners": sorted(self.learners),
                "outgoing_voters": (
                    None if self.outgoing_voters is None else sorted(self.outgoing_voters)
                ),
            },
            separators=(",", ":"),
        ).encode()

    @classmethod
    def load(cls, encoded: bytes) -> RaftConfiguration:
        decoded = json.loads(encoded)
        outgoing_voters = decoded["outgoing_voters"]
        return cls(
            voters=frozenset(decoded["voters"]),
            learners=frozenset(decoded["learners"]),
            outgoing_voters=None if outgoing_voters is None else frozenset(outgoing_voters),
        )
