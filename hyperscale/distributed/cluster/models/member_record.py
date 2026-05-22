"""
MemberRecord — the canonical representation of a cluster member.

Stored in the membership state machine (the result of applying the
sequence of EnterJoint / LeaveJoint / AddLearner / Promote / Remove /
UpdateMetadata entries). Every node maintains its own copy as part of
the soft-state cache (AD-52 §10).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class MemberStatus(str, Enum):
    """
    AD-52 membership statuses.

    LEARNER  — non-voting, catching up (AD-52 §7).
    VOTER    — full member, counts toward quorum.
    DRAINING — voluntary leave in progress (AD-52 §13).
    SUSPECT  — SWIM has flagged this peer (AD-52 §8).
    DEAD     — SWIM has declared this peer dead; tombstone pending.
    REMOVED  — Raft has committed a Remove entry for this node.
    """

    LEARNER = "learner"
    VOTER = "voter"
    DRAINING = "draining"
    SUSPECT = "suspect"
    DEAD = "dead"
    REMOVED = "removed"


@dataclass(frozen=True, slots=True)
class MemberRecord:
    """
    Canonical per-member record. Frozen because mutations always go
    through the apply layer of the membership state machine — never
    in-place. The apply layer constructs a new MemberRecord with the
    updated fields and stores it.

    Fields:
        node_id              uuid4() string generated at process start
                             (AD-52 §3). Never persisted across restart.
        role                 "gate" | "manager" | "worker" — though
                             workers are not in any Raft group, this
                             field is reserved for federation use.
        status               Current lifecycle state — see MemberStatus.
        advertised_address   (host, port) tuple peers use to reach this
                             member. May be the headless DNS FQDN, the
                             pod IP, or an external NLB address.
        capabilities         Free-form metadata blob: AD-25 capabilities,
                             AD-37 role-aware bracket settings, drain
                             flags, etc. Keys are sorted on iteration
                             (AD-52 §15 determinism).
        joined_at_epoch      Membership epoch at which this record was
                             first committed (via AddLearner). Stable
                             across status transitions.
        promoted_at_epoch    Membership epoch at which this record was
                             promoted from LEARNER to VOTER, or 0 if
                             still learning.
        last_metadata_epoch  Last epoch at which metadata changed; used
                             to detect "stale UpdateMetadata" duplicates.
    """

    node_id: str
    role: str
    status: MemberStatus
    advertised_address: tuple[str, int]
    capabilities: tuple[tuple[str, str], ...] = field(default_factory=tuple)
    joined_at_epoch: int = 0
    promoted_at_epoch: int = 0
    last_metadata_epoch: int = 0

    def with_status(self, new_status: MemberStatus) -> "MemberRecord":
        return MemberRecord(
            node_id=self.node_id,
            role=self.role,
            status=new_status,
            advertised_address=self.advertised_address,
            capabilities=self.capabilities,
            joined_at_epoch=self.joined_at_epoch,
            promoted_at_epoch=self.promoted_at_epoch,
            last_metadata_epoch=self.last_metadata_epoch,
        )

    def with_promotion(self, promoted_at_epoch: int) -> "MemberRecord":
        return MemberRecord(
            node_id=self.node_id,
            role=self.role,
            status=MemberStatus.VOTER,
            advertised_address=self.advertised_address,
            capabilities=self.capabilities,
            joined_at_epoch=self.joined_at_epoch,
            promoted_at_epoch=promoted_at_epoch,
            last_metadata_epoch=self.last_metadata_epoch,
        )

    def with_metadata(
        self,
        capabilities: tuple[tuple[str, str], ...],
        last_metadata_epoch: int,
    ) -> "MemberRecord":
        return MemberRecord(
            node_id=self.node_id,
            role=self.role,
            status=self.status,
            advertised_address=self.advertised_address,
            capabilities=capabilities,
            joined_at_epoch=self.joined_at_epoch,
            promoted_at_epoch=self.promoted_at_epoch,
            last_metadata_epoch=last_metadata_epoch,
        )
