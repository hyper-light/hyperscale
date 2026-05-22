"""
MembershipState — the in-memory result of applying the sequence of
membership log entries (AD-52 §6).

Holds the current ClusterMetadata, the voter/learner sets, the full
MemberRecord table keyed by node_id, and the joint-consensus window
state (joint_old_voters, joint_new_voters) when a transition is in flight.

Mutated only by JointConsensusStateMachine.apply_entry(). Reads are
free-threaded as long as the caller has a consistent snapshot — we
never lock here because Raft apply is single-threaded by definition.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .models.cluster_metadata import ClusterMetadata
from .models.member_record import MemberRecord, MemberStatus


@dataclass(slots=True)
class MembershipState:
    """
    Fields:
        cluster_metadata     ClusterMetadata; AD-52 §3 + §6.
        members              All known members keyed by node_id. Includes
                             learners and voters; removed members are
                             evicted from this dict on Remove commit.
        joint_old_voters     During a joint-consensus window, the OLD
                             voter set. None outside a joint window.
        joint_new_voters     During a joint-consensus window, the NEW
                             voter set. None outside a joint window.
    """

    cluster_metadata: ClusterMetadata
    members: dict[str, MemberRecord] = field(default_factory=dict)
    joint_old_voters: frozenset[str] | None = None
    joint_new_voters: frozenset[str] | None = None

    @property
    def current_voters(self) -> frozenset[str]:
        """Set of node_ids currently in VOTER status."""
        return frozenset(
            node_id
            for node_id, record in self.members.items()
            if record.status == MemberStatus.VOTER
        )

    @property
    def current_learners(self) -> frozenset[str]:
        return frozenset(
            node_id
            for node_id, record in self.members.items()
            if record.status == MemberStatus.LEARNER
        )

    @property
    def in_joint_consensus(self) -> bool:
        return self.joint_old_voters is not None and self.joint_new_voters is not None

    def quorum_size(self) -> int:
        """
        Number of votes required to commit a Raft entry (AD-3, AD-52 §6).

        Outside a joint window: floor(cluster_size / 2) + 1 over the
        configured cluster_size — never the live voter count. This is
        the AD-3 split-brain guard: a partition with fewer than
        majority-of-configured cannot make progress.

        Inside a joint window: callers must use majority_of_both()
        instead; see quorum_satisfied() below for the boolean check.
        """
        configured = self.cluster_metadata.cluster_size
        if configured <= 0:
            return 1
        return (configured // 2) + 1

    def quorum_satisfied(self, acknowledging_node_ids: frozenset[str]) -> bool:
        """
        True if the acknowledging set is a quorum.

        Outside joint: a majority of cluster_size (AD-3).
        Inside joint:  majority of joint_old_voters AND majority of
                       joint_new_voters (AD-52 §6).
        """
        if not self.in_joint_consensus:
            return len(acknowledging_node_ids) >= self.quorum_size()

        old_voters = self.joint_old_voters or frozenset()
        new_voters = self.joint_new_voters or frozenset()
        old_majority = (len(old_voters) // 2) + 1
        new_majority = (len(new_voters) // 2) + 1
        acks_in_old = len(acknowledging_node_ids & old_voters)
        acks_in_new = len(acknowledging_node_ids & new_voters)
        return acks_in_old >= old_majority and acks_in_new >= new_majority

    def is_member(self, node_id: str) -> bool:
        return node_id in self.members

    def get_member(self, node_id: str) -> MemberRecord | None:
        return self.members.get(node_id)
