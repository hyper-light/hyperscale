"""
JointConsensusQuorum — Raft-side quorum calculator that honors the
AD-52 §6 joint-consensus rule:

  Outside a joint window: standard majority of the configured cluster_size.
  Inside  a joint window: majority of OLD voters AND majority of NEW voters.

The cluster's JointConsensusStateMachine owns the membership table; this
class is a thin adapter the Raft replication loop consults on every
commit-index advance to know whether an ack count constitutes a commit.

Composition over inheritance (AD-1): RaftNode does NOT inherit from this.
RaftNode holds a reference and calls quorum_satisfied() at commit time.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from hyperscale.distributed.cluster.membership_state import MembershipState


class JointConsensusQuorum:
    """
    Thin façade over MembershipState's quorum_satisfied / quorum_size.

    Holds a reference (not a copy) so the Raft layer always sees the
    latest state. The reference is mutable from the cluster apply loop
    but Raft commit advancement is also single-threaded by Raft's own
    contract, so there is no torn-read concern at the call site.
    """

    __slots__ = ("_membership_state_provider",)

    def __init__(
        self,
        membership_state_provider: "callable[[], MembershipState]",
    ) -> None:
        """
        Args:
          membership_state_provider: zero-arg callable returning the
            current MembershipState. We do not store the state directly
            because the cluster apply layer replaces it as new entries
            commit.
        """
        self._membership_state_provider = membership_state_provider

    def quorum_size(self) -> int:
        """Outside-joint quorum size (AD-3 split-brain guard)."""
        return self._membership_state_provider().quorum_size()

    def quorum_satisfied(self, acknowledging_node_ids: frozenset[str]) -> bool:
        """True if the ack set is a quorum — accounts for joint window."""
        return self._membership_state_provider().quorum_satisfied(
            acknowledging_node_ids
        )

    def in_joint_consensus(self) -> bool:
        return self._membership_state_provider().in_joint_consensus
