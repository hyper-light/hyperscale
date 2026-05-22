"""
ClusterMetadata — the cluster-level state that survives every membership
transition: cluster_uuid (AD-52 §3, §6), configured cluster_size, freeze
flag, current membership epoch, last membership LSN.

Maintained inside the membership state machine. Updated via apply of
log entries; never mutated in-place.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ClusterMetadata:
    """
    Fields:
        cluster_uuid           Bootstrap-minted uuid4 (AD-52 §3). Immutable
                               for the lifetime of the cluster.
        cluster_id             Operator-supplied human-readable id
                               (--cluster-id). Distinct from cluster_uuid.
        cluster_size           Configured target size (AD-52 §3). Used by
                               quorum calculations even when current
                               membership is smaller (split-brain guard).
        membership_epoch       Monotonic counter incremented per committed
                               membership change. Used in ClusterRPCFence
                               (AD-52 §6).
        last_membership_lsn    Raft log LSN of the most recent committed
                               membership entry. Used in ClusterRPCFence
                               and watch-stream reconnect.
        freeze_active          AD-52 §13 freeze flag; when True, all
                               membership-change proposals are rejected
                               with FROZEN.
        joint_consensus_active True between EnterJoint commit and
                               LeaveJoint commit. While active, every
                               commit requires majorities of BOTH the
                               old and new configurations.
    """

    cluster_uuid: str
    cluster_id: str
    cluster_size: int
    membership_epoch: int = 0
    last_membership_lsn: int = 0
    freeze_active: bool = False
    joint_consensus_active: bool = False
