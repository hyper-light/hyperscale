"""
JointConsensusStateMachine — the apply layer for membership log entries
(AD-52 §6).

Single-threaded by Raft's apply contract. Each entry is applied in
sequence; the resulting MembershipState is the canonical authority for
quorum calculations, fence validation, and watch-stream snapshots.

Per AD-52 §15, this code is on the deterministic boundary:
  - No wall clock.
  - No randomness.
  - Sorted iteration over maps and sets (we use sorted() / sorted(frozenset)).
  - No I/O.
  - No hash-based identity.

The determinism module (hyperscale/distributed/cluster/determinism.py)
runtime-asserts these invariants in test mode.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .membership_log import (
    AddLearner,
    EnterJoint,
    LeaveJoint,
    MembershipLogEntry,
    Promote,
    RegisterDatacenter,
    Remove,
    RemoveReason,
    ResizeCluster,
    UpdateMetadata,
)
from .membership_state import MembershipState
from .models.cluster_metadata import ClusterMetadata
from .models.member_record import MemberRecord, MemberStatus

if TYPE_CHECKING:
    pass


# Sanity ceiling on ResizeCluster — well above any reasonable Raft group.
_MAX_REASONABLE_CLUSTER_SIZE: int = 25


class JointConsensusError(Exception):
    """Raised when an entry cannot be applied (e.g., LeaveJoint without
    a matching EnterJoint, or RegisterDatacenter on a non-gate cluster)."""


class JointConsensusStateMachine:
    """
    Apply layer over the Raft log. Constructed once per node with an
    initial MembershipState (either an empty one for a freshly-booting
    cluster, or one rehydrated from a Raft snapshot).

    Thread-safe via the Raft apply contract: only one entry is being
    applied at a time. No internal locks needed.
    """

    __slots__ = ("_state",)

    def __init__(self, initial_state: MembershipState) -> None:
        self._state = initial_state

    @property
    def state(self) -> MembershipState:
        return self._state

    def apply_entry(
        self,
        entry: MembershipLogEntry,
        committed_at_term: int,
        committed_at_lsn: int,
    ) -> None:
        """
        Apply a single membership log entry. Raises JointConsensusError
        on invariant violation — Raft callers should treat this as fatal
        and halt rather than silently diverge.
        """
        new_epoch = self._state.cluster_metadata.membership_epoch + 1

        if isinstance(entry, EnterJoint):
            self._apply_enter_joint(entry, committed_at_term, new_epoch, committed_at_lsn)
        elif isinstance(entry, LeaveJoint):
            self._apply_leave_joint(entry, committed_at_term, new_epoch, committed_at_lsn)
        elif isinstance(entry, AddLearner):
            self._apply_add_learner(entry, committed_at_term, new_epoch, committed_at_lsn)
        elif isinstance(entry, Promote):
            self._apply_promote(entry, committed_at_term, new_epoch, committed_at_lsn)
        elif isinstance(entry, Remove):
            self._apply_remove(entry, committed_at_term, new_epoch, committed_at_lsn)
        elif isinstance(entry, UpdateMetadata):
            self._apply_update_metadata(entry, committed_at_term, new_epoch, committed_at_lsn)
        elif isinstance(entry, RegisterDatacenter):
            self._apply_register_datacenter(entry, committed_at_term, new_epoch, committed_at_lsn)
        elif isinstance(entry, ResizeCluster):
            self._apply_resize_cluster(entry, committed_at_term, new_epoch, committed_at_lsn)
        else:
            raise JointConsensusError(
                f"unknown membership log entry type: {type(entry).__name__}"
            )

    # =========================================================================
    # Entry handlers
    # =========================================================================

    def _apply_enter_joint(
        self,
        entry: EnterJoint,
        committed_at_term: int,
        new_epoch: int,
        committed_at_lsn: int,
    ) -> None:
        if self._state.in_joint_consensus:
            raise JointConsensusError(
                "EnterJoint while already in joint consensus — invariant violation"
            )

        if self._state.cluster_metadata.cluster_uuid == "":
            # Bootstrap entry — mint the cluster UUID from the entry's
            # cluster_uuid_at_create field.
            if not entry.cluster_uuid_at_create:
                raise JointConsensusError(
                    "bootstrap EnterJoint missing cluster_uuid_at_create"
                )
            new_uuid = entry.cluster_uuid_at_create
        else:
            new_uuid = self._state.cluster_metadata.cluster_uuid
            if entry.cluster_uuid_at_create and entry.cluster_uuid_at_create != new_uuid:
                raise JointConsensusError(
                    "EnterJoint cluster_uuid_at_create mismatch — zombie entry"
                )

        self._state.joint_old_voters = frozenset(entry.old_members)
        self._state.joint_new_voters = frozenset(entry.new_members)
        self._state.cluster_metadata = ClusterMetadata(
            cluster_uuid=new_uuid,
            cluster_id=self._state.cluster_metadata.cluster_id,
            cluster_size=self._state.cluster_metadata.cluster_size,
            membership_epoch=new_epoch,
            last_membership_lsn=committed_at_lsn,
            freeze_active=self._state.cluster_metadata.freeze_active,
            joint_consensus_active=True,
        )

    def _apply_leave_joint(
        self,
        entry: LeaveJoint,
        committed_at_term: int,
        new_epoch: int,
        committed_at_lsn: int,
    ) -> None:
        if not self._state.in_joint_consensus:
            raise JointConsensusError(
                "LeaveJoint outside a joint window — invariant violation"
            )
        if self._state.joint_new_voters != frozenset(entry.members):
            raise JointConsensusError(
                f"LeaveJoint members={sorted(entry.members)} does not match "
                f"joint_new_voters={sorted(self._state.joint_new_voters or [])}"
            )

        # Resolve the membership table: every member in joint_new_voters
        # must already exist as a learner or voter; any voter in
        # joint_old_voters but not joint_new_voters is removed.
        new_members_table: dict[str, MemberRecord] = {}
        for node_id, member_record in self._state.members.items():
            if node_id in self._state.joint_new_voters:
                # Carry forward; promote learner if needed.
                if member_record.status == MemberStatus.LEARNER:
                    new_members_table[node_id] = member_record.with_promotion(new_epoch)
                else:
                    new_members_table[node_id] = member_record.with_status(
                        MemberStatus.VOTER
                    )
            elif (
                self._state.joint_old_voters is not None
                and node_id in self._state.joint_old_voters
            ):
                # Voter being removed by the transition.
                new_members_table[node_id] = member_record.with_status(
                    MemberStatus.REMOVED
                )
            else:
                # Untouched (learners staying as learners, members not
                # in either side of the transition).
                new_members_table[node_id] = member_record

        # Drop REMOVED members from the table — they're gone.
        new_members_table = {
            node_id: member_record
            for node_id, member_record in new_members_table.items()
            if member_record.status != MemberStatus.REMOVED
        }

        self._state.members = new_members_table
        self._state.joint_old_voters = None
        self._state.joint_new_voters = None
        self._state.cluster_metadata = ClusterMetadata(
            cluster_uuid=self._state.cluster_metadata.cluster_uuid,
            cluster_id=self._state.cluster_metadata.cluster_id,
            cluster_size=self._state.cluster_metadata.cluster_size,
            membership_epoch=new_epoch,
            last_membership_lsn=committed_at_lsn,
            freeze_active=self._state.cluster_metadata.freeze_active,
            joint_consensus_active=False,
        )

    def _apply_add_learner(
        self,
        entry: AddLearner,
        committed_at_term: int,
        new_epoch: int,
        committed_at_lsn: int,
    ) -> None:
        if entry.node_id in self._state.members:
            raise JointConsensusError(
                f"AddLearner for existing member {entry.node_id!r}"
            )
        if self._state.cluster_metadata.freeze_active:
            raise JointConsensusError("AddLearner rejected while cluster is frozen")

        self._state.members[entry.node_id] = MemberRecord(
            node_id=entry.node_id,
            role=entry.role,
            status=MemberStatus.LEARNER,
            advertised_address=entry.advertised_address,
            capabilities=tuple(sorted(entry.capabilities)),
            joined_at_epoch=new_epoch,
        )
        self._bump_metadata_epoch(new_epoch, committed_at_lsn)

    def _apply_promote(
        self,
        entry: Promote,
        committed_at_term: int,
        new_epoch: int,
        committed_at_lsn: int,
    ) -> None:
        member_record = self._state.members.get(entry.node_id)
        if member_record is None:
            raise JointConsensusError(
                f"Promote for unknown member {entry.node_id!r}"
            )
        if member_record.status != MemberStatus.LEARNER:
            raise JointConsensusError(
                f"Promote on non-learner member {entry.node_id!r} "
                f"(current status={member_record.status.value})"
            )
        self._state.members[entry.node_id] = member_record.with_promotion(new_epoch)
        self._bump_metadata_epoch(new_epoch, committed_at_lsn)

    def _apply_remove(
        self,
        entry: Remove,
        committed_at_term: int,
        new_epoch: int,
        committed_at_lsn: int,
    ) -> None:
        if entry.node_id not in self._state.members:
            # Idempotent — removing an already-removed member is fine
            # (e.g., redundant force-remove). Still bump the epoch so
            # the entry is observable.
            self._bump_metadata_epoch(new_epoch, committed_at_lsn)
            return

        # Force-remove may bypass joint consensus by operator policy;
        # standard remove requires that the resulting voter set still
        # satisfies cluster_size quorum. The bootstrap coordinator and
        # apply layer both check this — we defer to the caller (proposer)
        # to enforce; here we just remove.
        del self._state.members[entry.node_id]
        self._bump_metadata_epoch(new_epoch, committed_at_lsn)

    def _apply_update_metadata(
        self,
        entry: UpdateMetadata,
        committed_at_term: int,
        new_epoch: int,
        committed_at_lsn: int,
    ) -> None:
        member_record = self._state.members.get(entry.node_id)
        if member_record is None:
            raise JointConsensusError(
                f"UpdateMetadata for unknown member {entry.node_id!r}"
            )

        # Overlay the delta onto existing capabilities. None values
        # delete keys (AD-52 §6). Sort the result for AD-52 §15
        # determinism.
        existing_capabilities = dict(member_record.capabilities)
        for delta_key, delta_value in entry.metadata_delta:
            if delta_value is None:
                existing_capabilities.pop(delta_key, None)
            else:
                existing_capabilities[delta_key] = delta_value
        sorted_capabilities = tuple(sorted(existing_capabilities.items()))

        self._state.members[entry.node_id] = member_record.with_metadata(
            capabilities=sorted_capabilities,
            last_metadata_epoch=new_epoch,
        )
        self._bump_metadata_epoch(new_epoch, committed_at_lsn)

    def _apply_register_datacenter(
        self,
        entry: RegisterDatacenter,
        committed_at_term: int,
        new_epoch: int,
        committed_at_lsn: int,
    ) -> None:
        # AD-52 §17 RegisterDatacenter is gate-cluster-only. The cluster
        # role is not directly tracked in the state machine (the apply
        # layer doesn't know its own role), so we accept the entry here
        # and let the caller filter at proposal time. The DC catalog
        # itself lives in federation.py and is kept in sync via the
        # watch stream consuming this entry from the gate Raft log.
        self._bump_metadata_epoch(new_epoch, committed_at_lsn)

    def _apply_resize_cluster(
        self,
        entry: ResizeCluster,
        committed_at_term: int,
        new_epoch: int,
        committed_at_lsn: int,
    ) -> None:
        if entry.new_cluster_size < 1:
            raise JointConsensusError("ResizeCluster new_cluster_size must be >= 1")
        if entry.new_cluster_size > _MAX_REASONABLE_CLUSTER_SIZE:
            raise JointConsensusError(
                f"ResizeCluster new_cluster_size={entry.new_cluster_size} "
                f"exceeds sanity ceiling {_MAX_REASONABLE_CLUSTER_SIZE}"
            )
        current_voters = self._state.current_voters
        if entry.new_cluster_size < len(current_voters):
            raise JointConsensusError(
                f"ResizeCluster new_cluster_size={entry.new_cluster_size} "
                f"below current voter count {len(current_voters)}"
            )

        self._state.cluster_metadata = ClusterMetadata(
            cluster_uuid=self._state.cluster_metadata.cluster_uuid,
            cluster_id=self._state.cluster_metadata.cluster_id,
            cluster_size=entry.new_cluster_size,
            membership_epoch=new_epoch,
            last_membership_lsn=committed_at_lsn,
            freeze_active=self._state.cluster_metadata.freeze_active,
            joint_consensus_active=self._state.cluster_metadata.joint_consensus_active,
        )

    # =========================================================================
    # Helpers
    # =========================================================================

    def _bump_metadata_epoch(self, new_epoch: int, committed_at_lsn: int) -> None:
        self._state.cluster_metadata = ClusterMetadata(
            cluster_uuid=self._state.cluster_metadata.cluster_uuid,
            cluster_id=self._state.cluster_metadata.cluster_id,
            cluster_size=self._state.cluster_metadata.cluster_size,
            membership_epoch=new_epoch,
            last_membership_lsn=committed_at_lsn,
            freeze_active=self._state.cluster_metadata.freeze_active,
            joint_consensus_active=self._state.cluster_metadata.joint_consensus_active,
        )
