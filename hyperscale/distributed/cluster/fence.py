"""
ClusterRPCFence — every cluster RPC carries a fence header (AD-52 §6).

Receivers validate the header before any handler runs. The validation
covers four orthogonal axes:

  cluster_uuid    : right cluster?
  sender_node_id  : sender is in current membership?
  sender_term     : sender's term is at least as new as ours?
  membership_lsn  : sender's membership view isn't too stale?

This file holds the pure validator. The protocol layer that snaps the
fence header off the front of each frame lives in
hyperscale.distributed.server.protocol — that integration is done in a
separate task. The validator itself is stateless and side-effect-free
so it can be unit-tested without standing up a transport.

The validator extends AD-10's fence-tokens-from-terms idea along the
membership axis. Together they eliminate the "stale node writing into
the wrong cluster" class of failures.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from .models.fence_header import ClusterRPCFenceHeader


class FenceRejectionReason(str, Enum):
    """
    AD-52 §6 rejection reasons. The sender uses these to self-correct:
      - WRONG_CLUSTER     → exit non-zero (this node is a zombie or
                            misconfigured).
      - STALE_MEMBER      → re-join via Section 5 (this node was evicted).
      - STALE_TERM        → catch up via the leader's heartbeat; retry.
      - STALE_MEMBERSHIP  → resync via watch stream snapshot, retry.
    """

    WRONG_CLUSTER = "WRONG_CLUSTER"
    STALE_MEMBER = "STALE_MEMBER"
    STALE_TERM = "STALE_TERM"
    STALE_MEMBERSHIP = "STALE_MEMBERSHIP"


class ClusterFenceError(Exception):
    """
    Raised at the protocol boundary when a received fence header fails
    validation. The structured payload (reason + receiver's current
    fence state) lets the sender self-correct without round-trips.
    """

    __slots__ = (
        "reason",
        "receiver_cluster_uuid",
        "receiver_membership_epoch",
        "receiver_current_term",
        "receiver_membership_lsn",
        "rejected_header",
    )

    def __init__(
        self,
        reason: FenceRejectionReason,
        receiver_cluster_uuid: str,
        receiver_membership_epoch: int,
        receiver_current_term: int,
        receiver_membership_lsn: int,
        rejected_header: ClusterRPCFenceHeader,
    ) -> None:
        self.reason = reason
        self.receiver_cluster_uuid = receiver_cluster_uuid
        self.receiver_membership_epoch = receiver_membership_epoch
        self.receiver_current_term = receiver_current_term
        self.receiver_membership_lsn = receiver_membership_lsn
        self.rejected_header = rejected_header
        super().__init__(
            f"ClusterFenceError {reason.value}: "
            f"sender({rejected_header.sender_node_id}, "
            f"term={rejected_header.sender_term}, "
            f"epoch={rejected_header.membership_epoch}, "
            f"cluster={rejected_header.cluster_uuid}) "
            f"vs receiver(term={receiver_current_term}, "
            f"epoch={receiver_membership_epoch}, "
            f"cluster={receiver_cluster_uuid})"
        )


@dataclass(frozen=True, slots=True)
class ReceiverFenceState:
    """
    Snapshot of the receiver's authoritative fence state at the moment
    of validation. The ClusterRPCFence validator consumes this rather
    than reaching into any class' internals — keeps the validator
    purely functional.

    cluster_uuid          : bootstrap-minted, never changes.
    membership_epoch      : current epoch from ClusterMetadata.
    current_term          : current Raft term.
    last_membership_lsn   : last membership entry applied locally.
    current_membership    : the set of node_ids currently in the cluster.
    max_membership_lag    : how far behind a sender's epoch may be
                            before STALE_MEMBERSHIP is raised. Reasonable
                            default is a small integer (5-10 epochs).
    """

    cluster_uuid: str
    membership_epoch: int
    current_term: int
    last_membership_lsn: int
    current_membership: frozenset[str]
    max_membership_lag: int = 8


class ClusterRPCFence:
    """
    Pure validator. Construct once with a default max_membership_lag,
    call validate() on every received fence header. Receiver state is
    passed in fresh on every call — the validator never caches it,
    because the receiver's state changes as Raft applies entries.
    """

    __slots__ = ("_default_max_membership_lag",)

    DEFAULT_MAX_MEMBERSHIP_LAG: int = 8

    def __init__(self, max_membership_lag: int | None = None) -> None:
        if max_membership_lag is not None and max_membership_lag < 0:
            raise ValueError("max_membership_lag must be >= 0")
        self._default_max_membership_lag = (
            max_membership_lag
            if max_membership_lag is not None
            else self.DEFAULT_MAX_MEMBERSHIP_LAG
        )

    def validate(
        self,
        header: ClusterRPCFenceHeader,
        receiver_state: ReceiverFenceState,
    ) -> None:
        """
        Raises ClusterFenceError on any rejection condition. Returns
        normally on accept. Checks are ordered by recovery cost so the
        most-fatal failures surface first:

          1. WRONG_CLUSTER    — fatal; sender is a zombie or misconfigured.
          2. STALE_MEMBER     — sender must re-join.
          3. STALE_TERM       — sender's leader view is stale; the sender
                                will catch up on the next heartbeat.
          4. STALE_MEMBERSHIP — sender's epoch is too far behind; resync.
        """

        if header.cluster_uuid != receiver_state.cluster_uuid:
            raise ClusterFenceError(
                reason=FenceRejectionReason.WRONG_CLUSTER,
                receiver_cluster_uuid=receiver_state.cluster_uuid,
                receiver_membership_epoch=receiver_state.membership_epoch,
                receiver_current_term=receiver_state.current_term,
                receiver_membership_lsn=receiver_state.last_membership_lsn,
                rejected_header=header,
            )

        if header.sender_node_id not in receiver_state.current_membership:
            raise ClusterFenceError(
                reason=FenceRejectionReason.STALE_MEMBER,
                receiver_cluster_uuid=receiver_state.cluster_uuid,
                receiver_membership_epoch=receiver_state.membership_epoch,
                receiver_current_term=receiver_state.current_term,
                receiver_membership_lsn=receiver_state.last_membership_lsn,
                rejected_header=header,
            )

        if header.sender_term < receiver_state.current_term:
            raise ClusterFenceError(
                reason=FenceRejectionReason.STALE_TERM,
                receiver_cluster_uuid=receiver_state.cluster_uuid,
                receiver_membership_epoch=receiver_state.membership_epoch,
                receiver_current_term=receiver_state.current_term,
                receiver_membership_lsn=receiver_state.last_membership_lsn,
                rejected_header=header,
            )

        max_lag = (
            receiver_state.max_membership_lag
            if receiver_state.max_membership_lag >= 0
            else self._default_max_membership_lag
        )
        if header.membership_epoch < receiver_state.membership_epoch - max_lag:
            raise ClusterFenceError(
                reason=FenceRejectionReason.STALE_MEMBERSHIP,
                receiver_cluster_uuid=receiver_state.cluster_uuid,
                receiver_membership_epoch=receiver_state.membership_epoch,
                receiver_current_term=receiver_state.current_term,
                receiver_membership_lsn=receiver_state.last_membership_lsn,
                rejected_header=header,
            )

    def build_outgoing_header(
        self,
        cluster_uuid: str,
        membership_epoch: int,
        sender_node_id: str,
        sender_term: int,
        sender_membership_lsn: int,
    ) -> ClusterRPCFenceHeader:
        """
        Helper for senders: build a fresh fence header from the sender's
        current state. Centralized so future header-schema bumps go
        through one place.
        """
        return ClusterRPCFenceHeader(
            cluster_uuid=cluster_uuid,
            membership_epoch=membership_epoch,
            sender_node_id=sender_node_id,
            sender_term=sender_term,
            sender_membership_lsn=sender_membership_lsn,
        )
