"""
ClusterRPCFenceHeader — the on-the-wire fence carried by every cluster
RPC (AD-52 §6). Validated by the protocol layer before any handler runs.

Composed with the existing AD-10 fence-tokens-from-terms idea along the
membership axis: term + epoch + sender identity + sender's view of the
membership LSN, all checked against the receiver's authoritative state.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ClusterRPCFenceHeader:
    """
    Fields:
        cluster_uuid           Bootstrap-minted UUID (AD-52 §3). Receiver
                               rejects with WRONG_CLUSTER if this does
                               not match.
        membership_epoch       Sender's view of the cluster's current
                               membership_epoch. Receiver rejects with
                               STALE_MEMBERSHIP if too far behind.
        sender_node_id         Runtime uuid4 of the sending node.
                               Receiver rejects with STALE_MEMBER if
                               the sender is not in current membership.
        sender_term            Sender's current Raft term. Receiver
                               rejects with STALE_TERM if from a
                               leadership epoch prior to its own.
        sender_membership_lsn  Last membership entry the sender has
                               applied. Used by receivers to bound the
                               STALE_MEMBERSHIP check.
    """

    cluster_uuid: str
    membership_epoch: int
    sender_node_id: str
    sender_term: int
    sender_membership_lsn: int
