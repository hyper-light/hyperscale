"""
Join protocol messages (AD-52 §5).

Exchanged when a node without --initial-members joins an existing
cluster, or when a restarted founding node falls through to join
because the rest of the founding set is already JOINED.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from .fence_header import ClusterRPCFenceHeader
from .member_record import MemberRecord
from .node_capabilities_ref import NodeCapabilitiesRef


class ClusterState(str, Enum):
    """AD-52 §4-§5 cluster lifecycle states."""

    DISCOVERING = "discovering"
    FORMING = "forming"
    BOOTSTRAPPING = "bootstrapping"
    JOINING = "joining"
    JOINED = "joined"
    HALTED = "halted"


@dataclass(frozen=True, slots=True)
class JoinHello:
    """
    First message sent by a joiner to any candidate cluster member it
    can reach via seed locators. The receiver answers with HelloResponse
    pointing at the current leader (if any). The joiner then opens a
    second connection to the leader for the JoinRequest.

    Fields:
        node_id                   uuid4() runtime id of the joiner.
        cluster_id                --cluster-id. Mismatch → refuse with
                                  WRONG_CLUSTER.
        role                      "gate" | "manager" | "worker".
        protocol_version          AD-25 version.
        capabilities              AD-25 capabilities reference.
        advertised_address_hint   Joiner's best guess at its own
                                  externally-reachable address. May be
                                  empty; in that case the leader uses
                                  the source IP of the TLS connection.
    """

    node_id: str
    cluster_id: str
    role: str
    protocol_version: str
    capabilities: NodeCapabilitiesRef
    advertised_address_hint: tuple[str, int] | None = None


@dataclass(frozen=True, slots=True)
class HelloResponse:
    """
    Response to JoinHello. Identifies the responder, the current cluster
    state, and the leader hint the joiner should redirect to.

    Fields:
        their_node_id              uuid4() of the responder.
        their_role                 "gate" | "manager" | "worker".
        cluster_uuid               Cluster's bootstrap-minted UUID.
        cluster_state              Current ClusterState.
        leader_hint                Runtime node_id of the current leader,
                                   or None if no leader (e.g., election
                                   in progress).
        leader_address             Address peers should use to reach the
                                   leader, or None.
        membership_epoch           Responder's view of the membership_epoch.
        current_membership_size    Number of voters currently in cluster.
        server_capabilities        AD-25 capabilities reference of the
                                   responder.
        admission_refusal_reason   Set when the cluster refuses to admit
                                   this joiner (wrong cluster id, role
                                   not in AD-28 matrix, frozen, etc.).
                                   When set, the other fields may be
                                   absent — the joiner should not retry.
    """

    their_node_id: str
    their_role: str
    cluster_uuid: str
    cluster_state: ClusterState
    membership_epoch: int
    current_membership_size: int
    server_capabilities: NodeCapabilitiesRef
    leader_hint: str | None = None
    leader_address: tuple[str, int] | None = None
    admission_refusal_reason: str | None = None


@dataclass(frozen=True, slots=True)
class JoinRequest:
    """
    Sent by the joiner to the cluster leader. The leader proposes
    AddLearner via Raft and replies with JoinAccepted on commit.

    Fields:
        node_id                Joiner's uuid4().
        role                   "gate" | "manager" | "worker".
        advertised_address     Address peers should use. If
                               --advertised-address was set, that value;
                               otherwise unset and the leader uses the
                               TLS source IP.
        capabilities           AD-25 capabilities reference.
        attestation            Operator-defined attestation blob;
                               currently opaque, reserved for future
                               admission-control hooks.
    """

    node_id: str
    role: str
    advertised_address: tuple[str, int] | None
    capabilities: NodeCapabilitiesRef
    attestation: bytes = b""


@dataclass(frozen=True, slots=True)
class JoinAccepted:
    """
    Leader's reply to JoinRequest after AddLearner is committed via Raft.

    Fields:
        cluster_uuid              Bootstrap-minted UUID; AD-52 §3.
        current_membership        Snapshot of the membership table at
                                  AddLearner-commit time. The joiner
                                  uses this as the initial soft-state
                                  cache (AD-52 §10).
        current_membership_epoch  Epoch at which AddLearner was committed.
        cluster_size              Configured cluster_size; quorum guard.
        fence_token               First fence header the joiner should
                                  use for outgoing RPCs. Subsequent fence
                                  values come from the watch stream
                                  (AD-52 §9).
    """

    cluster_uuid: str
    current_membership: tuple[MemberRecord, ...]
    current_membership_epoch: int
    cluster_size: int
    fence_token: ClusterRPCFenceHeader
