"""
JoinCoordinator (AD-52 §5) — join an existing cluster as a learner.

State machine:

    DISCOVERING ── seed locator yields candidates ──► JOINING
        │                                                  │
        │                                                  │ leader proposes
        │                                                  │ AddLearner
        │                                                  ▼
        │                                              LEARNER (catch-up)
        │                                                  │
        │                                                  │ Promote committed
        │                                                  ▼
        │                                              JOINED
        │
        └── all candidates refuse / unreachable ──► HALTED  (non-zero exit)

The coordinator does NOT manage the actual Raft state machine — that's
ClusterRaftNode's job. JoinCoordinator only:

  1. Discovers the leader via the seed locators.
  2. Performs the JoinHello / HelloResponse / JoinRequest / JoinAccepted
     exchange.
  3. Hands the resulting initial MembershipState to the caller, which
     installs it on the local Raft node and begins catch-up.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from .identity import ClusterIdentity
from .membership_state import MembershipState
from .models.cluster_metadata import ClusterMetadata
from .models.fence_header import ClusterRPCFenceHeader
from .models.join_messages import (
    ClusterState,
    HelloResponse,
    JoinAccepted,
    JoinHello,
    JoinRequest,
)
from .models.member_record import MemberRecord, MemberStatus
from .models.node_capabilities_ref import NodeCapabilitiesRef
from .seed_locators import SeedResolver

if TYPE_CHECKING:
    from hyperscale.logging import Logger


class JoinState(str, Enum):
    DISCOVERING = "discovering"
    JOINING = "joining"
    LEARNING = "learning"
    JOINED = "joined"
    HALTED = "halted"


class JoinError(Exception):
    """Raised when the coordinator cannot complete the join (e.g., all
    seeds refuse with WRONG_CLUSTER, frozen cluster, role mismatch)."""


@dataclass(frozen=True, slots=True)
class JoinOutcome:
    """
    cluster_uuid             Cluster's bootstrap-minted UUID.
    initial_membership       Membership snapshot at JoinAccepted time.
    fence_token              First fence to use on outgoing RPCs.
    leader_address           Address of the current leader (the joiner
                             begins replicating from this peer).
    """

    cluster_uuid: str
    initial_membership: MembershipState
    fence_token: ClusterRPCFenceHeader
    leader_address: tuple[str, int]


@runtime_checkable
class JoinTransport(Protocol):
    """Transport surface JoinCoordinator depends on. Lets the coordinator
    be unit-tested without a TCP server."""

    async def send_join_hello(
        self,
        target_address: tuple[str, int],
        hello: JoinHello,
        timeout_seconds: float,
    ) -> HelloResponse | None:
        ...

    async def send_join_request(
        self,
        target_address: tuple[str, int],
        request: JoinRequest,
        timeout_seconds: float,
    ) -> JoinAccepted | None:
        ...


class JoinCoordinator:
    """
    Drives the AD-52 §5 join state machine.
    """

    __slots__ = (
        "_identity",
        "_seed_resolver",
        "_transport",
        "_capabilities",
        "_handshake_timeout_seconds",
        "_logger",
        "_state",
    )

    DEFAULT_HANDSHAKE_TIMEOUT_SECONDS: float = 5.0

    def __init__(
        self,
        identity: ClusterIdentity,
        seed_resolver: SeedResolver,
        transport: JoinTransport,
        capabilities: NodeCapabilitiesRef,
        handshake_timeout_seconds: float | None = None,
        logger: "Logger | None" = None,
    ) -> None:
        self._identity = identity
        self._seed_resolver = seed_resolver
        self._transport = transport
        self._capabilities = capabilities
        self._handshake_timeout_seconds = (
            handshake_timeout_seconds
            if handshake_timeout_seconds is not None
            else self.DEFAULT_HANDSHAKE_TIMEOUT_SECONDS
        )
        self._logger = logger
        self._state = JoinState.DISCOVERING

    @property
    def state(self) -> JoinState:
        return self._state

    async def coordinate(self) -> JoinOutcome:
        await self._log_event("ClusterJoinStarted", role=self._identity.role)

        self._state = JoinState.DISCOVERING
        leader_hint, leader_address = await self._discover_leader()

        self._state = JoinState.JOINING
        outcome = await self._send_join_request_to_leader(leader_address)

        self._state = JoinState.LEARNING
        await self._log_event(
            "ClusterJoinAccepted",
            cluster_uuid=outcome.cluster_uuid,
            leader_address=str(leader_address),
        )
        return outcome

    # =========================================================================
    # Discover leader.
    # =========================================================================

    async def _discover_leader(self) -> tuple[str, tuple[str, int]]:
        """Returns (leader_node_id, leader_address). Iterates seed
        locator results, sends JoinHello, follows leader_hints until
        the responder IS the leader (or points us at one)."""

        resolved_addresses = await self._seed_resolver.resolve_all()
        if not resolved_addresses:
            raise JoinError(
                "no seed addresses resolved — operator may need to supply "
                "--seeds (AD-52 §5)"
            )

        my_hello = self._build_my_hello()
        last_refusal_reason: str | None = None

        # Try each candidate in turn. Follow leader_hint forward as soon
        # as we get one.
        for resolved_address in resolved_addresses:
            candidate_address = (resolved_address.host, resolved_address.port)
            hello_response = await self._transport.send_join_hello(
                candidate_address,
                my_hello,
                timeout_seconds=self._handshake_timeout_seconds,
            )
            if hello_response is None:
                continue
            if hello_response.admission_refusal_reason:
                last_refusal_reason = hello_response.admission_refusal_reason
                await self._log_event(
                    "ClusterJoinRefused",
                    peer_address=str(candidate_address),
                    reason=last_refusal_reason,
                )
                # Configuration-level refusals (WRONG_CLUSTER, role
                # mismatch, frozen) propagate as JoinError — there's
                # no other seed that will help.
                if self._is_fatal_refusal(last_refusal_reason):
                    raise JoinError(
                        f"join refused by {candidate_address}: "
                        f"{last_refusal_reason}"
                    )
                continue
            if hello_response.cluster_state != ClusterState.JOINED:
                # Cluster isn't ready to admit joiners yet (still
                # bootstrapping or in election). Move on; the seed
                # resolver will return us here on the next refresh.
                continue
            # Got a JOINED responder. If they ARE the leader, proceed.
            if hello_response.leader_hint == hello_response.their_node_id:
                return hello_response.their_node_id, candidate_address
            # Otherwise follow the leader_hint.
            if hello_response.leader_address is not None:
                return (
                    hello_response.leader_hint or "",
                    hello_response.leader_address,
                )

        if last_refusal_reason:
            raise JoinError(
                f"all seeds refused join: last reason {last_refusal_reason}"
            )
        raise JoinError(
            "no JOINED cluster member reachable via --seeds; the cluster "
            "may be in election or unreachable on the network"
        )

    @staticmethod
    def _is_fatal_refusal(reason: str) -> bool:
        # AD-28 / AD-52 §5 fatal refusals — caller cannot recover.
        fatal_prefixes = (
            "WRONG_CLUSTER",
            "ROLE_REFUSED",
            "PROTOCOL_VERSION",
        )
        return any(reason.startswith(prefix) for prefix in fatal_prefixes)

    # =========================================================================
    # JoinRequest → JoinAccepted.
    # =========================================================================

    async def _send_join_request_to_leader(
        self,
        leader_address: tuple[str, int],
    ) -> JoinOutcome:
        request = JoinRequest(
            node_id=self._identity.node_id,
            role=self._identity.role,
            advertised_address=self._identity.advertised_address,
            capabilities=self._capabilities,
        )
        join_accepted = await self._transport.send_join_request(
            leader_address,
            request,
            timeout_seconds=self._handshake_timeout_seconds,
        )
        if join_accepted is None:
            raise JoinError(
                f"leader at {leader_address} did not respond to JoinRequest "
                f"within {self._handshake_timeout_seconds}s"
            )

        membership_state = self._materialize_membership_state(join_accepted)
        return JoinOutcome(
            cluster_uuid=join_accepted.cluster_uuid,
            initial_membership=membership_state,
            fence_token=join_accepted.fence_token,
            leader_address=leader_address,
        )

    def _materialize_membership_state(
        self,
        join_accepted: JoinAccepted,
    ) -> MembershipState:
        """Build an initial MembershipState from JoinAccepted so the
        local node can answer fence checks and serve cache reads
        immediately on connect."""

        members_by_id: dict[str, MemberRecord] = {
            record.node_id: record for record in join_accepted.current_membership
        }
        # Add self as a learner — the leader will deliver our AddLearner
        # entry via the Raft replication stream; once committed locally
        # we overwrite. This pre-state lets local code see "self" in the
        # table during catch-up.
        if self._identity.node_id not in members_by_id:
            members_by_id[self._identity.node_id] = MemberRecord(
                node_id=self._identity.node_id,
                role=self._identity.role,
                status=MemberStatus.LEARNER,
                advertised_address=self._identity.advertised_address or ("", 0),
                joined_at_epoch=join_accepted.current_membership_epoch,
            )

        return MembershipState(
            cluster_metadata=ClusterMetadata(
                cluster_uuid=join_accepted.cluster_uuid,
                cluster_id=self._identity.cluster_id,
                cluster_size=join_accepted.cluster_size,
                membership_epoch=join_accepted.current_membership_epoch,
                last_membership_lsn=0,  # filled in by Raft as entries replay
                freeze_active=False,
                joint_consensus_active=False,
            ),
            members=members_by_id,
        )

    def _build_my_hello(self) -> JoinHello:
        return JoinHello(
            node_id=self._identity.node_id,
            cluster_id=self._identity.cluster_id,
            role=self._identity.role,
            protocol_version=self._capabilities.version,
            capabilities=self._capabilities,
            advertised_address_hint=self._identity.advertised_address,
        )

    async def _log_event(self, event_name: str, **fields: object) -> None:
        if self._logger is None:
            return
        payload: dict[str, object] = {"event": event_name}
        payload.update(fields)
        await self._logger.log(payload)
