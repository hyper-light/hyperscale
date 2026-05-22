"""
BootstrapCoordinator (AD-52 §4) — deterministic cold formation.

State machine:

    DISCOVERING ── peer JOINED detected ──► JOINING (delegate to JoinCoordinator)
        │
        │ --initial-members supplied
        ▼
    FORMING ── quorum confirmed ──► BOOTSTRAPPING ── EnterJoint+LeaveJoint commit ──► JOINED

Hard invariants from AD-52 §4 (must NEVER be violated):

  - bootstrap only fires when --initial-members is supplied AND our
    node_id is in the list AND list size equals cluster_size;
  - every founding node must agree on initial_members_hash (sha256 of
    sorted, comma-joined --initial-members);
  - pre-vote runs at term 1; the seed EnterJoint is the very first
    committed log entry;
  - cluster_uuid is minted exactly once, by the pre-vote winner, and
    travels with the seed EnterJoint;
  - if any step fails, the coordinator exits non-zero — never silently
    falls back to a single-node bootstrap (AD-52 §4 explicit refusal of
    the "lowest-node_id-window" race).
"""

from __future__ import annotations

import asyncio
import hashlib
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from .identity import ClusterIdentity
from .joint_consensus import JointConsensusStateMachine
from .membership_log import EnterJoint, LeaveJoint
from .membership_log.base import CURRENT_SCHEMA_VERSION, EntryMetadata
from .membership_state import MembershipState
from .models.bootstrap_messages import BootstrapHello
from .models.cluster_metadata import ClusterMetadata
from .models.join_messages import ClusterState

if TYPE_CHECKING:
    from hyperscale.logging import Logger


class BootstrapState(str, Enum):
    DISCOVERING = "discovering"
    FORMING = "forming"
    BOOTSTRAPPING = "bootstrapping"
    JOINED = "joined"
    HALTED = "halted"


class BootstrapError(Exception):
    """Raised on unrecoverable bootstrap failure. The serve command
    exits non-zero on this; the orchestrator decides what to do next."""


@dataclass(frozen=True, slots=True)
class FoundingMemberEntry:
    """One entry from --initial-members: operator-assigned founding id
    + resolved address."""

    founding_node_id: str
    address: tuple[str, int]


@dataclass(frozen=True, slots=True)
class BootstrapOutcome:
    """
    Result of BootstrapCoordinator.coordinate().

    bootstrapped         True if this node was the bootstrap winner and
                         the seed EnterJoint+LeaveJoint pair is committed.
    fell_through_to_join True if the coordinator detected that peers are
                         already JOINED and the caller should now run
                         JoinCoordinator instead.
    cluster_uuid         Minted UUID if bootstrapped; "" otherwise.
    membership_state     Fresh MembershipState containing the seed
                         membership. Empty if fell_through_to_join.
    """

    bootstrapped: bool
    fell_through_to_join: bool
    cluster_uuid: str
    membership_state: MembershipState | None = None


@runtime_checkable
class BootstrapTransport(Protocol):
    """
    Transport surface the coordinator needs from the server layer.
    Lets us unit-test the coordinator without a TCP server.
    """

    async def send_bootstrap_hello(
        self,
        target_address: tuple[str, int],
        hello: BootstrapHello,
        timeout_seconds: float,
    ) -> BootstrapHello | None:
        """
        Open an mTLS connection to target_address, send our
        BootstrapHello, await theirs. Returns None on timeout or
        connection failure. Raises only on configuration-level errors
        (e.g., mTLS cert refused).
        """
        ...

    async def probe_cluster_state(
        self,
        target_address: tuple[str, int],
        timeout_seconds: float,
    ) -> ClusterState | None:
        """
        Lightweight probe: ask target_address for its ClusterState
        without exchanging BootstrapHello. Returns the responder's
        ClusterState, or None on timeout / refusal.
        Used to detect "fall through to join" — when a founding peer
        is already JOINED.
        """
        ...


@runtime_checkable
class ClusterRaftProposer(Protocol):
    """
    Raft-side interface the coordinator needs. The actual implementation
    lives in raft/ (extended for joint consensus per task #19).
    """

    async def run_pre_vote(self, term: int) -> bool:
        """Returns True if this node won the pre-vote and may proceed
        to propose the seed entry."""
        ...

    async def propose_seed_enter_joint(self, entry: EnterJoint) -> int:
        """Propose EnterJoint as Raft entry #1 at term 1. Returns the
        committed LSN. Raises on commit failure."""
        ...

    async def propose_seed_leave_joint(self, entry: LeaveJoint) -> int:
        """Propose LeaveJoint as Raft entry #2. Returns the committed LSN."""
        ...


class BootstrapCoordinator:
    """
    Coordinates the AD-52 §4 cold-formation state machine.
    """

    __slots__ = (
        "_identity",
        "_initial_members",
        "_cluster_size",
        "_transport",
        "_raft_proposer",
        "_bootstrap_window_seconds",
        "_handshake_timeout_seconds",
        "_logger",
        "_state",
        "_observed_hellos",
        "_state_machine",
    )

    DEFAULT_BOOTSTRAP_WINDOW_SECONDS: float = 5.0
    DEFAULT_HANDSHAKE_TIMEOUT_SECONDS: float = 2.0

    def __init__(
        self,
        identity: ClusterIdentity,
        initial_members: list[FoundingMemberEntry],
        cluster_size: int,
        transport: BootstrapTransport,
        raft_proposer: ClusterRaftProposer,
        bootstrap_window_seconds: float | None = None,
        handshake_timeout_seconds: float | None = None,
        logger: "Logger | None" = None,
    ) -> None:
        if cluster_size < 1:
            raise ValueError("cluster_size must be >= 1")
        if len(initial_members) != cluster_size:
            raise BootstrapError(
                f"--initial-members length {len(initial_members)} != "
                f"--cluster-size {cluster_size} (AD-52 §4)"
            )

        founding_ids = [member.founding_node_id for member in initial_members]
        if len(set(founding_ids)) != len(founding_ids):
            raise BootstrapError(
                "--initial-members has duplicate node_ids — operator must "
                "deduplicate before retry (AD-52 §4)"
            )

        self._identity = identity
        self._initial_members = list(initial_members)
        self._cluster_size = cluster_size
        self._transport = transport
        self._raft_proposer = raft_proposer
        self._bootstrap_window_seconds = (
            bootstrap_window_seconds
            if bootstrap_window_seconds is not None
            else self.DEFAULT_BOOTSTRAP_WINDOW_SECONDS
        )
        self._handshake_timeout_seconds = (
            handshake_timeout_seconds
            if handshake_timeout_seconds is not None
            else self.DEFAULT_HANDSHAKE_TIMEOUT_SECONDS
        )
        self._logger = logger
        self._state = BootstrapState.DISCOVERING
        self._observed_hellos: dict[str, BootstrapHello] = {}
        self._state_machine: JointConsensusStateMachine | None = None

    @property
    def state(self) -> BootstrapState:
        return self._state

    @property
    def initial_members_hash(self) -> str:
        """sha256 of sorted, comma-joined founding_node_id@host:port entries.
        Used by every founding node to confirm agreement."""
        canonical_form = ",".join(
            f"{member.founding_node_id}@{member.address[0]}:{member.address[1]}"
            for member in sorted(
                self._initial_members,
                key=lambda entry: entry.founding_node_id,
            )
        )
        return hashlib.sha256(canonical_form.encode("utf-8")).hexdigest()

    async def coordinate(self) -> BootstrapOutcome:
        """Run the state machine to completion. May fall through to
        JoinCoordinator territory; in that case the outcome.fell_through_to_join
        is True and the caller switches paths."""

        await self._log_event("ClusterBootstrapStarted", state=self._state.value)

        self._state = BootstrapState.DISCOVERING
        fall_through = await self._discover_phase()
        if fall_through:
            return BootstrapOutcome(
                bootstrapped=False,
                fell_through_to_join=True,
                cluster_uuid="",
                membership_state=None,
            )

        self._state = BootstrapState.FORMING
        await self._forming_phase()

        self._state = BootstrapState.BOOTSTRAPPING
        outcome = await self._bootstrapping_phase()

        self._state = BootstrapState.JOINED
        await self._log_event(
            "ClusterBootstrapCompleted",
            cluster_uuid=outcome.cluster_uuid,
        )
        return outcome

    # =========================================================================
    # Phase 1: DISCOVERING.
    #
    # Probe each initial-member's address. If any are already JOINED,
    # fall through to join. Otherwise gather BootstrapHello exchanges.
    # =========================================================================

    async def _discover_phase(self) -> bool:
        """Returns True if we should fall through to JoinCoordinator."""

        peers = [member for member in self._initial_members
                 if member.founding_node_id != self._self_founding_id()]
        if not peers:
            # Single-node bootstrap is a degenerate but valid case.
            return False

        probe_results = await asyncio.gather(
            *[
                self._transport.probe_cluster_state(
                    peer.address,
                    timeout_seconds=self._handshake_timeout_seconds,
                )
                for peer in peers
            ],
            return_exceptions=True,
        )

        for peer_index, probe_result in enumerate(probe_results):
            if isinstance(probe_result, Exception):
                continue
            if probe_result == ClusterState.JOINED:
                # AD-52 §4: "Otherwise, the node joins an existing
                # cluster via Section 5." Surface the signal.
                await self._log_event(
                    "ClusterBootstrapFellThroughToJoin",
                    peer_address=str(peers[peer_index].address),
                )
                return True

        return False

    # =========================================================================
    # Phase 2: FORMING.
    #
    # Exchange BootstrapHello with each peer. Each peer's hash must
    # match ours; cluster_id and cluster_size too. We require a quorum
    # of confirmations before proceeding.
    # =========================================================================

    async def _forming_phase(self) -> None:
        my_hello = self._build_my_hello()
        peers = [
            member for member in self._initial_members
            if member.founding_node_id != self._self_founding_id()
        ]

        deadline_monotonic = time.monotonic() + self._bootstrap_window_seconds
        while time.monotonic() < deadline_monotonic:
            outstanding = [
                peer
                for peer in peers
                if peer.founding_node_id not in self._observed_hellos
            ]
            if not outstanding:
                break

            handshake_results = await asyncio.gather(
                *[
                    self._transport.send_bootstrap_hello(
                        peer.address,
                        my_hello,
                        timeout_seconds=self._handshake_timeout_seconds,
                    )
                    for peer in outstanding
                ],
                return_exceptions=True,
            )

            for peer_index, handshake_result in enumerate(handshake_results):
                if isinstance(handshake_result, Exception) or handshake_result is None:
                    continue
                peer_hello: BootstrapHello = handshake_result
                self._validate_peer_hello(peer_hello, outstanding[peer_index])
                self._observed_hellos[peer_hello.my_node_id] = peer_hello

            confirmed_count = len(self._observed_hellos) + 1  # +1 for self
            quorum_for_bootstrap = (self._cluster_size // 2) + 1
            if confirmed_count >= quorum_for_bootstrap:
                break

            # Backoff briefly before retrying outstanding peers.
            await asyncio.sleep(0.1)

        confirmed_count = len(self._observed_hellos) + 1
        quorum_for_bootstrap = (self._cluster_size // 2) + 1
        if confirmed_count < quorum_for_bootstrap:
            raise BootstrapError(
                f"bootstrap FORMING phase failed: only {confirmed_count}/"
                f"{self._cluster_size} founding members reached within "
                f"{self._bootstrap_window_seconds}s; need quorum "
                f"({quorum_for_bootstrap}) to proceed (AD-52 §4)"
            )

    def _validate_peer_hello(
        self,
        peer_hello: BootstrapHello,
        expected_peer: FoundingMemberEntry,
    ) -> None:
        if peer_hello.cluster_id != self._identity.cluster_id:
            raise BootstrapError(
                f"peer {expected_peer.founding_node_id!r} reports "
                f"cluster_id={peer_hello.cluster_id!r}, ours="
                f"{self._identity.cluster_id!r} — refusing bootstrap"
            )
        if peer_hello.cluster_size != self._cluster_size:
            raise BootstrapError(
                f"peer {expected_peer.founding_node_id!r} cluster_size="
                f"{peer_hello.cluster_size} mismatches ours={self._cluster_size}"
            )
        expected_hash = self.initial_members_hash
        if peer_hello.initial_members_hash != expected_hash:
            raise BootstrapError(
                f"peer {expected_peer.founding_node_id!r} initial_members_hash "
                f"mismatch — operator must align --initial-members across "
                f"founding nodes (AD-52 §4)"
            )
        if peer_hello.my_node_id != expected_peer.founding_node_id:
            raise BootstrapError(
                f"peer at {expected_peer.address} reports founding_id "
                f"{peer_hello.my_node_id!r}, expected "
                f"{expected_peer.founding_node_id!r} per --initial-members"
            )

    # =========================================================================
    # Phase 3: BOOTSTRAPPING.
    #
    # Pre-vote winner proposes the seed EnterJoint, then LeaveJoint.
    # cluster_uuid is minted at this point and travels with EnterJoint.
    # =========================================================================

    async def _bootstrapping_phase(self) -> BootstrapOutcome:
        pre_vote_won = await self._raft_proposer.run_pre_vote(term=1)
        if not pre_vote_won:
            # Another founding member is becoming the bootstrap leader.
            # We become a follower of theirs.
            await self._log_event("ClusterBootstrapPreVoteLost")
            # The follower path waits for the leader's AppendEntries to
            # deliver the seed entries; the caller's main loop continues.
            # We don't mint a UUID here.
            return BootstrapOutcome(
                bootstrapped=False,
                fell_through_to_join=False,
                cluster_uuid="",
                membership_state=None,
            )

        cluster_uuid = uuid.uuid4().hex
        founding_runtime_uuids = self._collect_runtime_uuids()

        seed_enter_joint = EnterJoint(
            old_members=frozenset(),
            new_members=frozenset(founding_runtime_uuids),
            cluster_uuid_at_create=cluster_uuid,
            metadata=EntryMetadata(
                schema_version=CURRENT_SCHEMA_VERSION,
                committed_at_term=1,
                committed_at_epoch=1,
            ),
        )
        committed_enter_lsn = await self._raft_proposer.propose_seed_enter_joint(
            seed_enter_joint
        )

        seed_leave_joint = LeaveJoint(
            members=frozenset(founding_runtime_uuids),
            metadata=EntryMetadata(
                schema_version=CURRENT_SCHEMA_VERSION,
                committed_at_term=1,
                committed_at_epoch=2,
            ),
        )
        await self._raft_proposer.propose_seed_leave_joint(seed_leave_joint)

        membership_state = self._build_initial_membership_state(
            cluster_uuid=cluster_uuid,
            committed_at_lsn=committed_enter_lsn,
            founding_runtime_uuids=founding_runtime_uuids,
        )

        return BootstrapOutcome(
            bootstrapped=True,
            fell_through_to_join=False,
            cluster_uuid=cluster_uuid,
            membership_state=membership_state,
        )

    def _build_initial_membership_state(
        self,
        cluster_uuid: str,
        committed_at_lsn: int,
        founding_runtime_uuids: frozenset[str],
    ) -> MembershipState:
        # The membership table is constructed by re-applying the seed
        # entries through JointConsensusStateMachine — guarantees the
        # local state matches what followers will compute.
        initial_state = MembershipState(
            cluster_metadata=ClusterMetadata(
                cluster_uuid="",
                cluster_id=self._identity.cluster_id,
                cluster_size=self._cluster_size,
                membership_epoch=0,
                last_membership_lsn=0,
                freeze_active=False,
                joint_consensus_active=False,
            ),
            members={},
        )
        self._state_machine = JointConsensusStateMachine(initial_state)

        # The actual cluster module will replay the committed entries
        # via the Raft apply callback. Here we eagerly apply the seed
        # entries so the bootstrap leader has a ready MembershipState
        # to return.
        seed_enter_joint = EnterJoint(
            old_members=frozenset(),
            new_members=founding_runtime_uuids,
            cluster_uuid_at_create=cluster_uuid,
        )
        seed_leave_joint = LeaveJoint(members=founding_runtime_uuids)
        self._state_machine.apply_entry(
            seed_enter_joint, committed_at_term=1, committed_at_lsn=committed_at_lsn,
        )
        self._state_machine.apply_entry(
            seed_leave_joint, committed_at_term=1, committed_at_lsn=committed_at_lsn + 1,
        )
        return self._state_machine.state

    # =========================================================================
    # Helpers
    # =========================================================================

    def _self_founding_id(self) -> str:
        """
        AD-52 §4 says --initial-members carries operator-assigned
        founding_node_ids that map to addresses; we identify "self" by
        finding the entry whose address equals our advertised address.
        The serve command is expected to inject the correct mapping at
        construction; if it cannot, we refuse to start (this is
        configuration-level — the operator must supply self).
        """
        if self._identity.advertised_address is None:
            raise BootstrapError(
                "BootstrapCoordinator requires identity.advertised_address "
                "to be set so self can be located in --initial-members"
            )
        self_address = self._identity.advertised_address
        for member in self._initial_members:
            if member.address == self_address:
                return member.founding_node_id
        raise BootstrapError(
            f"this node's advertised_address {self_address} not present in "
            f"--initial-members (AD-52 §4 requires the founding node's own "
            f"identity to be in the list)"
        )

    def _build_my_hello(self) -> BootstrapHello:
        return BootstrapHello(
            my_node_id=self._self_founding_id(),
            cluster_id=self._identity.cluster_id,
            role=self._identity.role,
            initial_members_hash=self.initial_members_hash,
            runtime_uuid=self._identity.node_id,
            cluster_size=self._cluster_size,
            protocol_version="1",  # AD-25 placeholder; real value comes
                                   # from the protocol-version module at
                                   # integration time.
        )

    def _collect_runtime_uuids(self) -> frozenset[str]:
        """Map operator founding_node_ids to the runtime uuid4()s collected
        during FORMING. Self's runtime uuid is the identity's node_id."""
        runtime_uuids: list[str] = [self._identity.node_id]
        for peer_hello in self._observed_hellos.values():
            runtime_uuids.append(peer_hello.runtime_uuid)
        return frozenset(runtime_uuids)

    async def _log_event(self, event_name: str, **fields: object) -> None:
        if self._logger is None:
            return
        payload: dict[str, object] = {"event": event_name}
        payload.update(fields)
        await self._logger.log(payload)
