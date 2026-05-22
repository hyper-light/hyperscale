"""
ClusterNode — composition root for an AD-52 node.

Holds the identity, seed resolver, bootstrap / join coordinators, the
joint-consensus state machine, the learner coordinator, and the
soft-state cache. Wires them together at construction; runs the
lifecycle (bootstrap-or-join → catch-up → JOINED) when start() is
called.

ClusterNode itself owns no transport: it accepts a BootstrapTransport
and a JoinTransport (both Protocol classes) which the server.protocol
layer provides. This keeps the cluster module free of socket / mTLS
machinery and unit-testable with in-memory transports.

The "second half" of AD-52 (watch streams, disconnected mode, drain,
force-remove, freeze, snapshot import/export, federation, observability,
SWIM+phi-accrual hybrid) plugs into ClusterNode via additional
constructor arguments — those land in their own modules and are wired
here as they ship.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

from .bootstrap import (
    BootstrapCoordinator,
    BootstrapTransport,
    ClusterRaftProposer,
    FoundingMemberEntry,
)
from .fence import ClusterRPCFence
from .identity import ClusterIdentity
from .join import JoinCoordinator, JoinTransport
from .joint_consensus import JointConsensusStateMachine
from .learner import LearnerCoordinator
from .membership_state import MembershipState
from .models.cluster_metadata import ClusterMetadata
from .models.node_capabilities_ref import NodeCapabilitiesRef
from .seed_locators import SeedResolver

if TYPE_CHECKING:
    from hyperscale.distributed.taskex import TaskRunner
    from hyperscale.logging import Logger


class NodeLifecycle(str, Enum):
    UNSTARTED = "unstarted"
    STARTING = "starting"
    BOOTSTRAPPING = "bootstrapping"
    JOINING = "joining"
    JOINED = "joined"
    STOPPED = "stopped"
    HALTED = "halted"


@dataclass(frozen=True, slots=True)
class ClusterNodeConfig:
    """All AD-52 §21 tuning knobs in one struct so ClusterNode's
    constructor signature stays manageable."""

    cluster_size: int
    initial_members: tuple[FoundingMemberEntry, ...] = ()
    bootstrap_enabled: bool = True
    bootstrap_window_seconds: float = 5.0
    handshake_timeout_seconds: float = 5.0
    learner_promote_threshold: int = 256
    learner_max_lifetime_seconds: float = 30 * 60.0
    fence_max_membership_lag: int = 8


class ClusterNode:
    """
    Top-level orchestrator. One instance per process.

    Lifecycle:
      __init__       : wires components, no I/O.
      start()        : runs bootstrap or join, transitions to JOINED.
      stop()         : drains and shuts down.
    """

    __slots__ = (
        "_identity",
        "_config",
        "_seed_resolver",
        "_bootstrap_transport",
        "_join_transport",
        "_raft_proposer",
        "_capabilities",
        "_logger",
        "_task_runner",
        "_state_machine",
        "_learner_coordinator",
        "_fence",
        "_lifecycle",
        "_lifecycle_lock",
        "_stop_event",
    )

    def __init__(
        self,
        identity: ClusterIdentity,
        config: ClusterNodeConfig,
        seed_resolver: SeedResolver,
        bootstrap_transport: BootstrapTransport,
        join_transport: JoinTransport,
        raft_proposer: ClusterRaftProposer,
        capabilities: NodeCapabilitiesRef,
        task_runner: "TaskRunner",
        logger: "Logger | None" = None,
    ) -> None:
        self._identity = identity
        self._config = config
        self._seed_resolver = seed_resolver
        self._bootstrap_transport = bootstrap_transport
        self._join_transport = join_transport
        self._raft_proposer = raft_proposer
        self._capabilities = capabilities
        self._logger = logger
        self._task_runner = task_runner

        # Initial empty membership state. Bootstrap or join replaces it.
        self._state_machine = JointConsensusStateMachine(
            MembershipState(
                cluster_metadata=ClusterMetadata(
                    cluster_uuid="",
                    cluster_id=identity.cluster_id,
                    cluster_size=config.cluster_size,
                ),
            )
        )

        self._learner_coordinator = LearnerCoordinator(
            promote_threshold=config.learner_promote_threshold,
            max_lifetime_seconds=config.learner_max_lifetime_seconds,
        )

        self._fence = ClusterRPCFence(
            max_membership_lag=config.fence_max_membership_lag,
        )

        self._lifecycle = NodeLifecycle.UNSTARTED
        self._lifecycle_lock = asyncio.Lock()
        self._stop_event = asyncio.Event()

    @property
    def lifecycle(self) -> NodeLifecycle:
        return self._lifecycle

    @property
    def state_machine(self) -> JointConsensusStateMachine:
        return self._state_machine

    @property
    def learner_coordinator(self) -> LearnerCoordinator:
        return self._learner_coordinator

    @property
    def fence(self) -> ClusterRPCFence:
        return self._fence

    @property
    def identity(self) -> ClusterIdentity:
        return self._identity

    async def start(self) -> None:
        async with self._lifecycle_lock:
            if self._lifecycle != NodeLifecycle.UNSTARTED:
                raise RuntimeError(
                    f"ClusterNode.start() called in lifecycle {self._lifecycle.value}"
                )
            self._lifecycle = NodeLifecycle.STARTING

        if self._config.bootstrap_enabled and self._config.initial_members:
            self._lifecycle = NodeLifecycle.BOOTSTRAPPING
            bootstrap_coordinator = BootstrapCoordinator(
                identity=self._identity,
                initial_members=list(self._config.initial_members),
                cluster_size=self._config.cluster_size,
                transport=self._bootstrap_transport,
                raft_proposer=self._raft_proposer,
                bootstrap_window_seconds=self._config.bootstrap_window_seconds,
                handshake_timeout_seconds=self._config.handshake_timeout_seconds,
                logger=self._logger,
            )
            outcome = await bootstrap_coordinator.coordinate()
            if outcome.bootstrapped and outcome.membership_state is not None:
                # Install the seed membership state and we are JOINED.
                self._state_machine = JointConsensusStateMachine(
                    outcome.membership_state
                )
                self._lifecycle = NodeLifecycle.JOINED
                return
            if outcome.fell_through_to_join:
                self._lifecycle = NodeLifecycle.JOINING
                # Fall through to join path below.
            else:
                # We lost pre-vote; another founding node is bootstrap
                # leader. The Raft replication stream will deliver the
                # seed entries to us as a follower; transition to JOINED
                # once they apply.
                self._lifecycle = NodeLifecycle.JOINED
                return

        # Join path — for non-founding members, or fall-through after
        # bootstrap detected JOINED peers.
        self._lifecycle = NodeLifecycle.JOINING
        join_coordinator = JoinCoordinator(
            identity=self._identity,
            seed_resolver=self._seed_resolver,
            transport=self._join_transport,
            capabilities=self._capabilities,
            handshake_timeout_seconds=self._config.handshake_timeout_seconds,
            logger=self._logger,
        )
        join_outcome = await join_coordinator.coordinate()
        self._state_machine = JointConsensusStateMachine(
            join_outcome.initial_membership,
        )
        self._lifecycle = NodeLifecycle.JOINED

    async def stop(self) -> None:
        async with self._lifecycle_lock:
            if self._lifecycle in (NodeLifecycle.STOPPED, NodeLifecycle.HALTED):
                return
            self._lifecycle = NodeLifecycle.STOPPED
        self._stop_event.set()
