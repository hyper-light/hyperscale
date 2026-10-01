"""
Manager Raft integration coordinator.

Wires Raft consensus into the manager server by providing:
- Initialization of all Raft components
- TCP send callback for inter-node Raft messages
- SWIM membership event routing to Raft
- Message routing helpers for TCP handlers
"""

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from hyperscale.distributed.raft import RaftConsensus, RaftJobManager, RaftPeerOutbox
from hyperscale.distributed.raft.logging_models import RaftDebug
from hyperscale.distributed.raft.models import (
    AppendEntries,
    AppendEntriesResponse,
    RequestVote,
    RequestVoteResponse,
)

if TYPE_CHECKING:
    from hyperscale.distributed.jobs.job_leadership_tracker import JobLeadershipTracker
    from hyperscale.distributed.jobs.job_manager import JobManager
    from hyperscale.distributed.ledger.job_ledger_replica import JobLedgerReplica
    from hyperscale.distributed.nodes.manager.state import ManagerState
    from hyperscale.distributed.taskex import TaskRunner
    from hyperscale.logging import Logger
    from hyperscale.logging.lsn import HybridLamportClock


class ManagerRaftIntegration:
    """
    Coordinates Raft consensus integration for the manager server.

    Encapsulates initialization, message routing, and membership
    tracking. The server only needs to:
    1. Call initialize() during startup
    2. Wire TCP handlers to the route_* methods
    3. Call on_node_join/on_node_leave from SWIM callbacks
    """

    __slots__ = (
        "_consensus",
        "_raft_job_manager",
        "_send_tcp",
        "_node_id",
        "_node_addr",
        "_logger",
        "_outbox",
    )

    def __init__(
        self,
        node_id: str,
        job_manager: "JobManager",
        leadership_tracker: "JobLeadershipTracker",
        logger: "Logger",
        task_runner: "TaskRunner",
        send_tcp: Callable[..., Awaitable[bytes | Exception | None]],
        node_addr: tuple[str, int] | None = None,
        configured_cluster_size: int = 1,
        proposal_timeout_seconds: float = 5.0,
        on_job_raft_leader: Callable[[str], None] | None = None,
        on_job_raft_lose_leader: Callable[[str], None] | None = None,
        manager_state: "ManagerState | None" = None,
        clock: "HybridLamportClock | None" = None,
        *,
        ledger_replica: "JobLedgerReplica",
    ) -> None:
        self._node_id = node_id
        self._node_addr = node_addr if node_addr is not None else ("", 0)
        self._logger = logger
        self._send_tcp = send_tcp
        self._outbox = RaftPeerOutbox(
            exchange=self._exchange,
            task_runner=task_runner,
            logger=logger,
            node_id=node_id,
        )

        self._consensus = RaftConsensus(
            node_id=node_id,
            job_manager=job_manager,
            leadership_tracker=leadership_tracker,
            logger=logger,
            task_runner=task_runner,
            send_message=self._send_raft_message,
            configured_cluster_size=configured_cluster_size,
            proposal_timeout_seconds=proposal_timeout_seconds,
            on_become_leader=on_job_raft_leader,
            on_lose_leadership=on_job_raft_lose_leader,
            manager_state=manager_state,
            clock=clock,
            ledger_replica=ledger_replica,
        )

        self._raft_job_manager = RaftJobManager(
            consensus=self._consensus,
            logger=logger,
            node_id=node_id,
            node_addr=self._node_addr,
        )

    @property
    def consensus(self) -> RaftConsensus:
        """Access the underlying RaftConsensus coordinator."""
        return self._consensus

    @property
    def raft_job_manager(self) -> RaftJobManager:
        """Access the Raft-backed job manager wrapper."""
        return self._raft_job_manager

    def start(self) -> None:
        """Start the Raft tick loop."""
        self._consensus.start_tick_loop()

    async def stop(self) -> None:
        """Stop all Raft instances and the tick loop."""
        await self._consensus.destroy_all()
        await self._outbox.close()

    # =========================================================================
    # TCP Send Callback
    # =========================================================================

    async def _send_raft_message(
        self,
        addr: tuple[str, int],
        message: RequestVote | AppendEntries,
    ) -> None:
        """Queue a Raft request for ``addr``; never awaits network I/O.

        RaftNode calls this while holding its lock, so delivery and reply
        routing happen on the outbox's per-peer sender loop instead.
        """
        self._outbox.enqueue(addr, message)

    async def _exchange(
        self,
        addr: tuple[str, int],
        request: RequestVote | AppendEntries,
    ) -> None:
        """Deliver one Raft request and route the peer's reply.

        The peer's handler answers in the TCP reply, so that reply is the
        RPC response: dropping it (as this path once did) meant no vote or
        append ack ever reached a candidate or leader, and no multi-member
        group could elect or commit. A transport failure or an empty
        reply (peer at Raft capacity) is a lost message, which Raft
        recovers from by rebuilding the RPC on the next tick.
        """
        match request:
            case RequestVote():
                method = "raft_request_vote"
            case AppendEntries():
                method = "raft_append_entries"

        reply = await self._send_tcp(addr, method, request.dump())
        if isinstance(reply, Exception) or not reply:
            await self._logger.log(
                RaftDebug(
                    message=(
                        f"Raft {method} to {addr[0]}:{addr[1]} got no response "
                        f"({reply!r}); rebuilt on the next tick"
                    ),
                    node_id=self._node_id,
                    job_id=request.job_id,
                    term=request.term,
                )
            )
            return

        match request:
            case RequestVote():
                await self._consensus.route_request_vote_response(
                    RequestVoteResponse.load(reply)
                )
            case AppendEntries():
                await self._consensus.route_append_entries_response(
                    AppendEntriesResponse.load(reply)
                )

    # =========================================================================
    # TCP Handler Routing
    # =========================================================================

    async def handle_request_vote(self, data: bytes) -> bytes | None:
        """Handle incoming RequestVote RPC. Returns serialized response."""
        request = RequestVote.load(data)
        response = await self._consensus.route_request_vote(request)
        if response is None:
            return None
        return response.dump()

    async def handle_request_vote_response(self, data: bytes) -> None:
        """Handle incoming RequestVoteResponse RPC."""
        response = RequestVoteResponse.load(data)
        await self._consensus.route_request_vote_response(response)

    async def handle_append_entries(self, data: bytes) -> bytes | None:
        """Handle incoming AppendEntries RPC. Returns serialized response."""
        request = AppendEntries.load(data)
        response = await self._consensus.route_append_entries(request)
        if response is None:
            return None
        return response.dump()

    async def handle_append_entries_response(self, data: bytes) -> None:
        """Handle incoming AppendEntriesResponse RPC."""
        response = AppendEntriesResponse.load(data)
        await self._consensus.route_append_entries_response(response)

    # =========================================================================
    # SWIM Membership Routing
    # =========================================================================

    def on_node_join(self, node_id: str, addr: tuple[str, int]) -> None:
        """Route SWIM node join to Raft consensus."""
        self._consensus.on_node_join(node_id, addr)

    def on_node_leave(self, node_id: str) -> None:
        """Route SWIM node dead to Raft consensus."""
        if (addr := self._consensus.member_address(node_id)) is not None:
            self._outbox.forget_peer(addr)
        self._consensus.on_node_leave(node_id)

    def set_initial_membership(
        self,
        members: set[str],
        addrs: dict[str, tuple[str, int]],
    ) -> None:
        """Set initial cluster membership from SWIM state."""
        self._consensus.set_initial_membership(members, addrs)
