"""
Manager Raft integration coordinator.

Wires Raft consensus into the manager server by providing:
- Initialization of all Raft components
- TCP send callback for inter-node Raft messages
- the cluster's committed membership (AD-52 slice C) for the job groups
- Message routing helpers for TCP handlers
"""

from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING

from hyperscale.distributed.raft.store.raft_storage import RaftStorage
from hyperscale.distributed.raft import RaftConsensus, RaftPeerOutbox
from hyperscale.distributed.raft.logging_models import RaftDebug
from hyperscale.distributed.raft.models import (
    AppendEntries,
    AppendEntriesResponse,
    RequestVote,
    RequestVoteResponse,
)

if TYPE_CHECKING:
    from hyperscale.distributed.ledger.job_ledger_replica import JobLedgerReplica
    from hyperscale.distributed.taskex import TaskRunner
    from hyperscale.logging import Logger
    from hyperscale.distributed.hlc.hybrid_logical_clock import HybridLogicalClock


class ManagerRaftIntegration:
    """
    Coordinates Raft consensus integration for the manager server.

    Encapsulates initialization, message routing, and membership
    tracking. The server only needs to:
    1. Call initialize() during startup
    2. Wire TCP handlers to the route_* methods
    Membership comes from the manager cluster's membership group
    (``cluster_members``), never from SWIM events.
    """

    __slots__ = (
        "_consensus",
        "_send_tcp",
        "_node_id",
        "_logger",
        "_outbox",
        "_request_timeout_seconds",
    )

    def __init__(
        self,
        node_id: str,
        logger: "Logger",
        task_runner: "TaskRunner",
        send_tcp: Callable[..., Awaitable[bytes | Exception | None]],
        configured_cluster_size: int = 1,
        proposal_timeout_seconds: float = 5.0,
        on_job_raft_leader: Callable[[str], None] | None = None,
        on_job_raft_lose_leader: Callable[[str], None] | None = None,
        *,
        clock: "HybridLogicalClock",
        may_lead: Callable[[], bool],
        ledger_replica: "JobLedgerReplica",
        request_timeout_seconds: float,
        cluster_members: Callable[[], Mapping[str, tuple[str, int]]],
        storage: RaftStorage,
    ) -> None:
        """
        ``request_timeout_seconds`` bounds each Raft exchange over
        ``send_tcp``; the job groups' CheckQuorum window covers it.
        ``cluster_members`` is the cluster's committed membership (AD-52
        slice C): the members every job group moves toward.
        """
        self._node_id = node_id
        self._logger = logger
        self._send_tcp = send_tcp
        self._request_timeout_seconds = request_timeout_seconds
        self._outbox = RaftPeerOutbox(
            exchange=self._exchange,
            task_runner=task_runner,
            logger=logger,
            node_id=node_id,
        )

        self._consensus = RaftConsensus(
            node_id=node_id,
            logger=logger,
            task_runner=task_runner,
            send_message=self._send_raft_message,
            configured_cluster_size=configured_cluster_size,
            proposal_timeout_seconds=proposal_timeout_seconds,
            on_become_leader=on_job_raft_leader,
            on_lose_leadership=on_job_raft_lose_leader,
            clock=clock,
            may_lead=may_lead,
            ledger_replica=ledger_replica,
            request_timeout_seconds=request_timeout_seconds,
            cluster_members=cluster_members,
            storage=storage,
        )

    @property
    def consensus(self) -> RaftConsensus:
        """Access the underlying RaftConsensus coordinator."""
        return self._consensus

    async def start(self) -> None:
        """Resume the job groups this node's disk held, then start the
        Raft tick loop."""
        await self._consensus.recover_groups()
        self._consensus.start_tick_loop()

    def set_cohort_size(self, cohort_size: int) -> None:
        """The cluster's cohort was resized (AD-52 ``ResizeCluster``)."""
        self._consensus.set_cohort_size(cohort_size)

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

        reply = await self._send_tcp(addr, method, request.dump(), self._request_timeout_seconds)
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
