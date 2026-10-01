"""
Per-peer coalescing outbound queue for Raft RPCs.
"""

import asyncio
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from .logging_models import RaftWarning
from .models import AppendEntries, RequestVote

if TYPE_CHECKING:
    from hyperscale.distributed.taskex import TaskRunner
    from hyperscale.logging import Logger


RaftRequest = RequestVote | AppendEntries
PeerAddress = tuple[str, int]


class RaftPeerOutbox:
    """Delivers Raft requests to peers without blocking the node that built them.

    ``RaftNode`` builds RequestVote/AppendEntries while holding its lock.
    Delivering inline would hold that lock across network I/O, stall
    every job's tick behind one slow peer, and deadlock the moment the
    reply is routed back into the same node (``asyncio.Lock`` is not
    reentrant). ``enqueue`` therefore never awaits: one sender loop per
    peer runs the exchange (send + route reply) for whatever is pending.

    Pending requests coalesce per (job, request kind): a newer request
    for the same job supersedes an undelivered older one, which is safe
    because Raft rebuilds every RPC from current state each tick and
    tolerates lost messages. Memory is bounded by peers x jobs x 2, and
    a slow peer only delays its own deliveries.
    """

    __slots__ = (
        "_exchange",
        "_task_runner",
        "_logger",
        "_node_id",
        "_pending",
        "_wakeups",
        "_sender_tokens",
        "_closed",
    )

    def __init__(
        self,
        exchange: Callable[[PeerAddress, RaftRequest], Awaitable[None]],
        task_runner: "TaskRunner",
        logger: "Logger",
        node_id: str,
    ) -> None:
        self._exchange = exchange
        self._task_runner = task_runner
        self._logger = logger
        self._node_id = node_id
        self._pending: dict[PeerAddress, dict[tuple[str, type], RaftRequest]] = {}
        self._wakeups: dict[PeerAddress, asyncio.Event] = {}
        self._sender_tokens: dict[PeerAddress, str] = {}
        self._closed = False

    @property
    def pending_count(self) -> int:
        """Undelivered requests across all peers."""
        return sum(len(requests) for requests in self._pending.values())

    @property
    def peer_count(self) -> int:
        """Peers with a live sender loop."""
        return len(self._wakeups)

    def enqueue(self, peer_addr: PeerAddress, request: RaftRequest) -> None:
        """Queue ``request`` for ``peer_addr``, superseding any undelivered one."""
        if self._closed:
            return

        self._pending.setdefault(peer_addr, {})[(request.job_id, type(request))] = request
        if (wakeup := self._wakeups.get(peer_addr)) is None:
            wakeup = self._start_sender(peer_addr)
        wakeup.set()

    def forget_peer(self, peer_addr: PeerAddress) -> None:
        """Drop a departed peer's undelivered requests and end its sender loop."""
        self._pending.pop(peer_addr, None)
        self._sender_tokens.pop(peer_addr, None)
        if (wakeup := self._wakeups.pop(peer_addr, None)) is not None:
            wakeup.set()

    async def close(self) -> None:
        """Stop every sender loop and release all queued requests."""
        self._closed = True
        sender_tokens = list(self._sender_tokens.values())
        for peer_addr in list(self._wakeups):
            self.forget_peer(peer_addr)
        for token in sender_tokens:
            await self._task_runner.cancel(token)

    def _start_sender(self, peer_addr: PeerAddress) -> asyncio.Event:
        wakeup = asyncio.Event()
        self._wakeups[peer_addr] = wakeup
        # TaskRunner binds an alias to the FIRST callable run under it, so
        # a shared alias would route a second outbox's runs through the
        # first outbox's loop; the alias is unique to this node's outbox.
        run = self._task_runner.run(
            self._send_loop,
            peer_addr,
            wakeup,
            alias=f"raft_peer_outbox:{self._node_id}",
        )
        self._sender_tokens[peer_addr] = f"{run.task_name}:{run.run_id}"
        return wakeup

    async def _send_loop(self, peer_addr: PeerAddress, wakeup: asyncio.Event) -> None:
        # The loop owns exactly one wakeup event: once the peer is
        # forgotten (or forgotten and re-added, which starts a new loop
        # with a new event) this loop exits instead of draining requests
        # that now belong to its successor.
        while True:
            await wakeup.wait()
            if self._wakeups.get(peer_addr) is not wakeup:
                return
            wakeup.clear()
            if batch := self._pending.pop(peer_addr, None):
                await self._deliver(peer_addr, list(batch.values()))

    async def _deliver(self, peer_addr: PeerAddress, requests: list[RaftRequest]) -> None:
        outcomes = await asyncio.gather(
            *(self._exchange(peer_addr, request) for request in requests),
            return_exceptions=True,
        )
        for request, outcome in zip(requests, outcomes):
            if isinstance(outcome, BaseException):
                await self._logger.log(
                    RaftWarning(
                        message=(
                            f"Raft {type(request).__name__} exchange with "
                            f"{peer_addr[0]}:{peer_addr[1]} failed: {outcome!r}"
                        ),
                        node_id=self._node_id,
                        job_id=request.job_id,
                        term=request.term,
                    )
                )
