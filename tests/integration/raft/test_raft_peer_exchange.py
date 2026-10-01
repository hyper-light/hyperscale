"""
Raft RPC exchange between manager peers (ManagerRaftIntegration + RaftPeerOutbox).

The peer's Raft handler answers in the TCP reply, but the sender discarded
``send_tcp``'s return value, so no vote or append ack ever reached a
candidate or leader: measured, a 3-member group churned to term 23 in 5s
without electing, and nothing could ever commit. Routing the reply inline
would also have deadlocked — RaftNode sends while holding its lock and the
response handler takes the same lock — and a slow peer would have stalled
every job's tick.

Pinned against real integrations, a real TaskRunner and an in-memory
transport with send_tcp semantics: multi-member groups elect one leader,
stop churning terms, commit, and replicate the commit to followers; a hung
or unreachable peer does not stop a majority from committing; the outbox
stays bounded under a stuck peer; forgotten peers' sender loops end; and
shutdown leaves no sender tasks behind.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from hyperscale.distributed.ledger.job_ledger_replica import JobLedgerReplica
from hyperscale.distributed.env import Env
from hyperscale.distributed.nodes.manager.raft_integration import ManagerRaftIntegration
from hyperscale.distributed.raft import RaftPeerOutbox
from hyperscale.distributed.raft.models import AppendEntries, RaftCommandType, RequestVote
from hyperscale.distributed.raft.models.commands import RaftCommand
from hyperscale.distributed.raft.raft_node import ELECTION_TIMEOUT_MAX, HEARTBEAT_INTERVAL
from hyperscale.distributed.taskex import TaskRunner
from tests.unit.distributed.hlc.hlc_factory import new_hybrid_logical_clock

JOB_ID = "job-1"
HANDLERS = {
    "raft_request_vote": "handle_request_vote",
    "raft_append_entries": "handle_append_entries",
}
# Liveness ceiling for elections and commits: randomized timeouts make a
# split vote possible on an instant transport, so allow several full
# election rounds; a working group finishes in about one.
ELECTION_ROUNDS_CEILING = 20
LIVENESS_CEILING_SECONDS = ELECTION_TIMEOUT_MAX * ELECTION_ROUNDS_CEILING


def _logger() -> MagicMock:
    logger = MagicMock()
    logger.log = AsyncMock()
    return logger


def _member_id(addr: tuple[str, int]) -> str:
    return f"manager-{addr[1]}"


class InMemoryCluster:
    """N manager Raft integrations joined by a send_tcp-shaped transport."""

    def __init__(self, member_count: int) -> None:
        self.addresses = [("127.0.0.1", 9000 + index) for index in range(member_count)]
        # One TaskRunner per member, as in production (each server owns one).
        self.task_runners = {addr: TaskRunner(0, Env()) for addr in self.addresses}
        self.integrations: dict[tuple[str, int], ManagerRaftIntegration] = {}
        # hung / isolated: failed in BOTH directions (a stalled process, a
        # symmetric partition). deaf: can send, cannot receive (an
        # asymmetric partition) -- the disruptive-member case.
        self.hung_addresses: set[tuple[str, int]] = set()
        self.isolated_addresses: set[tuple[str, int]] = set()
        self.deaf_addresses: set[tuple[str, int]] = set()
        self.release_hung = asyncio.Event()
        self.logger = _logger()
        for addr in self.addresses:
            self.integrations[addr] = ManagerRaftIntegration(
                clock=new_hybrid_logical_clock(),
                ledger_replica=JobLedgerReplica(),
                node_id=_member_id(addr),
                job_manager=MagicMock(),
                leadership_tracker=MagicMock(),
                logger=self.logger,
                task_runner=self.task_runners[addr],
                send_tcp=self._sender(addr),
                node_addr=addr,
                configured_cluster_size=member_count,
            )

    def _sender(self, sender_addr: tuple[str, int]):
        async def send_tcp(addr, method, data, timeout=None):
            if {sender_addr, addr} & self.isolated_addresses or addr in self.deaf_addresses:
                return ConnectionRefusedError(f"{addr} unreachable")
            if {sender_addr, addr} & self.hung_addresses:
                await self.release_hung.wait()
                return asyncio.TimeoutError(f"{addr} timed out")
            reply = await getattr(self.integrations[addr], HANDLERS[method])(data)
            return reply if reply is not None else b""

        return send_tcp

    async def start(self) -> None:
        for addr, integration in self.integrations.items():
            peers = [peer for peer in self.addresses if peer != addr]
            integration.set_initial_membership(
                {_member_id(peer) for peer in peers},
                {_member_id(peer): peer for peer in peers},
            )
            await integration.consensus.create_job_raft(JOB_ID)
            integration.start()

    async def stop(self) -> None:
        self.release_hung.set()
        for integration in self.integrations.values():
            await integration.stop()
        for runner in self.task_runners.values():
            await runner.shutdown()

    def nodes(self, among: list[tuple[str, int]] | None = None):
        return [self.integrations[addr].consensus.get_node(JOB_ID) for addr in among or self.addresses]

    async def wait_for_leader(self, among: list[tuple[str, int]] | None = None) -> tuple[str, int]:
        candidates = among or self.addresses
        async with asyncio.timeout(LIVENESS_CEILING_SECONDS):
            while True:
                if leaders := [addr for addr in candidates if self.integrations[addr].consensus.get_node(JOB_ID).is_leader()]:
                    return leaders[0]
                await asyncio.sleep(HEARTBEAT_INTERVAL)


@pytest.fixture
async def task_runner():
    runner = TaskRunner(0, Env())
    yield runner
    await runner.shutdown()


def _sender_loop_tasks() -> list[asyncio.Task]:
    """Live asyncio tasks running an outbox sender loop (TaskRunner wraps
    each run's call in ``Run._execute``, so match on the run's call)."""
    return [
        task
        for task in asyncio.all_tasks()
        if not task.done()
        and (frame := task.get_coro().cr_frame) is not None
        and getattr(getattr(frame.f_locals.get("self"), "call", None), "__name__", "") == "_send_loop"
    ]


async def _propose(cluster: InMemoryCluster, leader: tuple[str, int]) -> tuple[bool, int]:
    command = RaftCommand(command_type=RaftCommandType.NO_OP, job_id=JOB_ID)
    return await cluster.integrations[leader].consensus.propose_command(JOB_ID, command)


@pytest.mark.asyncio
@pytest.mark.parametrize("member_count", [3, 5])
async def test_group_elects_one_leader_commits_and_followers_apply(
    member_count: int,
) -> None:
    cluster = InMemoryCluster(member_count)
    await cluster.start()
    try:
        leader = await cluster.wait_for_leader()
        committed, index = await _propose(cluster, leader)

        assert committed is True
        # Every member, followers included, must APPLY the entry -- each
        # member's own tick loop drives that, so this also proves every
        # member's loop runs.
        async with asyncio.timeout(LIVENESS_CEILING_SECONDS):
            while any(node._last_applied < index for node in cluster.nodes()):
                await asyncio.sleep(HEARTBEAT_INTERVAL)

        nodes = cluster.nodes()
        assert sum(node.is_leader() for node in nodes) == 1
        terms_after_commit = {node.current_term for node in nodes}
        assert len(terms_after_commit) == 1

        await asyncio.sleep(ELECTION_TIMEOUT_MAX * 2)
        assert {node.current_term for node in cluster.nodes()} == terms_after_commit
    finally:
        await cluster.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["hung", "isolated"])
async def test_majority_commits_despite_a_failed_peer(failure: str) -> None:
    cluster = InMemoryCluster(3)
    failed_peer = cluster.addresses[-1]
    getattr(cluster, f"{failure}_addresses").add(failed_peer)
    healthy = [addr for addr in cluster.addresses if addr != failed_peer]
    await cluster.start()
    try:
        leader = await cluster.wait_for_leader(among=healthy)
        committed, _ = await _propose(cluster, leader)

        assert committed is True
    finally:
        await cluster.stop()


@pytest.mark.asyncio
async def test_deaf_member_cannot_depose_a_live_leader() -> None:
    """A follower that can send but not receive never hears heartbeats and
    times out again and again. Without PreVote each timeout bumped its
    term and its RequestVote forced the live leader to step down; with
    PreVote no member grants while it hears the leader, so the term and
    the leader hold."""
    cluster = InMemoryCluster(3)
    await cluster.start()
    try:
        leader = await cluster.wait_for_leader()
        deaf_follower = next(addr for addr in cluster.addresses if addr != leader)
        term_before = cluster.integrations[leader].consensus.get_node(JOB_ID).current_term
        cluster.deaf_addresses.add(deaf_follower)

        await asyncio.sleep(ELECTION_TIMEOUT_MAX * 10)

        leader_node = cluster.integrations[leader].consensus.get_node(JOB_ID)
        assert leader_node.is_leader()
        assert leader_node.current_term == term_before
        committed, _ = await _propose(cluster, leader)
        assert committed is True
    finally:
        await cluster.stop()


@pytest.mark.asyncio
async def test_unreachable_peer_is_logged_not_swallowed() -> None:
    cluster = InMemoryCluster(3)
    cluster.isolated_addresses.add(cluster.addresses[-1])
    await cluster.start()
    try:
        await cluster.wait_for_leader(among=cluster.addresses[:-1])

        logged = [call.args[0].message for call in cluster.logger.log.await_args_list]
        assert any("got no response" in message and "9002" in message for message in logged)
    finally:
        await cluster.stop()


@pytest.mark.asyncio
async def test_shutdown_leaves_no_sender_loops() -> None:
    cluster = InMemoryCluster(3)
    cluster.hung_addresses.add(cluster.addresses[-1])
    await cluster.start()
    await cluster.wait_for_leader(among=cluster.addresses[:-1])
    assert _sender_loop_tasks()

    await cluster.stop()
    await asyncio.sleep(0)

    assert _sender_loop_tasks() == []
    for integration in cluster.integrations.values():
        assert integration._outbox.peer_count == 0
        assert integration._outbox.pending_count == 0


def _vote(job_id: str, term: int) -> RequestVote:
    return RequestVote(job_id=job_id, term=term, candidate_id="manager-a", last_log_index=0, last_log_term=0)


def _append(job_id: str, term: int) -> AppendEntries:
    return AppendEntries(
        job_id=job_id,
        term=term,
        leader_id="manager-a",
        prev_log_index=0,
        prev_log_term=0,
        entries=[],
        leader_commit=0,
    )


@pytest.mark.asyncio
async def test_outbox_stays_bounded_behind_a_stuck_peer(task_runner: TaskRunner) -> None:
    stuck = asyncio.Event()
    delivered: list[object] = []

    async def exchange(addr, request):
        delivered.append(request)
        await stuck.wait()

    outbox = RaftPeerOutbox(exchange=exchange, task_runner=task_runner, logger=_logger(), node_id="manager-a")
    job_ids = [f"job-{index}" for index in range(10)]
    peer = ("127.0.0.1", 9001)
    try:
        for term in range(1, 1_001):
            for job_id in job_ids:
                outbox.enqueue(peer, _vote(job_id, term))
                outbox.enqueue(peer, _append(job_id, term))
            await asyncio.sleep(0)

        assert outbox.pending_count <= len(job_ids) * 2
        latest_pending_terms = {request.term for request in outbox._pending[peer].values()}
        assert latest_pending_terms == {1_000}
    finally:
        stuck.set()
        await outbox.close()


@pytest.mark.asyncio
async def test_forgotten_peer_loop_ends_and_readded_peer_gets_a_fresh_loop(task_runner: TaskRunner) -> None:
    delivered: list[tuple[tuple[str, int], int]] = []

    async def exchange(addr, request):
        delivered.append((addr, request.term))

    outbox = RaftPeerOutbox(exchange=exchange, task_runner=task_runner, logger=_logger(), node_id="manager-a")
    peer = ("127.0.0.1", 9001)
    try:
        outbox.enqueue(peer, _vote(JOB_ID, 1))
        await asyncio.sleep(0.01)
        outbox.forget_peer(peer)
        await asyncio.sleep(0.01)
        assert _sender_loop_tasks() == []

        outbox.enqueue(peer, _vote(JOB_ID, 2))
        await asyncio.sleep(0.01)

        assert delivered == [(peer, 1), (peer, 2)]
        assert len(_sender_loop_tasks()) == 1
    finally:
        await outbox.close()
        await asyncio.sleep(0)
        assert _sender_loop_tasks() == []


@pytest.mark.asyncio
async def test_failing_exchange_is_logged_and_the_loop_keeps_delivering(task_runner: TaskRunner) -> None:
    logger = _logger()
    delivered: list[int] = []

    async def exchange(addr, request):
        if request.term == 1:
            raise ValueError("malformed reply")
        delivered.append(request.term)

    outbox = RaftPeerOutbox(exchange=exchange, task_runner=task_runner, logger=logger, node_id="manager-a")
    peer = ("127.0.0.1", 9001)
    try:
        outbox.enqueue(peer, _vote(JOB_ID, 1))
        await asyncio.sleep(0.01)
        outbox.enqueue(peer, _vote(JOB_ID, 2))
        await asyncio.sleep(0.01)

        assert delivered == [2]
        warning = logger.log.await_args_list[0].args[0]
        assert "malformed reply" in warning.message
    finally:
        await outbox.close()


@pytest.mark.asyncio
async def test_enqueue_after_close_is_dropped(task_runner: TaskRunner) -> None:
    outbox = RaftPeerOutbox(exchange=AsyncMock(), task_runner=task_runner, logger=_logger(), node_id="manager-a")
    await outbox.close()

    outbox.enqueue(("127.0.0.1", 9001), _vote(JOB_ID, 1))

    assert outbox.pending_count == 0
    assert outbox.peer_count == 0
