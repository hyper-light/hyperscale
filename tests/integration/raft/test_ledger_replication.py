"""
AD-38 REGIONAL: LedgerReplicator committing job-ledger entries
through the job's per-job Raft group.

Pinned against real ManagerRaftIntegrations (real RaftNodes, outbox and
state machine), a real TaskRunner and an in-memory transport with
send_tcp semantics: a replicate call made by the group's leader or by a
follower (forwarded) commits and every member mirrors the event in its
JobLedgerReplica; without a majority nothing is reported committed; a
group destroyed mid-replication ends the call with False; a forwarded
proposal to a non-leader is refused without appending.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from hyperscale.distributed.env import Env
from hyperscale.distributed.ledger.events.event_type import JobEventType
from hyperscale.distributed.ledger.events.job_event import JobCreated
from hyperscale.distributed.ledger.job_ledger_replica import JobLedgerReplica
from hyperscale.distributed.ledger.wal.entry_state import WALEntryState
from hyperscale.distributed.ledger.wal.wal_entry import WALEntry
from hyperscale.distributed.raft import LedgerReplicator
from hyperscale.distributed.raft.models.commands import ledger_append_command
from hyperscale.distributed.nodes.manager.raft_integration import ManagerRaftIntegration
from hyperscale.distributed.raft.models import LedgerProposal
from hyperscale.distributed.raft.raft_node import ELECTION_TIMEOUT_MAX, HEARTBEAT_INTERVAL
from hyperscale.distributed.runtime import RealClock
from hyperscale.distributed.taskex import TaskRunner
from tests.unit.distributed.hlc.hlc_factory import new_hybrid_logical_clock

JOB_ID = "job-1"
# Liveness ceiling: a working group elects and commits within about one
# election round on an instant transport; allow several.
LIVENESS_CEILING_SECONDS = ELECTION_TIMEOUT_MAX * 20


def _member_id(addr: tuple[str, int]) -> str:
    return f"manager-{addr[1]}"


class LedgerCluster:
    """Three managers' Raft integrations + ledger replicators, in memory."""

    def __init__(self) -> None:
        self.addresses = [("127.0.0.1", 9000 + index) for index in range(3)]
        # One TaskRunner per member, as in production (each server owns one).
        self.task_runners = {addr: TaskRunner(0, Env()) for addr in self.addresses}
        self.unreachable: set[tuple[str, int]] = set()
        self.forwarded: list[tuple[str, int]] = []
        self.replicas = {addr: JobLedgerReplica() for addr in self.addresses}
        self.integrations: dict[tuple[str, int], ManagerRaftIntegration] = {}
        self.replicators: dict[tuple[str, int], LedgerReplicator] = {}
        logger = MagicMock()
        logger.log = AsyncMock()
        for addr in self.addresses:
            self.integrations[addr] = ManagerRaftIntegration(
                clock=new_hybrid_logical_clock(),
                ledger_replica=self.replicas[addr],
                node_id=_member_id(addr),
                job_manager=MagicMock(),
                leadership_tracker=MagicMock(),
                logger=logger,
                task_runner=self.task_runners[addr],
                send_tcp=self._send_tcp,
                node_addr=addr,
                configured_cluster_size=len(self.addresses),
            )
            self.replicators[addr] = LedgerReplicator(
                consensus=self.integrations[addr].consensus,
                build_command=ledger_append_command,
                node_id=_member_id(addr),
                send_tcp=self._send_tcp,
                forward_method="raft_ledger_proposal",
                forward_timeout_seconds=LIVENESS_CEILING_SECONDS,
                clock=RealClock(),
                logger=logger,
            )

    async def _send_tcp(self, addr, method, data, timeout=None):
        if addr in self.unreachable:
            return ConnectionRefusedError(f"{addr} unreachable")
        integration = self.integrations[addr]
        match method:
            case "raft_request_vote":
                reply = await integration.handle_request_vote(data)
            case "raft_append_entries":
                reply = await integration.handle_append_entries(data)
            case "raft_ledger_proposal":
                self.forwarded.append(addr)
                result = await self.replicators[addr].handle_forwarded(LedgerProposal.load(data))
                reply = result.dump()
        return reply if reply is not None else b""

    async def start(self) -> None:
        for addr, integration in self.integrations.items():
            peers = [peer for peer in self.addresses if peer != addr]
            integration.set_initial_membership(
                {_member_id(peer) for peer in peers},
                {_member_id(peer): peer for peer in peers},
            )
            # As the job leadership announcement does on every member.
            await integration.consensus.create_job_raft(JOB_ID)
            integration.start()

    async def stop(self) -> None:
        for integration in self.integrations.values():
            await integration.stop()
        for runner in self.task_runners.values():
            await runner.shutdown()

    def node(self, addr: tuple[str, int]):
        return self.integrations[addr].consensus.get_node(JOB_ID)

    async def wait_until(self, predicate) -> None:
        async with asyncio.timeout(LIVENESS_CEILING_SECONDS):
            while not predicate():
                await asyncio.sleep(HEARTBEAT_INTERVAL)


@pytest.fixture
async def task_runner():
    runner = TaskRunner(0, Env())
    yield runner
    await runner.shutdown()


async def _job_created_entry() -> WALEntry:
    event = JobCreated(
        job_id=JOB_ID,
        hlc=new_hybrid_logical_clock().now(),
        fence_token=1,
        spec_hash=b"spec",
        assigned_datacenters=("dc-east",),
        requestor_id="client-1",
    )
    return WALEntry(
        lsn=0,
        hlc=event.hlc,
        state=WALEntryState.PENDING,
        event_type=JobEventType.JOB_CREATED,
        payload=event.to_bytes(),
    )


def _mirrored_everywhere(cluster: LedgerCluster) -> bool:
    return all(
        [event_type for event_type, _ in replica.history(JOB_ID)] == [JobEventType.JOB_CREATED]
        for replica in cluster.replicas.values()
    )


@pytest.mark.asyncio
async def test_replicate_commits_and_every_member_mirrors_the_event() -> None:
    cluster = LedgerCluster()
    await cluster.start()
    try:
        replicating_member = cluster.addresses[0]
        async with asyncio.timeout(LIVENESS_CEILING_SECONDS):
            committed = await cluster.replicators[replicating_member].replicate(
                await _job_created_entry()
            )

        assert committed is True
        await cluster.wait_until(lambda: _mirrored_everywhere(cluster))
    finally:
        await cluster.stop()


@pytest.mark.asyncio
async def test_follower_forwards_to_the_groups_leader() -> None:
    cluster = LedgerCluster()
    await cluster.start()
    try:
        leader_addr = cluster.addresses[1]
        await cluster.node(leader_addr).start_election()
        await cluster.wait_until(
            lambda: all(cluster.node(addr).current_leader == _member_id(leader_addr) for addr in cluster.addresses)
        )

        follower = cluster.addresses[0]
        async with asyncio.timeout(LIVENESS_CEILING_SECONDS):
            committed = await cluster.replicators[follower].replicate(await _job_created_entry())

        assert committed is True
        assert leader_addr in cluster.forwarded
        await cluster.wait_until(lambda: _mirrored_everywhere(cluster))
    finally:
        await cluster.stop()


@pytest.mark.asyncio
async def test_without_a_majority_nothing_is_reported_committed() -> None:
    cluster = LedgerCluster()
    cluster.unreachable.update(cluster.addresses[1:])
    await cluster.start()
    try:
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(ELECTION_TIMEOUT_MAX * 4):
                await cluster.replicators[cluster.addresses[0]].replicate(await _job_created_entry())

        assert all(replica.job_count == 0 for replica in cluster.replicas.values())
    finally:
        await cluster.stop()


@pytest.mark.asyncio
async def test_group_destroyed_mid_replication_ends_the_call() -> None:
    cluster = LedgerCluster()
    cluster.unreachable.update(cluster.addresses[1:])
    await cluster.start()
    try:
        replicating_member = cluster.addresses[0]
        replication = asyncio.create_task(
            cluster.replicators[replicating_member].replicate(await _job_created_entry())
        )
        await asyncio.sleep(ELECTION_TIMEOUT_MAX)

        await cluster.integrations[replicating_member].consensus.destroy_job_raft(JOB_ID)

        async with asyncio.timeout(LIVENESS_CEILING_SECONDS):
            assert await replication is False
    finally:
        await cluster.stop()


@pytest.mark.asyncio
async def test_forwarded_proposal_to_a_non_leader_is_refused_unappended() -> None:
    cluster = LedgerCluster()
    await cluster.start()
    try:
        leader_addr = cluster.addresses[0]
        await cluster.node(leader_addr).start_election()
        await cluster.wait_until(
            lambda: all(cluster.node(addr).current_leader == _member_id(leader_addr) for addr in cluster.addresses)
        )
        entry = await _job_created_entry()
        follower = cluster.addresses[2]

        result = await cluster.replicators[follower].handle_forwarded(
            LedgerProposal(job_id=JOB_ID, event_type=int(entry.event_type), payload=entry.payload)
        )

        assert (result.appended, result.committed) == (False, False)
        assert cluster.replicas[follower].job_count == 0
    finally:
        await cluster.stop()
