"""
AD-38 GLOBAL for the gate job ledger (LedgerRegionSpan): a committed entry
is GLOBAL once the gates holding it span two regions.

Pinned against real GateRaftIntegrations (real RaftNodes, outbox, gate
state machine), the shared LedgerReplicator, a TaskRunner per gate and an
in-memory transport with send_tcp semantics, three gates in regions
east/east/west: REGIONAL commits and every gate mirrors the event; GLOBAL
holds once the west gate acknowledges; with the west gate isolated REGIONAL
still commits on the east majority but GLOBAL is never reported; a gate
that is not the group's leader asks the leader where the entry is held; a
single-region tier reports GLOBAL unachievable at once.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from hyperscale.distributed.raft.store.volatile_raft_storage import VolatileRaftStorage
from hyperscale.distributed.env import Env
from hyperscale.distributed.ledger.events.event_type import JobEventType
from hyperscale.distributed.ledger.events.job_event import JobCreated
from hyperscale.distributed.ledger.job_ledger_replica import JobLedgerReplica
from hyperscale.distributed.ledger.wal.entry_state import WALEntryState
from hyperscale.distributed.ledger.wal.wal_entry import WALEntry
from hyperscale.distributed.nodes.gate.ledger_region_span import LedgerRegionSpan
from hyperscale.distributed.nodes.gate.raft_integration import GateRaftIntegration
from hyperscale.distributed.raft import LedgerReplicator
from hyperscale.distributed.raft.models import LedgerPlacementQuery, LedgerProposal
from hyperscale.distributed.raft.raft_node import ELECTION_TIMEOUT_MAX, HEARTBEAT_INTERVAL
from hyperscale.distributed.runtime import RealClock
from hyperscale.distributed.taskex import TaskRunner
from tests.unit.distributed.hlc.hlc_factory import new_hybrid_logical_clock

JOB_ID = "job-1"
LIVENESS_CEILING_SECONDS = ELECTION_TIMEOUT_MAX * 20
RAFT_HANDLERS = {
    "gate_raft_request_vote": "handle_request_vote",
    "gate_raft_append_entries": "handle_append_entries",
}


def _gate_id(addr: tuple[str, int]) -> str:
    return f"gate-{addr[1]}"


class GateLedgerCluster:
    """Gate Raft integrations + ledger replicators + region spans, in memory."""

    def __init__(self, regions: list[str]) -> None:
        self.addresses = [("127.0.0.1", 9100 + index) for index in range(len(regions))]
        # The gate cluster's committed membership (AD-52): every gate, formed
        # from the start.
        self.cluster_members = {_gate_id(addr): addr for addr in self.addresses}
        self.region_by_gate = {_gate_id(addr): region for addr, region in zip(self.addresses, regions)}
        self.task_runners = {addr: TaskRunner(0, Env()) for addr in self.addresses}
        self.isolated: set[tuple[str, int]] = set()
        self.queried_leaders: list[tuple[str, int]] = []
        self.replicas = {addr: JobLedgerReplica() for addr in self.addresses}
        self.integrations: dict[tuple[str, int], GateRaftIntegration] = {}
        self.replicators: dict[tuple[str, int], LedgerReplicator] = {}
        self.spans: dict[tuple[str, int], LedgerRegionSpan] = {}
        logger = MagicMock()
        logger.log = AsyncMock()
        for addr in self.addresses:
            send_tcp = self._sender(addr)
            self.integrations[addr] = GateRaftIntegration(
                clock=new_hybrid_logical_clock(),
                may_lead=lambda: True,
                ledger_replica=self.replicas[addr],
                cluster_size=lambda: len(self.addresses),
                proposal_timeout_seconds=LIVENESS_CEILING_SECONDS,
                node_id=_gate_id(addr),
                logger=logger,
                task_runner=self.task_runners[addr],
                send_tcp=self._tuple_reply(send_tcp),
                request_timeout_seconds=Env().GATE_TCP_TIMEOUT_STANDARD,
                cluster_members=lambda: self.cluster_members,
                storage=VolatileRaftStorage(),
            )
            consensus = self.integrations[addr].consensus
            self.replicators[addr] = LedgerReplicator(
                consensus=consensus,
                node_id=_gate_id(addr),
                send_tcp=send_tcp,
                forward_method="gate_raft_ledger_proposal",
                forward_timeout_seconds=LIVENESS_CEILING_SECONDS,
                clock=RealClock(),
                logger=logger,
            )
            self.spans[addr] = LedgerRegionSpan(
                consensus=consensus,
                node_id=_gate_id(addr),
                region_of=self.region_by_gate.get,
                tier_regions=lambda: set(self.region_by_gate.values()),
                send_tcp=send_tcp,
                query_method="gate_raft_ledger_placement",
                query_timeout_seconds=LIVENESS_CEILING_SECONDS,
                clock=RealClock(),
                logger=logger,
            )

    @staticmethod
    def _tuple_reply(send_tcp):
        """The gate Raft integration's send_tcp returns (reply, clock)."""
        async def send(addr, method, data, timeout=None):
            return await send_tcp(addr, method, data, timeout), 0

        return send

    def _sender(self, sender_addr: tuple[str, int]):
        async def send_tcp(addr, method, data, timeout=None):
            if {sender_addr, addr} & self.isolated:
                return ConnectionRefusedError(f"{addr} unreachable")
            match method:
                case "gate_raft_ledger_proposal":
                    reply = (await self.replicators[addr].handle_forwarded(LedgerProposal.load(data))).dump()
                case "gate_raft_ledger_placement":
                    self.queried_leaders.append(addr)
                    reply = (await self.spans[addr].handle_query(LedgerPlacementQuery.load(data))).dump()
                case _:
                    reply = await getattr(self.integrations[addr], RAFT_HANDLERS[method])(data)
            return reply if reply is not None else b""

        return send_tcp

    async def start(self) -> None:
        for integration in self.integrations.values():
            # As the committed job replica does on every gate: the group's
            # voters are the cluster's committed members.
            await integration.consensus.create_job_raft(JOB_ID, frozenset(self.cluster_members))
            await integration.start()

    async def stop(self) -> None:
        for integration in self.integrations.values():
            await integration.stop()
        for runner in self.task_runners.values():
            await runner.shutdown()

    def node(self, addr: tuple[str, int]):
        return self.integrations[addr].consensus.get_node(JOB_ID)

    async def elect(self, leader_addr: tuple[str, int]) -> None:
        await self.node(leader_addr).start_election()
        reachable = [addr for addr in self.addresses if addr not in self.isolated]
        async with asyncio.timeout(LIVENESS_CEILING_SECONDS):
            while not all(self.node(addr).current_leader == _gate_id(leader_addr) for addr in reachable):
                await asyncio.sleep(HEARTBEAT_INTERVAL)


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


@pytest.mark.asyncio
async def test_committed_entry_becomes_global_once_a_second_region_holds_it() -> None:
    cluster = GateLedgerCluster(["east", "east", "west"])
    await cluster.start()
    try:
        leader = cluster.addresses[0]
        await cluster.elect(leader)
        entry = await _job_created_entry()

        async with asyncio.timeout(LIVENESS_CEILING_SECONDS):
            assert await cluster.replicators[leader].replicate(entry) is True
            assert await cluster.spans[leader].replicate(entry) is True

        holders = cluster.spans[leader].holders(JOB_ID, entry.payload)
        assert {cluster.region_by_gate[holder] for holder in holders} == {"east", "west"}
    finally:
        await cluster.stop()


@pytest.mark.asyncio
async def test_isolated_second_region_leaves_the_entry_regional_only() -> None:
    cluster = GateLedgerCluster(["east", "east", "west"])
    west_gate = cluster.addresses[2]
    cluster.isolated.add(west_gate)
    await cluster.start()
    try:
        leader = cluster.addresses[0]
        await cluster.elect(leader)
        entry = await _job_created_entry()

        async with asyncio.timeout(LIVENESS_CEILING_SECONDS):
            assert await cluster.replicators[leader].replicate(entry) is True
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(ELECTION_TIMEOUT_MAX * 4):
                await cluster.spans[leader].replicate(entry)

        holders = cluster.spans[leader].holders(JOB_ID, entry.payload)
        assert {cluster.region_by_gate[holder] for holder in holders} == {"east"}
    finally:
        await cluster.stop()


@pytest.mark.asyncio
async def test_non_leader_asks_the_group_leader_where_the_entry_is() -> None:
    cluster = GateLedgerCluster(["east", "east", "west"])
    await cluster.start()
    try:
        leader = cluster.addresses[1]
        await cluster.elect(leader)
        follower = cluster.addresses[2]
        entry = await _job_created_entry()

        async with asyncio.timeout(LIVENESS_CEILING_SECONDS):
            assert await cluster.replicators[follower].replicate(entry) is True
            assert await cluster.spans[follower].replicate(entry) is True

        assert leader in cluster.queried_leaders
        assert cluster.spans[follower].holders(JOB_ID, entry.payload) == []
    finally:
        await cluster.stop()


@pytest.mark.asyncio
async def test_single_region_tier_reports_global_unachievable_at_once() -> None:
    cluster = GateLedgerCluster(["east", "east", "east"])
    await cluster.start()
    try:
        span = cluster.spans[cluster.addresses[0]]

        assert span.tier_spans_regions() is False
        async with asyncio.timeout(HEARTBEAT_INTERVAL):
            assert await span.replicate(await _job_created_entry()) is False
    finally:
        await cluster.stop()
