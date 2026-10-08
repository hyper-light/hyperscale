"""
Job leadership announcements round-trip between gates, and the receiver
acks what it actually did.

The receiving gate acked ``accepted=True`` even for a claim it rejected
(a stale fence token), so an announcer could not tell its claim had
lost.

Round trip through the real pieces: the real coordinator's broadcast is
captured on the wire and fed to the real receiving endpoint over a real
JobLeadershipTracker -- the peer records the leader at its real address
(JobLeadershipAnnouncement aliases ``leader_addr`` to
``leader_host``/``leader_tcp_port``) and acks a stale claim as rejected.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from hyperscale.distributed.jobs import JobLeadershipTracker
from hyperscale.distributed.models import JobLeadershipAck
from hyperscale.distributed.nodes.gate.leadership_coordinator import GateLeadershipCoordinator
from hyperscale.distributed.nodes.gate.server import GateServer
from hyperscale.distributed.nodes.gate.state import GateRuntimeState

JOB = "job-1"
LEADER_ID = "gate-a-node-id"
LEADER_ADDR = ("10.0.0.1", 9000)
PEER_ADDR = ("10.0.0.2", 9000)
LEADER_FENCE_TOKEN = 3


class RunNow:
    """Runs scheduled coroutines inline so the send happens in the test."""

    def __init__(self) -> None:
        self.pending: list = []

    def run(self, call, *args, **kwargs) -> None:
        self.pending.append(call(*args, **kwargs))

    async def drain(self) -> None:
        while self.pending:
            await self.pending.pop(0)


async def broadcast_from_leader() -> bytes:
    leader_tracker: JobLeadershipTracker[int] = JobLeadershipTracker(node_id=LEADER_ID, node_addr=LEADER_ADDR)
    leader_tracker.assume_leadership(JOB, metadata=1, initial_token=LEADER_FENCE_TOKEN)
    sent: list[tuple] = []

    async def send_tcp(addr, action, data, timeout):
        sent.append((addr, action, data))
        return b"", 0

    task_runner = RunNow()
    coordinator = GateLeadershipCoordinator(
        state=GateRuntimeState(forward_throughput_interval_start=0.0),
        logger=SimpleNamespace(log=AsyncMock()),
        task_runner=task_runner,
        leadership_tracker=leader_tracker,
        get_node_id=lambda: SimpleNamespace(full=LEADER_ID, short="gate-a"),
        get_node_addr=lambda: LEADER_ADDR,
        send_tcp=send_tcp,
        get_active_peers=lambda: [PEER_ADDR],
        get_cluster_size=lambda: 2,
        peer_rpc_timeout_seconds=1.0,
    )
    await coordinator.broadcast_leadership(JOB, target_dc_count=1)
    await task_runner.drain()
    [(addr, action, data)] = sent
    assert (addr, action) == (PEER_ADDR, "job_leadership_announcement")
    return data


def make_peer_gate(tracker: JobLeadershipTracker) -> GateServer:
    gate = object.__new__(GateServer)
    gate._job_leadership_tracker = tracker
    gate._orphan_job_coordinator = SimpleNamespace(clear_orphaned_job=lambda job_id: None)
    gate._task_runner = SimpleNamespace(run=lambda *args, **kwargs: None)
    gate._udp_logger = SimpleNamespace(log=AsyncMock())
    gate._host, gate._tcp_port = PEER_ADDR
    gate._node_id = SimpleNamespace(full="gate-b-node-id", short="gate-b")
    return gate


@pytest.mark.asyncio
async def test_a_peer_records_the_announced_leader_at_its_real_address() -> None:
    announcement = await broadcast_from_leader()
    peer_tracker: JobLeadershipTracker[int] = JobLeadershipTracker(node_id="gate-b-node-id", node_addr=PEER_ADDR)

    reply = await GateServer.job_leadership_announcement(make_peer_gate(peer_tracker), LEADER_ADDR, announcement, 0)

    assert JobLeadershipAck.load(reply).accepted
    assert peer_tracker.get_leader(JOB) == LEADER_ID
    assert peer_tracker.get_leader_addr(JOB) == LEADER_ADDR
    assert peer_tracker.get_fencing_token(JOB) == LEADER_FENCE_TOKEN


@pytest.mark.asyncio
async def test_a_stale_claim_is_acked_as_rejected() -> None:
    announcement = await broadcast_from_leader()
    peer_tracker: JobLeadershipTracker[int] = JobLeadershipTracker(node_id="gate-b-node-id", node_addr=PEER_ADDR)
    peer_tracker.assume_leadership(JOB, metadata=1, initial_token=LEADER_FENCE_TOKEN + 1)

    reply = await GateServer.job_leadership_announcement(make_peer_gate(peer_tracker), LEADER_ADDR, announcement, 0)

    assert not JobLeadershipAck.load(reply).accepted
    assert peer_tracker.get_leader(JOB) == "gate-b-node-id"
