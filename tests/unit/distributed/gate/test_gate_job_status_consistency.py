"""
AD-38 Part 8 at the gate tier: a gate answers EVENTUAL reads and reads of
a terminal status from what it holds; the job's leader gate answers the
rest -- STRONG once a quorum of gates committed its replica again; a gate
that follows the job passes the read to the leader, once.

Driven through the real ``GateServer._answer_job_status_query`` with its
collaborators stubbed.
"""

from types import SimpleNamespace

import pytest

from hyperscale.distributed.models import GlobalJobStatus, JobStatusQuery, ReadConsistency
from hyperscale.distributed.nodes.gate.server import GateServer

JOB_ID = "job-1"
LEADER_GATE = ("10.0.1.9", 9100)


def make_gate(
    *, leads_job: bool, status: str = "running", quorum_commits: bool = True
) -> tuple[GateServer, list[JobStatusQuery], list[object]]:
    forwarded: list[JobStatusQuery] = []
    revisions: list[object] = []
    gate = object.__new__(GateServer)
    gate._host, gate._tcp_port = "10.0.1.1", 9100
    gate._tcp_timeout_standard = 5.0
    gate._clock = SimpleNamespace(monotonic=lambda: 700.0)

    async def gather_job_status(job_id: str) -> GlobalJobStatus | None:
        return GlobalJobStatus(job_id=job_id, status=status, fence_token=3) if job_id == JOB_ID else None

    async def revise_committed_replica(job_id, revise, peer_addrs, quorum_size) -> bool:
        revisions.append(revise("committed-replica"))
        return quorum_commits

    async def send_tcp(addr, action, payload, timeout):
        assert (addr, action) == (LEADER_GATE, "job_status")
        forwarded.append(JobStatusQuery.load(payload))
        return b"", 0

    gate._gather_job_status = gather_job_status
    gate._job_leadership_tracker = SimpleNamespace(
        is_leader=lambda job_id: leads_job, get_leader_addr=lambda job_id: LEADER_GATE
    )
    gate._replication_coordinator = SimpleNamespace(revise_committed_replica=revise_committed_replica)
    gate._modular_state = SimpleNamespace(get_active_peers_list=lambda: [("10.0.1.2", 9100)])
    gate._quorum_size = lambda: 2
    gate._send_tcp = send_tcp
    return gate, forwarded, revisions


async def answer(gate: GateServer, consistency: ReadConsistency, **fields) -> GlobalJobStatus | None:
    response = await gate._answer_job_status_query(JobStatusQuery(job_id=JOB_ID, consistency=consistency.value, **fields))
    return GlobalJobStatus.load(response) if response else None


@pytest.mark.asyncio
async def test_eventual_and_terminal_reads_are_answered_by_any_gate() -> None:
    gate, forwarded, _revisions = make_gate(leads_job=False)
    assert (await answer(gate, ReadConsistency.EVENTUAL)).status == "running"

    gate, forwarded, _revisions = make_gate(leads_job=False, status="completed")
    assert (await answer(gate, ReadConsistency.STRONG)).status == "completed"
    assert forwarded == []


@pytest.mark.asyncio
@pytest.mark.parametrize("quorum_commits", [True, False])
async def test_the_leader_gate_answers_strong_once_a_quorum_recommits_its_replica(quorum_commits: bool) -> None:
    gate, forwarded, revisions = make_gate(leads_job=True, quorum_commits=quorum_commits)

    result = await answer(gate, ReadConsistency.STRONG)

    # The unchanged replica is committed again (a revision of None would
    # commit nothing and confirm nothing).
    assert revisions == ["committed-replica"]
    assert (result is not None) is quorum_commits
    if result is not None:
        assert (result.fence_token, result.view_time) == (3, 700.0)
    assert forwarded == []


@pytest.mark.asyncio
@pytest.mark.parametrize("consistency", [ReadConsistency.SESSION, ReadConsistency.BOUNDED_STALENESS, ReadConsistency.STRONG])
async def test_a_following_gate_passes_the_read_to_the_leader_once(consistency: ReadConsistency) -> None:
    gate, forwarded, _revisions = make_gate(leads_job=False)

    assert await answer(gate, consistency) is None
    ((forwarded_query,),) = [forwarded]
    assert forwarded_query.forwarded

    assert await answer(gate, consistency, forwarded=True) is None
    assert len(forwarded) == 1
