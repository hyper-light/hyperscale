"""
AD-38 Part 8: a manager answers a job status read at the consistency the
reader asks.

The status query was a bare job id, answered from whatever the asked
manager held -- every read EVENTUAL, whatever the doc promised. Now:

* EVENTUAL reads, and reads of a terminal status (which never changes),
  are answered from what the manager holds.
* The job's leader answers the rest -- STRONG only once a quorum of its
  peers accepted its state again (they accept only the current fenced
  leader), with nothing answered when the quorum does not.
* A follower answers a SESSION read its leader's last sync is at least as
  new as, and a BOUNDED_STALENESS read whose view is provably young
  enough (time since it arrived plus a sync's longest transit); anything
  else it passes to the leader, once -- a forwarded read is never passed
  on again.
* A bare job id (an older client) is still an EVENTUAL read.

Driven through the real ``ManagerServer.job_status`` handler with its
collaborators stubbed.
"""

from types import SimpleNamespace

import pytest

from hyperscale.distributed.models import GlobalJobStatus, JobStatusQuery, ReadConsistency
from hyperscale.distributed.nodes.manager.server import ManagerServer

JOB_ID = "job-1"
LEADER_ADDR = ("10.0.0.9", 9000)
SYNC_TRANSIT_BOUND_SECONDS = 2.0


class SteppedClock:
    def __init__(self) -> None:
        self.now = 500.0

    def monotonic(self) -> float:
        return self.now


class SilentLogger:
    async def log(self, entry: object) -> None:
        return None


def make_manager(
    *,
    leads_job: bool,
    status: str = "running",
    leader_view: tuple[int, float, float] | None = None,
    quorum_accepts: bool = True,
    leader_answer: bytes = b"",
) -> tuple[ManagerServer, list[tuple[tuple[str, int], JobStatusQuery]], SteppedClock]:
    clock = SteppedClock()
    forwarded: list[tuple[tuple[str, int], JobStatusQuery]] = []
    manager = object.__new__(ManagerServer)
    manager._clock = clock
    manager._host, manager._tcp_port = "10.0.0.1", 9000
    manager._udp_logger = SilentLogger()
    manager._config = SimpleNamespace(tcp_timeout_short_seconds=SYNC_TRANSIT_BOUND_SECONDS, tcp_timeout_standard_seconds=5.0)
    manager._job_ledger = None
    manager._job_manager = SimpleNamespace(
        get_job_by_id=lambda job_id: SimpleNamespace(status=status, elapsed_seconds=lambda: 3.0)
        if job_id == JOB_ID
        else None
    )
    manager._aggregate_job_progress = lambda job: (7, 1, 2.5)
    manager._leases = SimpleNamespace(is_job_leader=lambda job_id: leads_job, get_fence_token=lambda job_id: 4)
    manager._manager_state = SimpleNamespace(
        get_job_leader_view=lambda job_id: leader_view,
        get_job_leader_addr=lambda job_id: LEADER_ADDR,
    )

    async def sync_job_state_to_peers(job_id, job, require_quorum=False) -> bool:
        assert require_quorum
        return quorum_accepts

    async def send_tcp(addr, action, payload, timeout):
        assert action == "job_status"
        forwarded.append((addr, JobStatusQuery.load(payload)))
        return leader_answer, 0

    manager._sync_job_state_to_peers = sync_job_state_to_peers
    manager.send_tcp = send_tcp
    return manager, forwarded, clock


async def read(manager: ManagerServer, **query_fields) -> GlobalJobStatus | None:
    response = await manager.job_status(("10.0.0.50", 8500), JobStatusQuery(job_id=JOB_ID, **query_fields).dump(), 0)
    return GlobalJobStatus.load(response) if response else None


@pytest.mark.asyncio
@pytest.mark.parametrize("consistency", list(ReadConsistency))
async def test_a_terminal_status_answers_every_level_from_any_copy(consistency: ReadConsistency) -> None:
    manager, forwarded, _clock = make_manager(leads_job=False, status="completed")

    answer = await read(manager, consistency=consistency.value)

    assert answer is not None and answer.status == "completed"
    assert forwarded == []


@pytest.mark.asyncio
async def test_an_eventual_read_is_answered_by_any_holder() -> None:
    manager, forwarded, _clock = make_manager(leads_job=False)

    answer = await read(manager, consistency=ReadConsistency.EVENTUAL.value)

    assert answer is not None and (answer.total_completed, answer.total_failed) == (7, 1)
    assert forwarded == []


@pytest.mark.asyncio
async def test_a_bare_job_id_is_an_eventual_read() -> None:
    manager, _forwarded, _clock = make_manager(leads_job=False)

    response = await manager.job_status(("10.0.0.50", 8500), JOB_ID.encode(), 0)

    assert GlobalJobStatus.load(response).status == "running"


@pytest.mark.asyncio
@pytest.mark.parametrize("quorum_accepts", [True, False])
async def test_the_leader_answers_a_strong_read_only_once_a_quorum_accepts_it(quorum_accepts: bool) -> None:
    manager, forwarded, clock = make_manager(leads_job=True, quorum_accepts=quorum_accepts)

    answer = await read(manager, consistency=ReadConsistency.STRONG.value)

    if quorum_accepts:
        assert answer is not None and (answer.fence_token, answer.view_time) == (4, clock.now)
    else:
        assert answer is None
    assert forwarded == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("observed", "answered_here"),
    [((3, 120.0), True), ((4, 99.0), True), ((4, 100.0), True), ((4, 101.0), False), ((5, 0.0), False)],
)
async def test_a_follower_answers_a_session_read_its_leaders_view_is_as_new_as(
    observed: tuple[int, float], answered_here: bool
) -> None:
    manager, forwarded, _clock = make_manager(leads_job=False, leader_view=(4, 100.0, 450.0))

    answer = await read(
        manager,
        consistency=ReadConsistency.SESSION.value,
        observed_fence_token=observed[0],
        observed_view_time=observed[1],
    )

    if answered_here:
        assert answer is not None and (answer.fence_token, answer.view_time) == (4, 100.0)
        assert forwarded == []
    else:
        ((forwarded_to, forwarded_query),) = forwarded
        assert forwarded_to == LEADER_ADDR and forwarded_query.forwarded


@pytest.mark.asyncio
async def test_a_follower_answers_a_bounded_read_only_within_the_bound_and_transit() -> None:
    manager, forwarded, clock = make_manager(leads_job=False, leader_view=(4, 100.0, 495.0))
    # Arrived 5s ago; a sync takes up to 2s more to arrive: at most 7s old.
    assert clock.now - 495.0 + SYNC_TRANSIT_BOUND_SECONDS == 7.0

    within = await read(manager, consistency=ReadConsistency.BOUNDED_STALENESS.value, max_staleness_seconds=7.0)
    assert within is not None and forwarded == []

    beyond = await read(manager, consistency=ReadConsistency.BOUNDED_STALENESS.value, max_staleness_seconds=6.9)
    assert beyond is None and len(forwarded) == 1


@pytest.mark.asyncio
async def test_a_forwarded_read_is_never_passed_on_again() -> None:
    manager, forwarded, _clock = make_manager(leads_job=False, leader_view=None)

    answer = await read(manager, consistency=ReadConsistency.STRONG.value, forwarded=True)

    assert answer is None and forwarded == []
