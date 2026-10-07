"""
A manager refusing a job submission for a transient reason it can time
tells the submitter when to come back (``JobAck.retry_after_seconds``),
from what it holds:

* no known leader -- when its election next decides
  (``LocalLeaderElection.seconds_until_next_decision``); a known leader
  needs no hint, the submitter redirects to it;
* no quorum -- one SWIM probe period, as peers are confirmed alive again;
* cluster membership not formed -- its next formation round
  (``ClusterMembership.seconds_until_next_formation_round``);
* clock fenced -- one clock-offset probe interval, as offsets are
  re-measured.

What no clock of the manager's governs carries no hint: no worker
registered yet (a registration arrives when it does), the manager still
syncing at boot, and a read-only cluster (an operator's decision, not a
transient refusal).
"""

from types import SimpleNamespace

from hyperscale.distributed.env import Env
from hyperscale.distributed.models import JobAck, ManagerState
from hyperscale.distributed.nodes.manager.server import ManagerServer

SETTINGS = Env()
JOB_ID = "job-retry-hint"
LEADER_ADDRESS = ("10.0.0.6", 9000)
# Whatever the manager's own mechanisms read when asked: the hint must be
# their reading, passed through.
ELECTION_READING_SECONDS = SETTINGS.LEADER_ELECTION_TIMEOUT_JITTER / 3
FORMATION_READING_SECONDS = SETTINGS.CLUSTER_FORMATION_INTERVAL_SECONDS / 4


def make_manager(
    *,
    state: ManagerState = ManagerState.ACTIVE,
    clock_fenced: bool = False,
    is_leader: bool = True,
    known_leader: tuple[str, int] | None = None,
    has_quorum: bool = True,
    formed: bool = True,
    read_only: bool = False,
    worker_count: int = 1,
) -> ManagerServer:
    manager = object.__new__(ManagerServer)
    manager._env = SETTINGS
    manager._manager_state = SimpleNamespace(
        manager_state_enum=state,
        get_worker_count=lambda: worker_count,
    )
    manager._clock_offset_monitor = SimpleNamespace(is_fenced=clock_fenced)
    manager._leader_election = SimpleNamespace(
        state=SimpleNamespace(is_leader=lambda: is_leader),
        seconds_until_next_decision=lambda: ELECTION_READING_SECONDS,
    )
    manager._resolve_dc_leader_addr = lambda: known_leader
    manager._leadership = SimpleNamespace(has_quorum=lambda: has_quorum)
    manager._cluster_membership = SimpleNamespace(
        formed=formed,
        read_only=read_only,
        seconds_until_next_formation_round=lambda: FORMATION_READING_SECONDS,
    )
    return manager


def refusal(manager: ManagerServer) -> JobAck:
    answer = manager._job_admission_refusal(JOB_ID)
    assert answer is not None
    ack = JobAck.load(answer)
    assert not ack.accepted
    return ack


def test_a_follower_without_a_known_leader_says_when_its_election_next_decides() -> None:
    ack = refusal(make_manager(is_leader=False))
    assert ack.leader_addr is None
    assert ack.retry_after_seconds == ELECTION_READING_SECONDS


def test_a_follower_naming_its_leader_gives_no_hint() -> None:
    ack = refusal(make_manager(is_leader=False, known_leader=LEADER_ADDRESS))
    assert tuple(ack.leader_addr) == LEADER_ADDRESS
    assert ack.retry_after_seconds == 0.0


def test_a_leader_without_quorum_says_one_probe_period() -> None:
    ack = refusal(make_manager(has_quorum=False))
    assert ack.retry_after_seconds == float(SETTINGS.SWIM_UDP_POLL_INTERVAL)


def test_an_unformed_cluster_says_when_its_next_formation_round_begins() -> None:
    ack = refusal(make_manager(formed=False))
    assert ack.retry_after_seconds == FORMATION_READING_SECONDS


def test_a_fenced_clock_says_one_offset_probe_interval() -> None:
    ack = refusal(make_manager(clock_fenced=True))
    assert ack.retry_after_seconds == SETTINGS.HLC_OFFSET_PROBE_INTERVAL_SECONDS


def test_untimed_refusals_carry_no_hint() -> None:
    for manager in (
        make_manager(worker_count=0),
        make_manager(state=ManagerState.SYNCING),
        make_manager(read_only=True),
    ):
        assert refusal(manager).retry_after_seconds == 0.0


def test_an_admissible_job_is_not_refused() -> None:
    assert make_manager()._job_admission_refusal(JOB_ID) is None
