"""
A client that submits at boot is accepted one retry after the election
-- the gateless L2 topology (client -> lone manager -> worker, seed 101).

THE REGRESSION THIS PINS: a lone manager refuses a boot-time submission
("Not DC leader, retry at leader: unknown") until its election stands it
for leader, a randomized candidacy wait of up to
LEADER_ELECTION_TIMEOUT_JITTER (Raft section 5.2). The refusal carried no
retry hint, so the client fell back to its un-hinted back-off ladder,
whose base had become one overload sample interval: probed, it asked at
0.00 and 1.00 (both refused, the leader came at 1.17) and next at 3.45.
The manager now tells the client when its election next decides
(``LocalLeaderElection.seconds_until_next_decision``), and the client
waits that hint plus up to one more of jitter: accepted at 1.729.

THE BOUND, derived. Let D be the instant the manager becomes leader
(``leader-elected``, its become-leader callback) and m0 >= 0 the instant
it refuses the client's first attempt with the hint h = D - m0. The
client waits h * (1 + r), r in [0, 1), so its next attempt reaches the
manager no earlier than D and before m0 + 2h <= 2D, plus the exchange's
round trips: accepted <= D + (D - m0) + round trips <= 2D + 2 round
trips -- the election deadline plus one retry. It holds only while the
election is the last condition to clear, so the run's own rows must
show the cluster formed and the worker registered by D.

THE MUTATION: with the election's hint stripped (the manager answers
"leader unknown" with no hint, as before), the same run is accepted
outside the bound.
"""

from tests.simulation.harness.sim.multiprocess import SimulationCoordinator
from tests.simulation.harness.sim.multiprocess.boot_submission_demo import (
    boot_submission_manager_entry,
)
from tests.simulation.harness.sim.multiprocess.recovery_faults_demo import (
    recovery_dispatch_client_entry,
)
from tests.simulation.harness.sim.multiprocess.worker_manager_demo import (
    worker_entry,
)
from tests.simulation.oracle import JobStatusOracle

_SEED = 101
_LATENCY = 0.01
_CEILING = 30.0
_WAIT_TIMEOUT_SECONDS = 20.0

# One submission exchange, request to answer, in this harness: measured
# 0.04 for the refused first attempt (0.000 -> 0.040) and for the
# accepted one alike.
_SUBMISSION_ROUND_TRIP_SECONDS = 4 * _LATENCY


def _run_boot_submission(strip_election_retry_hint: bool) -> dict:
    coordinator = SimulationCoordinator(
        latency=_LATENCY, max_virtual_time=_CEILING, seed=_SEED
    )
    coordinator.add_process(
        "manager",
        boot_submission_manager_entry,
        "sim-mgr",
        9000,
        9001,
        "sim-dc",
        strip_election_retry_hint,
    )
    coordinator.add_process(
        "worker",
        worker_entry,
        "sim-wkr",
        9000,
        9001,
        "sim-dc",
        ("sim-mgr", 9000),
        2,
    )
    coordinator.add_process(
        "client",
        recovery_dispatch_client_entry,
        "sim-cli",
        9500,
        ("sim-mgr", 9000),
        _WAIT_TIMEOUT_SECONDS,
    )
    return coordinator.run()


def _milestone(process_log: list, name: str) -> float:
    rows = [row for row in process_log if row[0] == name]
    assert rows, (name, process_log)
    return rows[0][-1]


def _milestone_row(process_log: list, name: str) -> tuple:
    rows = [row for row in process_log if row[0] == name]
    assert len(rows) == 1, (name, process_log)
    return rows[0]


def _accepted_and_bound(results: dict) -> tuple[float, float, float]:
    """The client's acceptance instant, the leader-elected instant D, and
    the bound 2D + two round trips -- after checking the election was the
    last condition to clear."""
    manager_log = results["manager"]
    leader_elected_at = _milestone(manager_log, "leader-elected")
    assert _milestone(manager_log, "cluster-formed") <= leader_elected_at, manager_log
    assert _milestone(manager_log, "worker-registered") <= leader_elected_at, manager_log
    accepted_at = _milestone(results["client"], "job-submitted")
    bound = 2 * leader_elected_at + 2 * _SUBMISSION_ROUND_TRIP_SECONDS
    return accepted_at, leader_elected_at, bound


def test_a_boot_submission_is_accepted_within_one_retry_of_the_election():
    """The client is accepted no sooner than the election and no later
    than the election deadline plus one hinted retry (module docstring),
    and the job runs to completion."""
    results = _run_boot_submission(strip_election_retry_hint=False)
    accepted_at, leader_elected_at, bound = _accepted_and_bound(results)

    assert not [row for row in results["client"] if row[0] == "submit-rejected"], results["client"]
    assert leader_elected_at <= accepted_at <= bound, (
        f"accepted at {accepted_at}, election at {leader_elected_at}, "
        f"bound {bound} (measured 1.728973): {results['client']}"
    )
    (_tag, final_status, _finished_at) = _milestone_row(results["client"], "job-finished")
    assert final_status == "completed", results["client"]
    assert not JobStatusOracle().check_client_log(results["client"]), results["client"]


def test_without_the_election_hint_the_boot_submission_misses_the_bound():
    """Mutation check: strip the election's retry hint and the same run --
    the same election instant -- is accepted outside the bound, on the
    un-hinted back-off ladder."""
    results = _run_boot_submission(strip_election_retry_hint=True)
    accepted_at, leader_elected_at, bound = _accepted_and_bound(results)

    assert accepted_at > bound, (
        f"accepted at {accepted_at} within the bound {bound} even without "
        f"the election's hint (election at {leader_elected_at}): the bound "
        f"no longer measures the hint: {results['client']}"
    )


def test_boot_submission_is_replay_deterministic():
    assert _run_boot_submission(False) == _run_boot_submission(False)
