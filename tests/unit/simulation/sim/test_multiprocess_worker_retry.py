"""
Fault injection with recovery: the job's worker dies mid-flight and the
manager re-dispatches to a worker that joins AFTER the loss — the job
COMPLETES.

Topology forces the story deterministically: worker-a is the only
worker alive at submission, so the job provably dispatches to it
(active on worker-a from ~11.75); at virtual 12.0 worker-a and both of
its executor children are SIGKILLed (host death). Worker-b only begins
its startup at 25.0 — fifteen virtual seconds of ZERO cluster capacity,
exactly the regime where the dispatcher used to busy-spin (13/N).
Production machinery then recovers unaided: the workflow times out /
its worker is declared dead, the retry requeues it, worker-b registers
(~27) and signals capacity, the dispatcher wakes from its consumed-event
wait, re-dispatches, worker-b's executor pool runs the workflow, and
the client's ``wait_for_job`` resolves ``completed``. Replay-identical.
"""

from tests.simulation.harness.sim.multiprocess import SimulationCoordinator
from tests.simulation.harness.sim.multiprocess.job_dispatch_demo import (
    dispatch_client_entry,
)
from tests.simulation.harness.sim.multiprocess.worker_manager_demo import (
    manager_entry,
    worker_entry,
)

_CEILING = 90.0
_KILL_AT = 12.0
_WORKER_B_START = 25.0


def _run_with_retry() -> dict:
    coordinator = SimulationCoordinator(
        latency=0.01, max_virtual_time=_CEILING, seed=23
    )
    coordinator.add_process(
        "manager", manager_entry, "sim-mgr", 9000, 9001, "sim-dc"
    )
    coordinator.add_process(
        "worker-a",
        worker_entry,
        "sim-wkr-a",
        9000,
        9001,
        "sim-dc",
        ("sim-mgr", 9000),
        2,
    )
    coordinator.add_process(
        "worker-b",
        worker_entry,
        "sim-wkr-b",
        9000,
        9001,
        "sim-dc",
        ("sim-mgr", 9000),
        2,
        _WORKER_B_START,
    )
    coordinator.add_process(
        "client", dispatch_client_entry, "sim-cli", 9500, ("sim-mgr", 9000)
    )
    coordinator.schedule_kill("worker-a", at_time=_KILL_AT)
    coordinator.schedule_kill("executor-sim-wkr-a-9009", at_time=_KILL_AT)
    coordinator.schedule_kill("executor-sim-wkr-a-9011", at_time=_KILL_AT)
    return coordinator.run()


def test_job_retries_onto_late_joining_worker_and_completes():
    results = _run_with_retry()

    # The dead host has no result rows; the survivor and its pool do.
    assert "worker-a" not in results
    assert "executor-sim-wkr-a-9009" not in results
    assert "executor-sim-wkr-a-9011" not in results
    assert "executor-sim-wkr-b-9009" in results
    assert "executor-sim-wkr-b-9011" in results

    worker_b_log = results["worker-b"]
    client_log = results["client"]

    # Worker-b actually ran the retried workflow (its active count rose
    # after it joined) — the job could not have completed anywhere else.
    activations = [
        entry
        for entry in worker_b_log
        if entry[0] == "workflows-active" and entry[1] > 0
    ]
    assert len(activations) >= 1, worker_b_log
    assert activations[0][2] > _WORKER_B_START

    finished = [entry for entry in client_log if entry[0] == "job-finished"]
    assert len(finished) == 1, client_log
    (_tag, job_status, finished_time) = finished[0]
    assert job_status == "completed", client_log
    assert _KILL_AT < finished_time < _CEILING


def test_worker_retry_is_replay_deterministic():
    assert _run_with_retry() == _run_with_retry()
