"""
Fault injection with recovery: the job's worker dies mid-flight and the
manager re-dispatches to a worker that joins AFTER the loss — the job
COMPLETES.

Topology forces the story deterministically: worker-a is the only
worker alive at submission, so the job provably dispatches to it; the
moment worker-a reports the workflow active, worker-a and both of its
executor children are SIGKILLed (host death) one coordinator latency
later — an EVENT-TRIGGERED kill, so it lands mid-run under every
schedule (the ping workflow runs ~0.5-0.75s, and when it starts moves
with each seed's startup/election timing: a kill pinned at virtual
7.75 found the job already completed on worker-a under other
schedules, leaving nothing to retry). Worker-b is only admitted 13s
after the kill — virtual seconds of ZERO cluster capacity,
exactly the regime where the dispatcher used to busy-spin (13/N).
Production machinery then recovers unaided: the workflow times out /
its worker is declared dead, the retry requeues it, worker-b registers
(~2s after its start) and signals capacity, the dispatcher wakes from its consumed-event
wait, re-dispatches, worker-b's executor pool runs the workflow, and
the client's ``wait_for_job`` resolves ``completed``. Replay-identical.
"""

import pytest

from tests.simulation.harness.sim.multiprocess import SimulationCoordinator
from tests.simulation.harness.sim.multiprocess.job_dispatch_demo import (
    dispatch_client_entry,
)
from tests.simulation.harness.sim.multiprocess.worker_manager_demo import (
    manager_entry,
    worker_entry,
)

_SEED = 23
# The original seed first, then a sweep of schedules whose workflow
# start times differ (probed 2026-10-06).
_SWEEP_SEEDS = (_SEED, 1, 4, 9, 14)
_LATENCY = 0.01
_CEILING = 90.0
_WORKER_B_JOIN_DELAY_SECONDS = 13.0
_WORKER_A_PROCESS_IDS = (
    "worker-a",
    "executor-sim-wkr-a-9009",
    "executor-sim-wkr-a-9011",
)


def _is_workflow_activation(row: tuple) -> bool:
    """A worker milestone showing a workflow active on it."""
    return row[0] == "workflows-active" and row[1] > 0


def _run_with_retry(seed: int = _SEED) -> tuple[dict, dict[str, float]]:
    """Run the scenario; returns its results and the fault instants the
    activation trigger derived (``kill_at``, ``worker_b_start``)."""
    coordinator = SimulationCoordinator(
        latency=_LATENCY, max_virtual_time=_CEILING, seed=seed
    )
    fault_instants: dict[str, float] = {}

    def kill_worker_a_mid_run(activation_row: tuple) -> None:
        kill_at = activation_row[2] + _LATENCY
        worker_b_start = kill_at + _WORKER_B_JOIN_DELAY_SECONDS
        fault_instants.update(kill_at=kill_at, worker_b_start=worker_b_start)
        for process_id in _WORKER_A_PROCESS_IDS:
            coordinator.schedule_kill(process_id, at_time=kill_at)
        coordinator.schedule_admission(
            "worker-b",
            worker_b_start,
            worker_entry,
            "sim-wkr-b",
            9000,
            9001,
            "sim-dc",
            ("sim-mgr", 9000),
            2,
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
        "client", dispatch_client_entry, "sim-cli", 9500, ("sim-mgr", 9000)
    )
    coordinator.schedule_on_event(
        "worker-a", _is_workflow_activation, kill_worker_a_mid_run
    )
    return coordinator.run(), fault_instants


@pytest.mark.parametrize("seed", _SWEEP_SEEDS)
def test_job_retries_onto_late_joining_worker_and_completes(seed: int):
    results, fault_instants = _run_with_retry(seed)
    kill_at = fault_instants["kill_at"]
    worker_b_start = fault_instants["worker_b_start"]

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
    assert activations[0][2] > worker_b_start

    finished = [entry for entry in client_log if entry[0] == "job-finished"]
    assert len(finished) == 1, client_log
    (_tag, job_status, finished_time) = finished[0]
    assert job_status == "completed", client_log
    assert kill_at < finished_time < _CEILING


def test_worker_retry_is_replay_deterministic():
    assert _run_with_retry() == _run_with_retry()
