"""
Fault injection over the coordinator boundary: SIGKILL a pool executor
mid-workflow and watch the production recovery chain fire on virtual
time.

Same five-process topology as the dispatch scenario, plus one scheduled
fault: executor ``executor-sim-wkr-9009`` dies at virtual 19.0 — after
the workflow dispatch lands on the worker (18.75) and before it drains.
The kill is a real SIGKILL to a real OS process; the coordinator
removes the victim from the route map (silence semantics) and surfaces
``(kill_time, process_id, exitcode)`` to the worker's exit-code
snapshot at exactly 19.0.

From there every hop is unchanged production code: the worker's
pool-health loop (0.25s cadence) observes the non-``None`` exitcode ->
``_fail_active_workflows_for_pool_exit`` fails the active workflow with
``Worker subprocess exited during workflow execution`` and sends the
FAILED ``WorkflowFinalResult`` to the manager -> the manager records it,
completes the job as failed, and pushes the terminal status to the
client -> ``wait_for_job`` resolves. And the whole faulted run replays
byte-identically.
"""

from tests.simulation.harness.sim.multiprocess import SimulationCoordinator
from tests.simulation.harness.sim.multiprocess.job_dispatch_demo import (
    dispatch_client_entry,
)
from tests.simulation.harness.sim.multiprocess.worker_manager_demo import (
    manager_entry,
    worker_entry,
)

_CEILING = 60.0


def _run_with_executor_kill() -> dict:
    coordinator = SimulationCoordinator(
        latency=0.01, max_virtual_time=_CEILING, seed=23
    )
    coordinator.add_process(
        "manager", manager_entry, "sim-mgr", 9000, 9001, "sim-dc"
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
        "client", dispatch_client_entry, "sim-cli", 9500, ("sim-mgr", 9000)
    )
    coordinator.schedule_kill("executor-sim-wkr-9009", at_time=19.0)
    return coordinator.run()


def test_executor_kill_fails_job_and_notifies_client():
    results = _run_with_executor_kill()

    # The victim produced no result row; its sibling survived to STOP.
    assert "executor-sim-wkr-9009" not in results
    assert "executor-sim-wkr-9011" in results

    client_log = results["client"]
    submitted = [entry for entry in client_log if entry[0] == "job-submitted"]
    finished = [entry for entry in client_log if entry[0] == "job-finished"]

    assert len(submitted) == 1, client_log
    assert len(finished) == 1, client_log

    (_tag, job_status, finished_time) = finished[0]
    # The pool-exit failure path terminates the job as failed — not
    # completed, not hung until the ceiling.
    assert job_status == "failed", client_log
    assert submitted[0][1] < finished_time < _CEILING


def test_executor_kill_is_replay_deterministic():
    assert _run_with_executor_kill() == _run_with_executor_kill()
