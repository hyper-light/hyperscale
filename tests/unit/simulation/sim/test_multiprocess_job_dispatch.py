"""
A real workflow dispatched end to end under multi-process SIM.

Five real OS processes: a ``HyperscaleClient`` child submits
``SimPingWorkflow`` to a ``ManagerServer`` child (production retry until
leadership + capacity), the manager dispatches to the ``WorkerServer``
child, the worker fans out to its two executor-pool children, the
executors run the workflow's VUs against virtual time
(``WorkflowRunner``'s duration now elapses on the loop clock), and the
completion flows back client-ward — every hop over the deterministic
coordinator boundary, the whole thing replaying byte-identically.

This is the client-facing API of the entire distributed system
exercised under SIM: the exit ramp toward seed-driven fault schedules
over real job traffic.
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


def _run_dispatch() -> dict:
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
    return coordinator.run()


def test_workflow_dispatches_end_to_end_under_sim():
    results = _run_dispatch()

    # The worker's executor pool was admitted as coordinator children.
    assert "executor-sim-wkr-9009" in results
    assert "executor-sim-wkr-9011" in results

    client_log = results["client"]
    submitted_times = [
        entry[1] for entry in client_log if entry[0] == "job-submitted"
    ]
    finished = [entry for entry in client_log if entry[0] == "job-finished"]

    assert len(submitted_times) == 1, client_log
    assert len(finished) == 1, client_log

    (_tag, job_status, finished_time) = finished[0]
    assert job_status == "completed", client_log
    assert submitted_times[0] < finished_time < _CEILING


def test_workflow_dispatch_is_replay_deterministic():
    assert _run_dispatch() == _run_dispatch()
