"""
Fault injection: the worker host dies mid-job.

Same five-process dispatch topology; at virtual 19.0 — job in flight —
the worker AND both of its executor children are SIGKILLed at the same
instant (the host-death model: everything on the box goes silent at
once). No process the manager can reach holds the job anymore.

From there every hop is unchanged production code on the manager: SWIM
probes toward the dead worker go unanswered, suspicion escalates, the
dead-worker reap empties the registry (the ``worker-lost`` milestone);
the ceiling is sized generously past the detector's sustained-silence
latency because detection timing MOVES when the topology changes (the
WAL-enabled manager consumes different jitter draws);
the in-flight job can never complete and terminates through the
manager's timeout machinery, whose terminal push resolves the client's
``wait_for_job`` — the job fails loudly instead of hanging. The whole
faulted run replays byte-identically.
"""

from tests.simulation.harness.sim.multiprocess import SimulationCoordinator
from tests.simulation.harness.sim.multiprocess.job_dispatch_demo import (
    dispatch_client_entry,
)
from tests.simulation.harness.sim.multiprocess.worker_manager_demo import (
    manager_entry,
    worker_entry,
)

_CEILING = 150.0
_KILL_AT = 19.0


def _run_with_worker_kill() -> dict:
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
    coordinator.schedule_kill("worker", at_time=_KILL_AT)
    coordinator.schedule_kill("executor-sim-wkr-9009", at_time=_KILL_AT)
    coordinator.schedule_kill("executor-sim-wkr-9011", at_time=_KILL_AT)
    return coordinator.run()


def test_worker_kill_fails_job_and_manager_reaps_the_worker():
    results = _run_with_worker_kill()

    # The whole worker host is gone: no result rows for any of it.
    assert "worker" not in results
    assert "executor-sim-wkr-9009" not in results
    assert "executor-sim-wkr-9011" not in results

    manager_log = results["manager"]
    client_log = results["client"]

    # The manager registered the worker, then observed its death (SWIM
    # suspicion + dead-worker reap) before the ceiling.
    registered = [entry for entry in manager_log if entry[0] == "worker-registered"]
    lost = [entry for entry in manager_log if entry[0] == "worker-lost"]
    assert len(registered) == 1, manager_log
    assert len(lost) == 1, manager_log
    # The failure detector's design bound: ~38s of sustained silence
    # (LHM-stretched suspicion) before declaring death. Assert the
    # BOUND, not merely "before the ceiling": at one intermediate tree
    # state detection drifted to 2-3x this bound and a generous
    # ceiling hid it. 20s floor guards against false-instant death.
    detection_latency = lost[0][1] - _KILL_AT
    assert 20.0 <= detection_latency <= 70.0, (
        f"death detection latency {detection_latency}s is outside the "
        f"design bound (~38s +/- margin): {manager_log}"
    )

    # The in-flight job terminated loudly for the client — no silent hang.
    finished = [entry for entry in client_log if entry[0] == "job-finished"]
    assert len(finished) == 1, client_log
    (_tag, job_status, finished_time) = finished[0]
    assert job_status != "completed", client_log
    assert _KILL_AT < finished_time < _CEILING


def test_worker_kill_is_replay_deterministic():
    assert _run_with_worker_kill() == _run_with_worker_kill()
