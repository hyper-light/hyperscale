"""
Two-sided worker deregistration under multi-process SIM: a manager
legitimately evicts its (healthy) worker mid-run, the eviction NOTICE
tells the worker, the worker re-registers, and a second job dispatches
and completes on the recovered registration. Byte-identical replay.

Before the notice existed, eviction was one-sided: the manager forgot
the worker but kept acking its SWIM probes, so the worker believed the
relationship healthy forever and never re-registered — a stuck-then-
recovered worker was silently lost for good. This scenario drives the
production eviction chokepoint (``_handle_worker_failure`` — the same
path SWIM-death and deadline eviction funnel through) at t=30 and
asserts the full recovery loop.
"""

from tests.simulation.harness.sim.multiprocess import SimulationCoordinator
from tests.simulation.harness.sim.multiprocess.eviction_recovery_demo import (
    evicting_manager_entry,
    two_job_client_entry,
)
from tests.simulation.harness.sim.multiprocess.worker_manager_demo import (
    worker_entry,
)

_CEILING = 90.0
_EVICT_AT = 30.0
_SECOND_SUBMIT_AT = 45.0


def _run_eviction_recovery() -> dict:
    coordinator = SimulationCoordinator(
        latency=0.01, max_virtual_time=_CEILING, seed=47
    )
    coordinator.add_process(
        "manager",
        evicting_manager_entry,
        "sim-mgr",
        9000,
        9001,
        "sim-dc",
        _EVICT_AT,
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
        two_job_client_entry,
        "sim-cli",
        9500,
        ("sim-mgr", 9000),
        _SECOND_SUBMIT_AT,
    )
    return coordinator.run()


def test_evicted_worker_is_notified_reregisters_and_serves_again():
    results = _run_eviction_recovery()

    manager_log = results["manager"]
    client_log = results["client"]

    # The eviction actually fired against a registered worker and
    # completed without error.
    evictions = [entry for entry in manager_log if entry[0] == "evicting"]
    assert len(evictions) == 1, manager_log
    assert not [
        entry
        for entry in manager_log
        if entry[0] in ("evict-skipped-no-worker", "evict-error")
    ], manager_log

    # Event-anchored eviction evidence: the registry count WAS 0 the
    # moment the production failure path returned...
    post_evict_counts = [
        entry[1] for entry in manager_log if entry[0] == "post-evict-count"
    ]
    assert post_evict_counts == [0], manager_log

    # ...and the notice -> re-register loop closed so fast the 0.5s
    # count watcher never even sampled the gap: its transitions stay
    # [0, 1] (registration), with no 0 sample after the eviction. A
    # dropped-or-lost notice would leave 0s from t=30.5 onward.
    count_transitions = [
        entry[1] for entry in manager_log if entry[0] == "worker-count"
    ]
    assert count_transitions == [0, 1], manager_log

    # Both jobs completed — the second on the recovered registration.
    for job_tag in ("job1", "job2"):
        finished = [
            entry for entry in client_log if entry[0] == f"{job_tag}-finished"
        ]
        assert len(finished) == 1, client_log
        assert finished[0][1] == "completed", client_log
    job2_finished_time = [
        entry for entry in client_log if entry[0] == "job2-finished"
    ][0][2]
    assert job2_finished_time < _CEILING

    # The worker ran BOTH workflows (active count rose twice).
    worker_log = results["worker"]
    activations = [
        entry
        for entry in worker_log
        if entry[0] == "workflows-active" and entry[1] > 0
    ]
    assert len(activations) == 2, worker_log


def test_eviction_recovery_is_replay_deterministic():
    # Eviction timing, notice delivery, re-registration, and both job
    # schedules must land on identical virtual timestamps across two
    # independent same-seed runs.
    assert _run_eviction_recovery() == _run_eviction_recovery()
