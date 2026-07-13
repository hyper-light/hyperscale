"""
Manager power-loss restart with job RESUME — the full Phase 7 stack
proving itself end to end.

The manager is power-lost MID-JOB (the workflow is running on the
worker when the disk collapses to durable content) and reboots from
that surviving disk after a down window. The new generation:

* replays its WAL and finds the job ACTIVE (JobLedger recovery);
* loads the persisted submission payload and RESUMES the job — the
  submit tail re-runs (dispatcher registration, Raft job group,
  leadership, dispatch), deliberately at the END of start(), when that
  machinery exists;
* rejoins SWIM strictly above its pre-restart incarnation (the
  persisted incarnation store), which is how the worker's rejoin watch
  can detect restarts the failure detector never sees;
* re-admits the worker (heartbeat-driven dispatch kicks + the
  connect-timeout fix — a dial to the still-down manager must time out
  rather than wedge the registration retry loop);
* re-dispatches and COMPLETES the job: the client — which never
  restarted — observes completion for the SAME job id it submitted.

Workflow execution is at-least-once across the restart (the worker's
pre-restart run finished into a dead manager and its result was lost);
the client-facing outcome is exactly-once.

The fsync_reorder variant reboots from a disk where a seeded SUBSET of
un-fsynced volatile segments survived (torn, out of order). The
durable job records were group-committed (fsynced) so recovery reads
them unaffected — the reorder debris must simply never break replay.

Timing pinned by probe (seed 101): submit 9.21, dispatch 9.25, restart
9.4 (mid-run), gen-2 up 54.4, worker re-registered 60.4, completion
observed 60.71.
"""

from tests.simulation.harness.sim.multiprocess import SimulationCoordinator
from tests.simulation.harness.sim.multiprocess.job_dispatch_demo import (
    dispatch_client_entry,
)
from tests.simulation.harness.sim.multiprocess.worker_manager_demo import (
    manager_entry,
    worker_entry,
)

_RESTART_AT = 9.4
_DOWN_SECONDS = 45.0
_CEILING = 220.0


def _run_restart_resume(fsync_reorder_seed: int | None = None) -> dict:
    coordinator = SimulationCoordinator(
        latency=0.01, max_virtual_time=_CEILING, seed=101
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
    coordinator.schedule_restart(
        "manager",
        _RESTART_AT,
        down_seconds=_DOWN_SECONDS,
        fsync_reorder_seed=fsync_reorder_seed,
    )
    return coordinator.run()


def _assert_job_completed_across_restart(results: dict) -> None:
    client_log = results["client"]

    submitted = [entry for entry in client_log if entry[0] == "job-submitted"]
    assert submitted, client_log
    assert submitted[0][1] < _RESTART_AT, (
        "probe-pinned timing drifted: the job must be submitted BEFORE "
        f"the restart instant: {client_log}"
    )

    completions = [
        entry
        for entry in client_log
        if entry[0] == "status-seen" and entry[1] == "completed"
    ]
    assert completions, (
        f"client never observed completion across the restart: {client_log}"
    )
    assert completions[0][2] > _RESTART_AT + _DOWN_SECONDS, (
        "completion must come from the RESUMED generation (after the "
        f"down window): {client_log}"
    )

    # Generation results: gen-1 saw the original registration; the
    # final generation re-registered the worker after reboot.
    first_generation = results["manager.gen1"]
    assert any(
        entry[0] == "worker-registered" for entry in first_generation
    ), first_generation
    final_generation = results["manager"]
    assert any(
        entry[0] == "worker-registered" for entry in final_generation
    ), final_generation


def test_job_resumes_and_completes_across_manager_restart():
    results = _run_restart_resume()
    _assert_job_completed_across_restart(results)


def test_restart_resume_is_replay_deterministic():
    assert _run_restart_resume() == _run_restart_resume()


def test_job_resumes_through_fsync_reordered_crash_debris():
    """Power loss with reordering-crash semantics: the rebooted disk
    carries a seeded subset of torn un-fsynced segments. Durable
    (group-committed) job records are unaffected by construction —
    recovery must read straight through the debris and resume."""
    results = _run_restart_resume(fsync_reorder_seed=7)
    _assert_job_completed_across_restart(results)


def test_fsync_reordered_restart_is_replay_deterministic():
    assert _run_restart_resume(fsync_reorder_seed=7) == _run_restart_resume(
        fsync_reorder_seed=7
    )
