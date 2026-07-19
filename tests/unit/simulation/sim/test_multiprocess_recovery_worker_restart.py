"""
Checklist C4 — worker power-loss restart over the gateless L2 topology
(client -> manager -> worker, seed 101).

The coordinator refuses to restart a process with live spawned
children, but same-instant kills drain BEFORE restarts in ``_drive`` —
so killing both executor children ("executor-sim-wkr-9009" /
"executor-sim-wkr-9011", ids probe-verified) at the restart instant
makes the live-children check pass, and the gen-2 worker respawns its
pool under the SAME executor ids (the dead executors left the
coordinator's route/connection maps). This is the sanctioned
kill-executors-first recipe; no coordinator wrinkle surfaced in any
probe.

Probed timelines (seed 101):

* FRESH-JOB scenario (restart at 2.0, down 10): gen-1 worker registers
  at the 1.0 sample and dies idle; gen-2 boots at 12.0, respawns the
  pool, re-registers; the client's retrying submission is accepted at
  14.193062 — strictly AFTER the reboot — dispatch 14.25, drain 15.0,
  completion 14.773062. (Gen-2's ``worker-started`` milestone lands at
  15.623419, AFTER the dispatch it already served: ``start()`` returns
  only once the pool is fully settled, while registration + dispatch
  service begin mid-``start()``.)
* IN-FLIGHT scenario (restart at 9.5, down 20): dispatch 9.25, the
  gen-1 worker dies with the workflow ACTIVE (its generation log ends
  ``workflows-active 1`` — restarts, unlike kills, preserve the
  victim's milestones). The silence window [9.5, ~29.6] stays under
  the ~38s SWIM detection bound and the SAME-NodeId gen-2 re-registers
  at ~29.6-33.2, so the death-triggered retry path NEVER fires; the
  orphan scan (30s cadence) loses the race to the unified-timeout tick
  (30s cadence: 30.03, 60.03), which declares the job ``timeout`` at
  60.03 — the first tick past the deadline. The rebooted worker never
  executes (zero activations in gen-2's log) — pinned as TRUE current
  behavior: when an in-flight-reassignment path lands, the
  no-execution assertion is the one it flips.

KNOWN BUG (skip-pinned reproducer below): submitting a job ~8s AFTER
the rebooted worker re-registered drives the MANAGER's
``WorkflowDispatcher`` into a zero-delay self-rescheduling loop —
``_wait_dispatch_trigger`` / ``_consume_ready_signal`` — at virtual
22.03 (100% CPU livelock in production; the harness spin guard aborts
the child). A no-fault control with the same late submission completes
cleanly at 20.6, and the immediate-submission variant (accepted 2.2s
after re-registration) dispatches fine — the livelock needs BOTH the
worker restart and the later submission.
"""

import pytest

from tests.simulation.harness.sim.multiprocess import SimulationCoordinator
from tests.simulation.harness.sim.multiprocess.recovery_faults_demo import (
    recovery_dispatch_client_entry,
    recovery_manager_entry,
)
from tests.simulation.harness.sim.multiprocess.worker_manager_demo import (
    worker_entry,
)
from tests.simulation.oracle import JobStatusOracle

_SEED = 101
_WAIT_TIMEOUT_SECONDS = 200.0
_EXECUTOR_IDS = ("executor-sim-wkr-9009", "executor-sim-wkr-9011")

_FRESH_RESTART_AT = 2.0
_FRESH_DOWN_SECONDS = 10.0
_FRESH_REBOOT = _FRESH_RESTART_AT + _FRESH_DOWN_SECONDS  # 12.0
_FRESH_CEILING = 90.0

_INFLIGHT_RESTART_AT = 9.5
_INFLIGHT_DOWN_SECONDS = 20.0
_INFLIGHT_CEILING = 90.0
# The job-level timeout (submit ~9.213 + 30s) is declared by the
# unified-timeout loop's 30s-cadence tick: earliest the deadline
# itself, latest one full tick later (measured: 60.03).
_JOB_TIMEOUT_SECONDS = 30.0
_TIMEOUT_TICK_SECONDS = 30.0
_TERMINAL_SLACK_SECONDS = 1.5


def _run_worker_restart(
    restart_at: float,
    down_seconds: float,
    ceiling: float,
    client_submit_at: float = 0.0,
) -> dict:
    coordinator = SimulationCoordinator(
        latency=0.01, max_virtual_time=ceiling, seed=_SEED
    )
    coordinator.add_process(
        "manager",
        recovery_manager_entry,
        "sim-mgr",
        9000,
        9001,
        "sim-dc",
        (),
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
        client_submit_at,
    )
    # Kills drain before restarts at the same instant, so the worker
    # has no live spawned children when the restart snapshot runs.
    for executor_id in _EXECUTOR_IDS:
        coordinator.schedule_kill(executor_id, at_time=restart_at)
    coordinator.schedule_restart(
        "worker", restart_at, down_seconds=down_seconds
    )
    return coordinator.run()


def _assert_no_unswapped_imports(results: dict) -> None:
    for process_id, process_log in results.items():
        audit_entries = [
            entry
            for entry in (process_log or [])
            if isinstance(entry, tuple)
            and entry[:1] == ("determinism-audit-unswapped",)
        ]
        assert not audit_entries, (process_id, audit_entries)


def _assert_oracle_clean(client_log: list) -> None:
    violations = JobStatusOracle().check_client_log(client_log)
    assert not violations, (violations, client_log)


def _finished(client_log: list) -> tuple:
    finished = [entry for entry in client_log if entry[0] == "job-finished"]
    assert len(finished) == 1, (
        "result delivery must be exactly-once per job",
        client_log,
    )
    return finished[0]


def _submitted_at(client_log: list) -> float:
    submitted = [entry for entry in client_log if entry[0] == "job-submitted"]
    assert submitted, client_log
    return submitted[0][1]


def _activation_times(worker_log: list) -> list[float]:
    return [
        entry[2]
        for entry in worker_log
        if entry[0] == "workflows-active" and entry[1] > 0
    ]


# ---------------------------------------------------------------------------
# Scenario 1: restart an idle worker; a job submitted after reboot completes
# ---------------------------------------------------------------------------


def _run_fresh_job_after_worker_restart() -> dict:
    return _run_worker_restart(
        _FRESH_RESTART_AT, _FRESH_DOWN_SECONDS, _FRESH_CEILING
    )


def test_job_submitted_after_worker_reboot_completes_on_new_pool():
    """The rebooted worker generation must re-register and serve: the
    client's retrying submission is only accepted once gen-2's
    capacity is registered (strictly after the reboot instant), and
    the job runs to completion on the RESPAWNED executor pool."""
    results = _run_fresh_job_after_worker_restart()
    client_log = results["client"]

    # Acceptance strictly after the reboot: the manager held the
    # (stale) gen-1 registration through the sub-detection down window,
    # but leader election (~9.2) plus gen-2 re-registration gate the
    # accept — probed 14.193062.
    submitted_time = _submitted_at(client_log)
    assert _FRESH_REBOOT < submitted_time < _FRESH_REBOOT + 5.0, client_log

    (_tag, final_status, finished_time) = _finished(client_log)
    assert final_status == "completed", client_log
    assert submitted_time < finished_time < submitted_time + 5.0, (
        "post-reboot completion must be prompt (measured 14.773062, "
        f"+0.58 after acceptance): {client_log}"
    )

    # The workflow ran on the REBOOTED generation, and only there.
    generation_one_activations = _activation_times(results["worker.gen1"])
    assert not generation_one_activations, results["worker.gen1"]
    generation_two_activations = _activation_times(results["worker"])
    assert generation_two_activations, results["worker"]
    assert generation_two_activations[0] > _FRESH_REBOOT, results["worker"]

    # The respawned pool reported under the SAME executor ids (the
    # killed gen-1 executors freed them; SIGKILLed children never
    # produce result rows, so these rows are gen-2's).
    for executor_id in _EXECUTOR_IDS:
        assert executor_id in results, sorted(results.keys())

    _assert_oracle_clean(client_log)
    _assert_no_unswapped_imports(results)


def test_fresh_job_after_worker_restart_is_replay_deterministic():
    assert (
        _run_fresh_job_after_worker_restart()
        == _run_fresh_job_after_worker_restart()
    )


# ---------------------------------------------------------------------------
# Scenario 2: restart the worker MID-WORKFLOW — loud terminal, no retry
# ---------------------------------------------------------------------------


def _run_worker_restart_mid_workflow() -> dict:
    return _run_worker_restart(
        _INFLIGHT_RESTART_AT, _INFLIGHT_DOWN_SECONDS, _INFLIGHT_CEILING
    )


def test_inflight_job_reaches_loud_timeout_across_worker_restart():
    """TRUE current behavior, pinned: a job in flight when its worker
    host power-cycles ends in a LOUD client-observed ``timeout`` — not
    a retry onto the rebooted worker.

    Mechanism (traced): the sub-detection silence window plus the
    same-NodeId re-registration means SWIM never declares the worker
    dead, so the death-triggered requeue never fires; the 30s-cadence
    orphan scan loses the race to the 30s-cadence unified-timeout tick,
    which declares the job at the first tick past its deadline
    (submit 9.213 + 30s -> declared 60.03). When an in-flight
    reassignment path lands, the no-execution assertion below is the
    one it flips."""
    results = _run_worker_restart_mid_workflow()
    client_log = results["client"]

    submitted_time = _submitted_at(client_log)
    assert submitted_time < _INFLIGHT_RESTART_AT, client_log

    # Structural in-flight proof: the gen-1 worker's preserved log ends
    # with the workflow ACTIVE (dispatch 9.25 < restart 9.5, and no
    # drain entry follows).
    generation_one_log = results["worker.gen1"]
    generation_one_activations = _activation_times(generation_one_log)
    assert generation_one_activations, generation_one_log
    assert generation_one_activations[0] < _INFLIGHT_RESTART_AT, (
        generation_one_log
    )
    assert generation_one_log[-1][:2] == ("workflows-active", 1), (
        "the restart must land INSIDE live execution",
        generation_one_log,
    )

    # Loud terminal inside the unified-timeout design bound.
    (_tag, final_status, finished_time) = _finished(client_log)
    assert final_status == "timeout", client_log
    earliest = submitted_time + _JOB_TIMEOUT_SECONDS
    latest = earliest + _TIMEOUT_TICK_SECONDS + _TERMINAL_SLACK_SECONDS
    assert earliest <= finished_time <= latest, (
        f"in-flight terminal at {finished_time} outside the "
        f"unified-timeout design bound [{earliest}, {latest}] "
        f"(measured 60.03): {client_log}"
    )

    # The rebooted worker re-attached (its registry observed a healthy
    # manager) but never executed — no silent half-retry.
    generation_two_log = results["worker"]
    assert any(
        entry[0] == "manager-healthy" for entry in generation_two_log
    ), generation_two_log
    assert not _activation_times(generation_two_log), generation_two_log

    _assert_oracle_clean(client_log)
    _assert_no_unswapped_imports(results)


def test_inflight_worker_restart_is_replay_deterministic():
    assert (
        _run_worker_restart_mid_workflow()
        == _run_worker_restart_mid_workflow()
    )


# ---------------------------------------------------------------------------
# Scenario 3 (KNOWN BUG, pinned as a reproducer): dispatcher livelock on
# a submission landing well after the rebooted worker re-registered
# ---------------------------------------------------------------------------


@pytest.mark.skip(
    reason=(
        "KNOWN BUG (deterministic reproducer): restart the worker at "
        "2.0 (down 10; executors killed same-instant), then submit the "
        "job at t=20 — ~8s after gen-2 re-registered. The MANAGER's "
        "WorkflowDispatcher enters a zero-delay self-rescheduling loop "
        "(_wait_dispatch_trigger / _consume_ready_signal, "
        "workflow_dispatcher.py:1058/1078) at virtual 22.03; the "
        "harness spin guard aborts the child ('run_window spun 500001 "
        "times without advancing'). In production this is a 100% CPU "
        "livelock. Controls: the SAME late submission with no restart "
        "completes at 20.6, and the immediate submission after the "
        "restart (accepted 14.19, ~2.2s post-re-registration) "
        "dispatches fine — the livelock needs the restart AND the "
        "delayed submission. Reproduce: seed 101, worker restart 2.0 "
        "down 10, client submit_at 20. Unskip when the dispatcher's "
        "ready-signal consume path backs off instead of spinning."
    )
)
def test_late_submission_after_worker_restart_must_not_livelock_dispatch():
    results = _run_worker_restart(
        _FRESH_RESTART_AT,
        _FRESH_DOWN_SECONDS,
        _FRESH_CEILING,
        client_submit_at=20.0,
    )
    client_log = results["client"]
    (_tag, final_status, _finished_time) = _finished(client_log)
    assert final_status == "completed", client_log
    _assert_oracle_clean(client_log)
    _assert_no_unswapped_imports(results)
