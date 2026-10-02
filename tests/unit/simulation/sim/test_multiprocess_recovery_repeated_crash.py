"""
Checklist C6 — a SECOND power loss DURING recovery: repeated manager
crash/restart cycles over the gateless L2 restart-resume scenario
(client -> manager -> worker, seed 101).

Recovery must be idempotent: however many generations replay the WAL
and resume the persisted submission, the client observes exactly one
terminal result and the durable truth never regresses. Two probed
placements of the second crash:

* R2 = 57.0 — INSIDE gen-2's recovery span. Gen-2 boots at 54.4,
  replays the WAL, re-activates the job and queues its dispatch, and
  dies at 57.0 BEFORE the worker's re-registration (~62.4) — its
  generation log carries ``manager-started`` and nothing else, the
  structural proof the crash landed mid-recovery. Gen-3 boots at 82.0,
  replays the SAME still-ACTIVE ledger + submission payload, resumes
  again, re-admits the worker (85.5 sample), re-dispatches
  (85.5-86.0) and the client observes exactly one ``completed`` at
  85.974191 — prompt: boot + 3.97s.
* R2 = 59.5 — AFTER gen-2's re-dispatch, BEFORE the completion record.
  Gen-2 re-admitted the worker (59.4) and re-dispatched (59.25); the
  crash lands mid-execution, so the worker's second run drains into a
  dead manager (its active entry lingers to 83.25) and gen-3 resumes a
  THIRD dispatch (86.25-86.75). Execution is at-least-once times
  three; the client outcome is exactly-once: one ``completed``,
  delivered at 86.620758 -- the third run's result reaches gen-3 0.08s
  after the run ends. Re-probed 2026-10-02: gen-2's recovery got
  3.0s faster (re-dispatch 62.25 -> 59.25, completion record ~62.61 ->
  ~59.7), so the old 62.5 landed AFTER the completion and the job
  finished under gen-2; 59.5 keeps the crash inside the same window.

FIXED GAP (second placement): gen-3's completion used to arrive ~44s
late (130.9). Every manager generation re-registers under a new node id
at the same address, and the worker kept them all: its final-result
send resolved the address to gen-1's dead id, whose circuit breaker the
failed sends to the downed manager had opened, so the fresh result was
skipped and queued until that breaker half-opened on its own. The
worker now treats direct evidence (the registration exchange, the
manager's own heartbeat) as superseding a previous incarnation at the
same address, so the result goes to gen-3 at once.
"""

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

_RESTART_ONE_AT = 9.4
_DOWN_ONE_SECONDS = 45.0
_GENERATION_TWO_BOOT = _RESTART_ONE_AT + _DOWN_ONE_SECONDS  # 54.4

_MID_RECOVERY_RESTART_AT = 57.0
_MID_RECOVERY_DOWN_SECONDS = 25.0
_GENERATION_THREE_BOOT = (
    _MID_RECOVERY_RESTART_AT + _MID_RECOVERY_DOWN_SECONDS
)  # 82.0
_MID_RECOVERY_CEILING = 140.0
# Gen-3's resume leg (boot -> worker re-registration -> re-dispatch ->
# completion push) measured 3.97s; gen-2's equivalent leg in the house
# baseline measured 8.26s — bound the completion by the slower leg
# plus slack.
_RESUME_LEG_CEILING_SECONDS = 12.0

_MID_REDISPATCH_RESTART_AT = 59.5
_MID_REDISPATCH_DOWN_SECONDS = 25.0
_GENERATION_THREE_LATE_BOOT = (
    _MID_REDISPATCH_RESTART_AT + _MID_REDISPATCH_DOWN_SECONDS
)  # 84.5
_MID_REDISPATCH_CEILING = 160.0


def _run_double_restart(
    second_restart_at: float,
    second_down_seconds: float,
    ceiling: float,
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
    )
    coordinator.schedule_restart(
        "manager", _RESTART_ONE_AT, down_seconds=_DOWN_ONE_SECONDS
    )
    coordinator.schedule_restart(
        "manager", second_restart_at, down_seconds=second_down_seconds
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
        "result delivery must be exactly-once per job — a second crash "
        "mid-recovery must never double-deliver",
        client_log,
    )
    return finished[0]


def _submitted_at(client_log: list) -> float:
    submitted = [entry for entry in client_log if entry[0] == "job-submitted"]
    assert submitted, client_log
    return submitted[0][1]


def _manager_started_at(manager_log: list) -> float:
    started = [entry for entry in manager_log if entry[0] == "manager-started"]
    assert started, manager_log
    return started[0][1]


def _activation_times(worker_log: list) -> list[float]:
    return [
        entry[2]
        for entry in worker_log
        if entry[0] == "workflows-active" and entry[1] > 0
    ]


# ---------------------------------------------------------------------------
# Scenario 1: the second crash lands INSIDE gen-2's recovery span
# ---------------------------------------------------------------------------


def _run_crash_during_recovery() -> dict:
    return _run_double_restart(
        _MID_RECOVERY_RESTART_AT,
        _MID_RECOVERY_DOWN_SECONDS,
        _MID_RECOVERY_CEILING,
    )


def test_second_crash_inside_recovery_is_idempotent_and_completes():
    """Gen-2 crashes after replaying the WAL and re-activating the job
    but before its dispatch could reach the (not yet re-registered)
    worker. Gen-3 must replay the SAME durable state and finish the
    job — exactly-once at the client, prompt on the resume leg."""
    results = _run_crash_during_recovery()
    client_log = results["client"]

    assert _submitted_at(client_log) < _RESTART_ONE_AT, client_log

    # Generation bookkeeping: both dead generations kept their logs.
    assert "manager.gen1" in results and "manager.gen2" in results, (
        sorted(results.keys())
    )
    assert any(
        entry[0] == "worker-registered" for entry in results["manager.gen1"]
    ), results["manager.gen1"]

    # Structural proof R2 landed inside the recovery span: gen-2 booted
    # at the window edge and never reached worker re-registration.
    generation_two_log = results["manager.gen2"]
    assert _manager_started_at(generation_two_log) == _GENERATION_TWO_BOOT, (
        generation_two_log
    )
    assert not any(
        entry[0] == "worker-registered" for entry in generation_two_log
    ), (
        "gen-2 re-registered the worker — the second restart no longer "
        f"lands inside the recovery span: {generation_two_log}"
    )

    # Gen-3 recovered and finished the job.
    generation_three_log = results["manager"]
    assert (
        _manager_started_at(generation_three_log) == _GENERATION_THREE_BOOT
    ), generation_three_log
    assert any(
        entry[0] == "worker-registered" for entry in generation_three_log
    ), generation_three_log

    (_tag, final_status, finished_time) = _finished(client_log)
    assert final_status == "completed", client_log
    resume_ceiling = _GENERATION_THREE_BOOT + _RESUME_LEG_CEILING_SECONDS
    assert _GENERATION_THREE_BOOT < finished_time <= resume_ceiling, (
        f"triple-generation completion at {finished_time} outside the "
        f"resume-leg design bound ({_GENERATION_THREE_BOOT}, "
        f"{resume_ceiling}] (measured 85.974191): {client_log}"
    )

    # Execution truth: the doomed gen-1 dispatch plus gen-3's resumed
    # dispatch — gen-2 died before it could dispatch.
    activation_times = _activation_times(results["worker"])
    assert len(activation_times) == 2, results["worker"]
    assert activation_times[0] < _RESTART_ONE_AT, results["worker"]
    assert activation_times[1] > _GENERATION_THREE_BOOT, results["worker"]

    _assert_oracle_clean(client_log)
    _assert_no_unswapped_imports(results)


def test_crash_during_recovery_is_replay_deterministic():
    assert _run_crash_during_recovery() == _run_crash_during_recovery()


# ---------------------------------------------------------------------------
# Scenario 2: the second crash lands AFTER gen-2's re-dispatch, BEFORE
# the completion record
# ---------------------------------------------------------------------------


def _run_crash_during_redispatch() -> dict:
    return _run_double_restart(
        _MID_REDISPATCH_RESTART_AT,
        _MID_REDISPATCH_DOWN_SECONDS,
        _MID_REDISPATCH_CEILING,
    )


def test_second_crash_after_redispatch_never_double_delivers():
    """The sharpest exactly-once placement: gen-2 already re-dispatched
    (the worker is EXECUTING) when the crash lands, one window before
    the completion record. The workflow ultimately runs THREE times —
    gen-1's doomed dispatch, gen-2's interrupted re-dispatch, gen-3's
    resumed dispatch — and the client must still observe exactly one
    terminal ``completed``, with both dead generations' logs preserved
    under their ``.genN`` keys."""
    results = _run_crash_during_redispatch()
    client_log = results["client"]

    # Gen-2 DID reach re-registration and re-dispatch before dying.
    generation_two_log = results["manager.gen2"]
    assert any(
        entry[0] == "worker-registered" for entry in generation_two_log
    ), (
        "gen-2 never re-registered the worker — the second restart no "
        f"longer lands after the re-dispatch: {generation_two_log}"
    )

    # Execution at-least-once, three times, at the probed placements.
    activation_times = _activation_times(results["worker"])
    assert len(activation_times) == 3, results["worker"]
    assert activation_times[0] < _RESTART_ONE_AT, results["worker"]
    assert (
        _GENERATION_TWO_BOOT
        < activation_times[1]
        < _MID_REDISPATCH_RESTART_AT
    ), results["worker"]
    assert activation_times[2] > _GENERATION_THREE_LATE_BOOT, (
        results["worker"]
    )

    # Exactly-once, loud, correct terminal -- delivered within the
    # resume leg of gen-3's boot (see the module docstring's fixed-gap
    # note).
    (_tag, final_status, finished_time) = _finished(client_log)
    assert final_status == "completed", client_log
    resume_deadline = _GENERATION_THREE_LATE_BOOT + _RESUME_LEG_CEILING_SECONDS
    assert _GENERATION_THREE_LATE_BOOT < finished_time < resume_deadline, (
        f"exactly-once completion at {finished_time} outside "
        f"({_GENERATION_THREE_LATE_BOOT}, {resume_deadline}) -- the resumed "
        f"job's result must reach the live generation promptly "
        f"(measured 86.620758): {client_log}"
    )

    _assert_oracle_clean(client_log)
    _assert_no_unswapped_imports(results)


def test_crash_during_redispatch_is_replay_deterministic():
    assert _run_crash_during_redispatch() == _run_crash_during_redispatch()
