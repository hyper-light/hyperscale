"""
Checklist C6 — a SECOND power loss DURING recovery: repeated manager
crash/restart cycles over the gateless L2 restart-resume scenario
(client -> manager -> worker, seed 101).

Recovery must be idempotent: however many generations replay the WAL
and resume the persisted submission, the client observes exactly one
terminal result and the durable truth never regresses. Two probed
placements of the second crash:

* R2 = gen-2 boot + 2.6 (49.75) — INSIDE gen-2's recovery span. Gen-2
  boots at 47.15, replays the WAL, re-activates the job and queues its
  dispatch, and dies BEFORE the worker's re-registration (boot + 4.5) —
  its generation log carries ``manager-started`` and nothing else, the
  structural proof the crash landed mid-recovery. Gen-3 boots at 74.75,
  replays the SAME still-ACTIVE ledger + submission payload, resumes
  again, re-admits the worker (80.25), re-dispatches (80.0-80.5) and the
  client observes exactly one ``completed`` at 80.428157 — prompt: boot
  + 5.68s.
* R2 = one coordinator latency after the worker's first activation under
  gen-2 — AFTER gen-2's re-dispatch, BEFORE the completion record,
  derived from the run (an event trigger) rather than a fixed offset
  from gen-2's boot. The crash lands mid-execution, so the worker's
  second run drains into a dead manager and gen-3 resumes a THIRD
  dispatch. Execution is at-least-once times three; the client outcome
  is exactly-once: one ``completed``, delivered on gen-3's resume leg.

Re-probed 2026-10-04: the first restart moved from 9.4 to 2.15 (0.15
after the activation sample, as before) once a lone manager led the
moment its own vote made the majority; the second crashes are expressed
from gen-2's boot so they keep their places in its recovery.

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

from collections.abc import Callable

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

# Mid-run (seed 101: the worker's activation is sampled at 2.0, the
# completion record lands ~2.37), 0.15 after the activation sample as
# before (9.25 -> 9.4 while a lone manager waited out a full pre-vote and
# vote wait for a majority its own vote already made).
_RESTART_ONE_AT = 2.15
_DOWN_ONE_SECONDS = 45.0
_GENERATION_TWO_BOOT = _RESTART_ONE_AT + _DOWN_ONE_SECONDS  # 47.15

# Inside gen-2's recovery span: before it re-admits the worker (probed
# boot + 4.5).
_MID_RECOVERY_RESTART_AT = _GENERATION_TWO_BOOT + 2.6
_MID_RECOVERY_DOWN_SECONDS = 25.0
_GENERATION_THREE_BOOT = (
    _MID_RECOVERY_RESTART_AT + _MID_RECOVERY_DOWN_SECONDS
)  # 74.75
_MID_RECOVERY_CEILING = 140.0
# Gen-3's resume leg (boot -> worker re-registration -> re-dispatch ->
# completion push) measured 3.97s; gen-2's equivalent leg in the house
# baseline measured 8.26s — bound the completion by the slower leg
# plus slack.
_RESUME_LEG_CEILING_SECONDS = 12.0

# After gen-2's re-dispatch, before its completion record: derived from
# the run, one coordinator latency after the worker's first activation
# under gen-2 -- the earliest instant a crash can follow the re-dispatch.
# A fixed offset from gen-2's boot went stale when recovery got faster
# (probed 2026-10-06: re-dispatch at 51.0, completion record at 51.42,
# so boot + 4.6 = 51.75 landed after the job had already finished).
_LATENCY = 0.01
_MID_REDISPATCH_DOWN_SECONDS = 25.0
_MID_REDISPATCH_CEILING = 160.0


def _run_double_restart(
    arm_second_restart: Callable[[SimulationCoordinator], None],
    ceiling: float,
) -> dict:
    coordinator = SimulationCoordinator(
        latency=_LATENCY, max_virtual_time=ceiling, seed=_SEED
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
    arm_second_restart(coordinator)
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


def _schedule_mid_recovery_restart(coordinator: SimulationCoordinator) -> None:
    coordinator.schedule_restart(
        "manager",
        _MID_RECOVERY_RESTART_AT,
        down_seconds=_MID_RECOVERY_DOWN_SECONDS,
    )


def _run_crash_during_recovery() -> dict:
    return _run_double_restart(
        _schedule_mid_recovery_restart, _MID_RECOVERY_CEILING
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
        f"{resume_ceiling}] (measured 80.428157): {client_log}"
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


def _is_generation_two_activation(row: tuple) -> bool:
    """The worker running a workflow gen-2 re-dispatched."""
    return (
        row[0] == "workflows-active"
        and row[1] > 0
        and row[2] > _GENERATION_TWO_BOOT
    )


def _is_generation_two_registration(row: tuple) -> bool:
    """Gen-2's registry holding the re-registered worker."""
    return row[0] == "worker-registered" and row[1] > _GENERATION_TWO_BOOT


def _run_crash_during_redispatch() -> tuple[dict, dict[str, float]]:
    """Run the scenario; returns its results and the instant the
    re-dispatch triggers derived (``restart_at``).

    The second crash waits for BOTH gen-2 milestones -- the worker running
    the re-dispatched workflow and gen-2's registry holding the worker --
    and lands one coordinator latency after the later one, whichever
    order their samplers publish them in."""
    fault_instants: dict[str, float] = {}
    milestone_instants: list[float] = []

    def arm_mid_redispatch_restart(coordinator: SimulationCoordinator) -> None:
        def restart_after_both_milestones(milestone_row: tuple) -> None:
            milestone_instants.append(milestone_row[-1])
            if len(milestone_instants) < 2:
                return
            restart_at = max(milestone_instants) + _LATENCY
            fault_instants.update(restart_at=restart_at)
            coordinator.schedule_restart(
                "manager", restart_at, down_seconds=_MID_REDISPATCH_DOWN_SECONDS
            )

        coordinator.schedule_on_event(
            "worker", _is_generation_two_activation, restart_after_both_milestones
        )
        coordinator.schedule_on_event(
            "manager", _is_generation_two_registration, restart_after_both_milestones
        )

    results = _run_double_restart(
        arm_mid_redispatch_restart, _MID_REDISPATCH_CEILING
    )
    return results, fault_instants


def test_second_crash_after_redispatch_never_double_delivers():
    """The sharpest exactly-once placement: gen-2 already re-dispatched
    (the worker is EXECUTING) when the crash lands, one window before
    the completion record. The workflow ultimately runs THREE times —
    gen-1's doomed dispatch, gen-2's interrupted re-dispatch, gen-3's
    resumed dispatch — and the client must still observe exactly one
    terminal ``completed``, with both dead generations' logs preserved
    under their ``.genN`` keys."""
    results, fault_instants = _run_crash_during_redispatch()
    client_log = results["client"]
    restart_at = fault_instants["restart_at"]
    generation_three_boot = _manager_started_at(results["manager"])

    # Gen-3 booted after the derived restart's downtime, and gen-2's
    # completion record never landed before the crash.
    assert generation_three_boot > restart_at, results["manager"]
    assert _finished(client_log)[2] > generation_three_boot, client_log

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
        _GENERATION_TWO_BOOT < activation_times[1] < restart_at
    ), results["worker"]
    assert activation_times[2] > generation_three_boot, results["worker"]

    # Exactly-once, loud, correct terminal -- delivered within the
    # resume leg of gen-3's boot (see the module docstring's fixed-gap
    # note).
    (_tag, final_status, finished_time) = _finished(client_log)
    assert final_status == "completed", client_log
    resume_deadline = generation_three_boot + _RESUME_LEG_CEILING_SECONDS
    assert generation_three_boot < finished_time < resume_deadline, (
        f"exactly-once completion at {finished_time} outside "
        f"({generation_three_boot}, {resume_deadline}) -- the resumed "
        f"job's result must reach the live generation promptly "
        f"(measured 77.337345): {client_log}"
    )

    _assert_oracle_clean(client_log)
    _assert_no_unswapped_imports(results)


def test_crash_during_redispatch_is_replay_deterministic():
    assert _run_crash_during_redispatch() == _run_crash_during_redispatch()
