"""
Checklist B8 — storage faults DURING recovery, pinned over the gateless
L2 restart-resume scenario (client -> manager -> worker, seed 101).

Both scenarios reuse the house restart schedule
(power-lose the manager at 2.15 mid-workflow, reboot from the surviving
disk at 47.15) and arm a
``SimFilesystem`` fault via the BOOT-AWARE schedule applier
(``recovery_faults_demo.apply_boot_aware_storage_fault_schedule``):
a rebooted generation re-runs the same entry args, and a fault window
already open at the boot instant is armed SYNCHRONOUSLY during entry
setup — strictly before the server task's first step — so gen-2's
recovery (incarnation store, WAL replay, submission resume) runs
against the faulted disk from its very first I/O. The stock
``call_at``-only applier provably misses that window: the boot task's
first step runs the whole synchronous recovery prefix before any
past-due timer callback fires (probe, before the 2026-10-04 re-pin:
gen-2 ``manager-started`` landed at exactly its boot instant under a
20ms slow disk armed the ``call_at`` way, and 0.12 later armed
boot-aware).

Probed timeline (seed 101, re-probed 2026-10-04: the restart moved
from 9.4 to 2.15 once a lone manager led the moment its own vote made
the majority; every later pin is expressed from it or from gen-2's
boot):

* baseline (no storage fault): submit 1.795823, worker active from the
  2.0 sample, restart 2.15 mid-run, gen-2 up 47.15, resumed re-dispatch,
  completion observed 51.914493.
* slow disk 20ms over [2.05, 67.75): completion 51.974493 (+0.060 over
  baseline: the charged boot-recovery and completion-path operations).
  (Before the re-pin: +0.040, the boot recovery charging exactly 6
  storage operations.) The client-visible
  slack stays small because the worker's own re-registration cadence,
  not the manager's charged boot, dominates the resume leg.
* disk_full budget sweep (armed at gen-2 boot): budgets >= 2048 fit
  every gen-2 write (completion at the exact baseline); budgets 96 and
  512 pinch the completion leg. Write-level trace (budget 512):
  recovery itself is nearly write-free — gen-2's boot writes ONE
  76-byte incarnation record; WAL replay reads and the resume path
  (job re-activation, Raft job group, leadership, dispatch queue)
  append NOTHING — so the budget survives to the completion instant,
  where the 86-byte JobCompleted ``append_fsync`` lands DURABLY and
  the 236-byte archive copy is the first ENOSPC. That raise aborts
  ``_handle_job_completion`` after the durable terminal but before
  the tier-1 client push; the client's own 5s-cadence gateless poll
  fallback recovers the terminal from the manager's live state at the
  next tick — completion observed 64.253062 (= the 64.21 poll,
  +1.6345 over baseline). A third restart (gen-2 boot + 15.6) proves the terminal
  record's durability behaviorally (gen-3 recovers the job TERMINAL:
  no resume, no re-execution). ``start()`` never wedges at any swept
  budget; the client is never silent.
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
_CEILING = 120.0
# Mid-run, 0.15 after the worker's activation sample (2.0), as the
# repeated-crash scenario's (seed 101; 9.4 while a lone manager waited out
# a full pre-vote and vote wait for a majority its own vote already made).
_RESTART_AT = 2.15
_DOWN_SECONDS = 45.0
_GENERATION_TWO_BOOT = _RESTART_AT + _DOWN_SECONDS  # 47.15
# Never expires under the ceiling, so ``job-finished`` is always logged.
_WAIT_TIMEOUT_SECONDS = 200.0

_SLOW_DISK_DELAY_SECONDS = 0.02
# Armed after the pre-restart dispatch and before the restart so the
# window genuinely STRADDLES the power loss; open until well past the
# resumed completion (gen-2 boot + ~5).
_SLOW_DISK_SCHEDULE = (
    ("slow_disk", _RESTART_AT - 0.1, _SLOW_DISK_DELAY_SECONDS, _GENERATION_TWO_BOOT + 20.6),
)
# Probe-measured completion of the no-fault restart baseline (see the
# module docstring) — the slow-disk bound is derived from it.
_BASELINE_COMPLETION = 51.914493
# Design bound on charged storage operations along the client-visible
# path (boot recovery measured 6 ops, completion path 2; the ceiling
# leaves headroom for schedule drift without ever hiding a stall).
_CHARGED_OPS_CEILING = 50
_BOOT_CHARGED_OPS_CEILING = 20


def _run_restart_with_storage_schedule(storage_fault_schedule: tuple) -> dict:
    coordinator = SimulationCoordinator(
        latency=0.01, max_virtual_time=_CEILING, seed=_SEED
    )
    coordinator.add_process(
        "manager",
        recovery_manager_entry,
        "sim-mgr",
        9000,
        9001,
        "sim-dc",
        storage_fault_schedule,
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
        "manager", _RESTART_AT, down_seconds=_DOWN_SECONDS
    )
    return coordinator.run()


def _assert_no_unswapped_imports(results: dict) -> None:
    """The determinism audit must be clean in EVERY child (all
    generations): an unswapped deferred import silently forks the
    schedule."""
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


def _submitted_at(client_log: list) -> float:
    submitted = [entry for entry in client_log if entry[0] == "job-submitted"]
    assert submitted, client_log
    return submitted[0][1]


def _finished(client_log: list) -> tuple:
    finished = [entry for entry in client_log if entry[0] == "job-finished"]
    assert len(finished) == 1, (
        "result delivery must be exactly-once per job",
        client_log,
    )
    return finished[0]


def _manager_started_at(manager_log: list) -> float:
    started = [entry for entry in manager_log if entry[0] == "manager-started"]
    assert started, manager_log
    return started[0][1]


# ---------------------------------------------------------------------------
# Scenario 1: slow disk armed ACROSS the restart — recovery under latency
# ---------------------------------------------------------------------------


def _run_slow_disk_through_recovery() -> dict:
    return _run_restart_with_storage_schedule(_SLOW_DISK_SCHEDULE)


def test_recovery_completes_through_slow_disk():
    """Gen-2 recovery (WAL replay + submission resume) must complete
    THROUGH a slow disk and the resumed job must reach the client
    within the charged-operation design bound over the no-fault
    restart baseline — bounded storage latency is never a legitimate
    strand, not even during recovery.

    The straddle is asserted structurally: gen-2's ``manager-started``
    must land STRICTLY after the boot instant (the no-fault boot is
    virtually instantaneous at exactly its boot instant, so any shift is charged
    recovery I/O — measured +0.12 = 6 operations) yet within the
    boot-op ceiling.
    """
    results = _run_slow_disk_through_recovery()
    client_log = results["client"]

    submitted_time = _submitted_at(client_log)
    assert submitted_time < _RESTART_AT, client_log

    # The restart landed INSIDE live execution: the worker (never
    # restarted in this scenario) shows the workflow ACTIVE before the
    # restart instant (dispatch 9.25 < 9.4). The client's own
    # ``running`` observation CANNOT witness this — its 0.5s status
    # cadence first samples ``running`` at 9.713, after the restart.
    worker_activation_times = [
        entry[2]
        for entry in results["worker"]
        if entry[0] == "workflows-active" and entry[1] > 0
    ]
    assert worker_activation_times, results["worker"]
    assert worker_activation_times[0] < _RESTART_AT, (
        "the restart must land INSIDE live execution",
        results["worker"],
    )

    # The slow disk genuinely covered gen-2's recovery: boot I/O was
    # charged (strict shift past the instantaneous-boot instant),
    # inside the charged-op ceiling.
    generation_two_started = _manager_started_at(results["manager"])
    boot_ceiling = _GENERATION_TWO_BOOT + (
        _BOOT_CHARGED_OPS_CEILING * _SLOW_DISK_DELAY_SECONDS
    )
    assert _GENERATION_TWO_BOOT < generation_two_started <= boot_ceiling, (
        f"gen-2 start at {generation_two_started} outside the charged "
        f"recovery bracket ({_GENERATION_TWO_BOOT}, {boot_ceiling}]: "
        f"{results['manager']}"
    )

    # Loud, exactly-once completion from the RESUMED generation, within
    # the charged-delay design bound over the restart baseline.
    (_tag, final_status, finished_time) = _finished(client_log)
    assert final_status == "completed", client_log
    completion_ceiling = _BASELINE_COMPLETION + (
        _CHARGED_OPS_CEILING * _SLOW_DISK_DELAY_SECONDS
    )
    assert _GENERATION_TWO_BOOT < finished_time <= completion_ceiling, (
        f"slow-disk resumed completion at {finished_time} outside the "
        f"design bound ({_GENERATION_TWO_BOOT}, {completion_ceiling}]: "
        f"{client_log}"
    )

    # Both generations admitted the worker.
    assert any(
        entry[0] == "worker-registered" for entry in results["manager.gen1"]
    ), results["manager.gen1"]
    assert any(
        entry[0] == "worker-registered" for entry in results["manager"]
    ), results["manager"]

    _assert_oracle_clean(client_log)
    _assert_no_unswapped_imports(results)


def test_slow_disk_recovery_is_replay_deterministic():
    assert _run_slow_disk_through_recovery() == _run_slow_disk_through_recovery()


# ---------------------------------------------------------------------------
# Scenario 2: disk FULL armed inside gen-2's recovery window
# ---------------------------------------------------------------------------

# Armed at a virtual instant INSIDE the down window: gen-1 dies before
# the timer fires (its budget is never armed), gen-2's boot-aware replay
# arms the FRESH budget synchronously at its boot — before the first
# recovery I/O.
_DISK_FULL_ARM_AT = _RESTART_AT + 20.6
# Probe-swept budgets 96/512/2048/8192: at >= 2048 every gen-2 write
# fits (completion at the exact no-fault baseline); at <= 512 the
# budget pinches gen-2's completion-record leg. 512 keeps the ~110-byte
# incarnation record inside the budget so recovery itself is clean.
_DISK_FULL_BUDGET_BYTES = 512
_DISK_FULL_SCHEDULE = (("disk_full", _DISK_FULL_ARM_AT, _DISK_FULL_BUDGET_BYTES),)
# Probe-measured completion: 51.914493 (2026-10-04) — byte-identical to
# the no-fault restart baseline. Traced: the durable JobCompleted append
# fits the budget; the 236-byte archive copy ENOSPCs and is ISOLATED
# (parked for healing, logged loudly — JobLedger._archive_job_isolated)
# so the tier-1 client push runs and delivers at the baseline instant:
# archive failure is invisible to the client. (Pre-isolation the
# archive exception aborted the handler before the push and the 5s
# gateless poll fallback rescued the terminal at 64.253062 — the fix
# reclaimed that 1.63s and, in gate topologies where no manager poll
# exists, the outcome itself.) Ceiling = baseline + push/apply slack.
_DISK_FULL_COMPLETION = 51.914493
_DISK_FULL_COMPLETION_CEILING = _DISK_FULL_COMPLETION + 1.081448

# Scenario 3: a THIRD restart after the degraded completion — if the
# terminal record had not genuinely landed durably, gen-3 would replay
# the job ACTIVE and re-run it.
_TRUTH_RESTART_AT = _GENERATION_TWO_BOOT + 15.6
_TRUTH_DOWN_SECONDS = 20.0
_TRUTH_CEILING = 130.0


def _assert_no_start_failures(results: dict) -> None:
    """``start()`` must never raise (a wedged recovery is the exact
    failure mode this scenario exists to catch)."""
    for process_id, process_log in results.items():
        start_failures = [
            entry
            for entry in (process_log or [])
            if isinstance(entry, tuple) and entry[0] == "manager-start-failed"
        ]
        assert not start_failures, (process_id, start_failures)


def _run_disk_full_during_recovery() -> dict:
    return _run_restart_with_storage_schedule(_DISK_FULL_SCHEDULE)


def test_disk_full_during_recovery_never_wedges_and_stays_truthful():
    """A byte budget exhausted during gen-2's lifetime must never wedge
    ``start()`` and never strand the client in silence.

    Probed truth (budget 512, armed at boot before the first recovery
    I/O): recovery, resume, and re-dispatch all complete THROUGH the
    budget — gen-2's recovery writes exactly one 76-byte incarnation
    record — and the ENOSPC lands on the completion leg instead: the
    86-byte durable JobCompleted append fits, the 236-byte archive
    copy raises and is ISOLATED (parked + logged, never aborting the
    handler), so the tier-1 push delivers the exactly-once
    ``completed`` at 51.914493 — the no-fault baseline instant; the
    archive failure is invisible to the client. Gen-2 boots at exactly
    its boot instant, 47.15 (disk_full charges bytes, not time), with no
    ``manager-start-failed``.
    """
    results = _run_disk_full_during_recovery()
    client_log = results["client"]

    assert _submitted_at(client_log) < _RESTART_AT, client_log

    # Boot neither wedged nor stalled: started at the boot instant.
    generation_two_started = _manager_started_at(results["manager"])
    assert _GENERATION_TWO_BOOT <= generation_two_started < (
        _GENERATION_TWO_BOOT + 0.5
    ), results["manager"]
    _assert_no_start_failures(results)

    (_tag, final_status, finished_time) = _finished(client_log)
    assert final_status == "completed", client_log
    assert _GENERATION_TWO_BOOT < finished_time <= (
        _DISK_FULL_COMPLETION_CEILING
    ), (
        f"disk-full degraded completion at {finished_time} outside the "
        f"design bound ({_GENERATION_TWO_BOOT}, "
        f"{_DISK_FULL_COMPLETION_CEILING}] (measured "
        f"{_DISK_FULL_COMPLETION}): {client_log}"
    )

    assert any(
        entry[0] == "worker-registered" for entry in results["manager"]
    ), results["manager"]

    _assert_oracle_clean(client_log)
    _assert_no_unswapped_imports(results)


def test_disk_full_recovery_is_replay_deterministic():
    assert _run_disk_full_during_recovery() == _run_disk_full_during_recovery()


# ---------------------------------------------------------------------------
# Scenario 3: durable truth — a third restart after the degraded completion
# ---------------------------------------------------------------------------


def _run_disk_full_completion_truth() -> dict:
    coordinator = SimulationCoordinator(
        latency=0.01, max_virtual_time=_TRUTH_CEILING, seed=_SEED
    )
    coordinator.add_process(
        "manager",
        recovery_manager_entry,
        "sim-mgr",
        9000,
        9001,
        "sim-dc",
        _DISK_FULL_SCHEDULE,
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
        "manager", _RESTART_AT, down_seconds=_DOWN_SECONDS
    )
    coordinator.schedule_restart(
        "manager", _TRUTH_RESTART_AT, down_seconds=_TRUTH_DOWN_SECONDS
    )
    return coordinator.run()


def test_degraded_completion_record_survives_a_further_restart():
    """The ledger truth behind the disk-full degraded completion: the
    terminal record genuinely reached the durable disk. A THIRD
    generation rebooting from that disk at 90.0 must recover the job
    TERMINAL — no resume, no re-execution — and the client's delivered
    result stands.

    Probed (before the 2026-10-04 re-pin; every instant has since moved
    with the first restart, 9.4 -> 2.15): gen-3 booted 90.0, re-admitted
    the worker, and the worker never ran the workflow again (its last
    activation was gen-2's resumed run). If the JobCompleted record had
    been lost to the exhausted budget, gen-3 would resume the job and
    the worker would show an activation after gen-3's boot — the
    assertion that
    catches any silent ledger/client divergence here.
    """
    results = _run_disk_full_completion_truth()
    client_log = results["client"]

    (_tag, final_status, finished_time) = _finished(client_log)
    assert final_status == "completed", client_log
    assert finished_time < _TRUTH_RESTART_AT, (
        "the degraded completion must be delivered BEFORE the third "
        f"restart for this scenario to prove anything: {client_log}"
    )

    # All three generations reported; the final one re-admitted the
    # worker.
    assert "manager.gen1" in results and "manager.gen2" in results, (
        sorted(results.keys())
    )
    generation_three_started = _manager_started_at(results["manager"])
    truth_boot = _TRUTH_RESTART_AT + _TRUTH_DOWN_SECONDS
    assert truth_boot <= generation_three_started < truth_boot + 0.5, (
        results["manager"]
    )
    assert any(
        entry[0] == "worker-registered" for entry in results["manager"]
    ), results["manager"]
    _assert_no_start_failures(results)

    # The durable terminal survived: gen-3 recovered the job TERMINAL,
    # so nothing re-executes after the third restart.
    late_activations = [
        entry
        for entry in results["worker"]
        if entry[0] == "workflows-active"
        and entry[1] > 0
        and entry[2] > _TRUTH_RESTART_AT
    ]
    assert not late_activations, (
        "gen-3 re-ran a job the client already observed COMPLETED — "
        f"the durable terminal record was lost: {results['worker']}"
    )

    _assert_oracle_clean(client_log)
    _assert_no_unswapped_imports(results)


def test_completion_truth_restart_is_replay_deterministic():
    assert (
        _run_disk_full_completion_truth() == _run_disk_full_completion_truth()
    )
