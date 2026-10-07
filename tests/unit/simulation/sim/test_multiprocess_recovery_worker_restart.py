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

Probed timelines (seed 101). Every fault and late admission is derived
from the run by event triggers (``SimulationCoordinator.schedule_on_event``)
rather than placed at fixed instants. Re-probed 2026-10-07, once a
manager refusing a submission for want of a known leader told the client
when its election next decides (the client is accepted at 1.729, one
retry after the boot election, instead of 3.45 on an un-hinted ladder):

* FRESH-JOB scenario (restart 1.0 after the manager's registry first
  holds gen-1: 2.0, down 10): gen-1 worker registers at the 1.0 sample
  and dies idle; the client is admitted 7.2s into the down window
  (accepted 9.24); gen-2 boots at 12.0, respawns the pool,
  re-registers; the dispatch retry ladder carries the job across the
  reboot onto gen-2 (active 15.75-16.5), completion 16.263419. (Gen-2's
  ``worker-started`` milestone lands at 15.663419: ``start()`` returns
  only once the pool is fully settled, while registration + dispatch
  service begin mid-``start()``.)
* IN-FLIGHT scenario (restart 0.1 after the worker's first activation
  sample: 1.85, down 20): accepted 1.729, activation sampled at 1.75, the
  gen-1 worker dies with the workflow ACTIVE (its generation log ends
  ``workflows-active 1`` — restarts, unlike kills, preserve the
  victim's milestones). Gen-2 re-registers under a NEW node id (a node
  id embeds its process start time) at the SAME address, inside the
  ~38s SWIM detection bound -- SWIM never declares gen-1 dead, because
  gen-2 answers its probes. The manager recognizes the different id at
  the address as gen-1's successor: it recovers gen-1 as a dead worker
  (pool entry dropped, unfinished workflow reassigned) and drops the
  cached transport to the address, so the reassigned workflow
  dispatches to gen-2 at once (active 25.6) and the client observes
  ``completed`` at 26.113419 -- reboot + 4.26, inside the job's
  budget. (Before the in-flight reassignment landed, the stale id kept
  the workflow forever and the job timed out; with the reassignment
  but without the transport drop, the first re-dispatch rode gen-1's
  dead socket for a full send timeout.)

FIXED BUG (scenario 3 below pins the fix): submitting a job ~8s AFTER
the rebooted worker re-registered used to drive the MANAGER's
``WorkflowDispatcher`` into a zero-delay loop frozen at virtual
22.029999999983676 (the harness spin guard aborted the child; on a
real host, a 100%-CPU micro-spin). Root cause: retry-backoff and
routing-cooldown REMAINDERS can be positive sub-quantum float
artifacts (~1.6e-11s) of deadline arithmetic on a quantized clock —
waiting on them re-arms a timer at the SAME virtual instant, so time
never advances and the eligibility comparison never flips. Fixed with
1ms progress floors at both wait chokepoints
(``workflow_dispatcher._job_dispatch_loop`` wait timeout and
``worker_pool.allocate_cores`` condition wait). Post-fix truth,
probed: submit 20.04, dispatch retries pace through their backoff and
land at 34.25, completion 34.603 — while a no-restart control
completes at 20.6 and the immediate-submission variant at 14.77 (the
schedules of every no-wait path are byte-identical to pre-fix).
Re-probed 2026-10-07 (client admitted 8.0 after gen-2's reboot, 20.0):
accepted 20.08, gen-2 had already replaced gen-1's pool entry, so the
first dispatch lands and the job completes at 20.66 -- as fast as the
no-restart control.
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
_EXECUTOR_IDS = ("executor-sim-wkr-9009", "executor-sim-wkr-9011")
_CLIENT_ARGS = ("sim-cli", 9500, ("sim-mgr", 9000), _WAIT_TIMEOUT_SECONDS)

# Gen-1 dies idle, derived from the run: 1.0 after the manager's registry
# first holds it (the registration sample at 1.0, restart at 2.0, as
# probed) -- the manager keeps the dead generation's entry and accepts.
_FRESH_RESTART_AFTER_REGISTRATION_SECONDS = 1.0
_FRESH_DOWN_SECONDS = 10.0
_FRESH_CEILING = 90.0
# The client is admitted 7.2s into the worker's down window: the instant
# this scenario is about (the manager now leads at once and would take a
# job submitted at boot while gen-1 is still up).
_FRESH_SUBMIT_INTO_DOWN_SECONDS = 7.2

# Inside live execution, derived from the run: 0.1 after the worker's
# first activation sample (the sampler publishes it at the window edge,
# and the completion record follows by ~0.37s).
_INFLIGHT_RESTART_AFTER_ACTIVATION_SECONDS = 0.1
_INFLIGHT_DOWN_SECONDS = 20.0
_INFLIGHT_CEILING = 90.0
# The job's budget: the in-flight recovery must complete the job inside
# it rather than surface as a timeout.
_JOB_TIMEOUT_SECONDS = 30.0


def _is_first_registration(row: tuple) -> bool:
    """The manager's registry holding gen-1."""
    return row[0] == "worker-registered"


def _is_first_activation(row: tuple) -> bool:
    """The worker running its first workflow."""
    return row[0] == "workflows-active" and row[1] > 0


def _run_worker_restart(
    arm_restart: Callable[[SimulationCoordinator, dict[str, float]], None],
    ceiling: float,
) -> tuple[dict, dict[str, float]]:
    """Run the scenario; returns its results and the fault instants
    ``arm_restart``'s triggers derived: ``restart_at`` and ``reboot_at``
    (that plus the downtime)."""
    fault_instants: dict[str, float] = {}
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
    arm_restart(coordinator, fault_instants)
    return coordinator.run(), fault_instants


def _schedule_power_cycle(
    coordinator: SimulationCoordinator,
    fault_instants: dict[str, float],
    restart_at: float,
    down_seconds: float,
) -> None:
    """Kill the executor children and restart the worker at ``restart_at``.

    Kills drain before restarts at the same instant, so the worker has no
    live spawned children when the restart snapshot runs."""
    fault_instants.update(restart_at=restart_at, reboot_at=restart_at + down_seconds)
    for executor_id in _EXECUTOR_IDS:
        coordinator.schedule_kill(executor_id, at_time=restart_at)
    coordinator.schedule_restart("worker", restart_at, down_seconds=down_seconds)


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


def _worker_started_at(worker_log: list) -> float:
    started = [entry for entry in worker_log if entry[0] == "worker-started"]
    assert started, worker_log
    return started[0][1]


def _activation_times(worker_log: list) -> list[float]:
    return [
        entry[2]
        for entry in worker_log
        if entry[0] == "workflows-active" and entry[1] > 0
    ]


# ---------------------------------------------------------------------------
# Scenario 1: restart an idle worker; a job submitted after reboot completes
# ---------------------------------------------------------------------------


def _arm_idle_restart_then_submit(submit_after_restart_seconds: float):
    """Restart gen-1 once the manager holds it, and admit the client
    ``submit_after_restart_seconds`` after the restart."""

    def arm(coordinator: SimulationCoordinator, fault_instants: dict[str, float]) -> None:
        def restart_idle_worker(registration_row: tuple) -> None:
            restart_at = registration_row[1] + _FRESH_RESTART_AFTER_REGISTRATION_SECONDS
            _schedule_power_cycle(coordinator, fault_instants, restart_at, _FRESH_DOWN_SECONDS)
            submit_at = restart_at + submit_after_restart_seconds
            fault_instants.update(submit_at=submit_at)
            coordinator.schedule_admission(
                "client", submit_at, recovery_dispatch_client_entry, *_CLIENT_ARGS
            )

        coordinator.schedule_on_event("manager", _is_first_registration, restart_idle_worker)

    return arm


def _run_fresh_job_after_worker_restart() -> tuple[dict, dict[str, float]]:
    return _run_worker_restart(
        _arm_idle_restart_then_submit(_FRESH_SUBMIT_INTO_DOWN_SECONDS),
        _FRESH_CEILING,
    )


def test_job_submitted_after_worker_reboot_completes_on_new_pool():
    """The rebooted worker generation must re-register and serve: a job
    accepted AROUND the power cycle completes on the RESPAWNED pool.

    Probed mechanism (post stale-pool-entry eviction + registry-miss
    purge): the manager may legitimately ACCEPT while the worker is
    still down — the sub-detection gen-1 registration satisfies the
    capacity fence — because the dispatch retry ladder carries the job
    across the reboot: attempt 1 (down worker) burns its send timeout,
    gen-2 re-registers EVICTING gen-1's same-addr pool entry, and attempt
    2 lands on gen-2. Acceptance is therefore bounded by the submission,
    not the reboot; the load-bearing invariants are prompt post-reboot
    completion and gen-2-only execution.
    """
    results, fault_instants = _run_fresh_job_after_worker_restart()
    client_log = results["client"]
    reboot_at = fault_instants["reboot_at"]

    # Accepted at the submission inside the down window -- the manager
    # accepts for a worker it still holds -- and never past the reboot +
    # re-registration + one retry rung.
    submitted_time = _submitted_at(client_log)
    assert fault_instants["submit_at"] <= submitted_time < reboot_at + 5.0, client_log

    (_tag, final_status, finished_time) = _finished(client_log)
    assert final_status == "completed", client_log
    # Completion promptly after gen-2 re-registration: the retry
    # ladder's next attempt + dispatch + workflow + push (probed
    # 16.263419).
    assert reboot_at < finished_time < reboot_at + 6.0, (
        "post-reboot completion must be prompt (probed 16.263419): "
        f"{client_log}"
    )

    # The workflow ran on the REBOOTED generation, and only there.
    generation_one_activations = _activation_times(results["worker.gen1"])
    assert not generation_one_activations, results["worker.gen1"]
    generation_two_activations = _activation_times(results["worker"])
    assert generation_two_activations, results["worker"]
    assert reboot_at <= _worker_started_at(results["worker"]), results["worker"]
    assert generation_two_activations[0] > reboot_at, results["worker"]

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


def _arm_mid_workflow_restart(
    coordinator: SimulationCoordinator, fault_instants: dict[str, float]
) -> None:
    """Admit the client at boot; restart the worker just after its first
    activation sample."""
    coordinator.add_process("client", recovery_dispatch_client_entry, *_CLIENT_ARGS)

    def restart_mid_workflow(activation_row: tuple) -> None:
        restart_at = activation_row[2] + _INFLIGHT_RESTART_AFTER_ACTIVATION_SECONDS
        _schedule_power_cycle(coordinator, fault_instants, restart_at, _INFLIGHT_DOWN_SECONDS)

    coordinator.schedule_on_event("worker", _is_first_activation, restart_mid_workflow)


def _run_worker_restart_mid_workflow() -> tuple[dict, dict[str, float]]:
    return _run_worker_restart(_arm_mid_workflow_restart, _INFLIGHT_CEILING)


def test_inflight_job_completes_on_the_rebooted_worker():
    """A job in flight when its worker host power-cycles completes on the
    rebooted worker, inside the job's own budget: the manager recovers
    the dead incarnation's in-flight workflow when its successor
    registers at the same address, and re-dispatches it there."""
    results, fault_instants = _run_worker_restart_mid_workflow()
    client_log = results["client"]
    restart_at = fault_instants["restart_at"]
    reboot_at = fault_instants["reboot_at"]

    submitted_time = _submitted_at(client_log)
    assert submitted_time < restart_at, client_log

    # Structural in-flight proof: the gen-1 worker's preserved log ends
    # with the workflow ACTIVE (and no drain entry follows).
    generation_one_log = results["worker.gen1"]
    generation_one_activations = _activation_times(generation_one_log)
    assert generation_one_activations, generation_one_log
    assert generation_one_activations[0] < restart_at, generation_one_log
    assert generation_one_log[-1][:2] == ("workflows-active", 1), (
        "the restart must land INSIDE live execution",
        generation_one_log,
    )

    # The rebooted worker re-attached and ran the reassigned workflow
    # exactly once.
    generation_two_log = results["worker"]
    assert any(
        entry[0] == "manager-healthy" for entry in generation_two_log
    ), generation_two_log
    assert reboot_at <= _worker_started_at(generation_two_log), generation_two_log
    generation_two_activations = _activation_times(generation_two_log)
    assert len(generation_two_activations) == 1, generation_two_log
    assert generation_two_activations[0] > reboot_at, generation_two_log

    # Exactly one loud ``completed``, after the reboot and before the
    # job's deadline -- the recovery fits the job's budget instead of
    # surfacing as a timeout.
    (_tag, final_status, finished_time) = _finished(client_log)
    assert final_status == "completed", client_log
    deadline = submitted_time + _JOB_TIMEOUT_SECONDS
    assert reboot_at < finished_time < deadline, (
        f"in-flight completion at {finished_time} outside "
        f"({reboot_at}, {deadline}) (measured 26.113419): {client_log}"
    )

    _assert_oracle_clean(client_log)
    _assert_no_unswapped_imports(results)


def test_inflight_worker_restart_is_replay_deterministic():
    assert (
        _run_worker_restart_mid_workflow()
        == _run_worker_restart_mid_workflow()
    )


# ---------------------------------------------------------------------------
# Scenario 3 (FIXED BUG, pinned): a submission landing well after the
# rebooted worker re-registered must dispatch — never livelock
# ---------------------------------------------------------------------------

# ~8s after gen-2's reboot (the restart at 2.0, down 10, submit at 20.0
# as probed): well after it re-registered.
_LATE_SUBMIT_AFTER_REBOOT_SECONDS = 8.0
# Post-fix probe: submit 20.04, backoff-paced dispatch retries land at
# 34.25, completion 34.603 (re-probed 2026-10-07: 20.66, no retry
# needed). Bound: acceptance + the full designed backoff ladder
# (1+2+4+8s) + dispatch/drain slack — anything past it would mean pacing
# regressed toward the old frozen-instant behavior.
_LATE_COMPLETION_AFTER_SUBMIT_SECONDS = 20.0


def _run_late_submission_after_worker_restart() -> tuple[dict, dict[str, float]]:
    return _run_worker_restart(
        _arm_idle_restart_then_submit(_FRESH_DOWN_SECONDS + _LATE_SUBMIT_AFTER_REBOOT_SECONDS),
        _FRESH_CEILING,
    )


def test_late_submission_after_worker_restart_must_not_livelock_dispatch():
    """Regression pin for the dispatcher frozen-instant livelock
    (formerly a deterministic reproducer: seed 101, worker restart at
    2.0/down 10, submit at t=20 froze the manager at virtual
    22.029999999983676 — sub-quantum backoff/cooldown remainders armed
    same-instant timers forever; see the module docstring). With the
    1ms progress floors the retry machinery paces honestly: the job
    dispatches onto the rebooted pool and completes within the
    designed backoff ladder."""
    results, fault_instants = _run_late_submission_after_worker_restart()
    client_log = results["client"]
    submit_at = fault_instants["submit_at"]

    submitted_time = _submitted_at(client_log)
    assert submit_at <= submitted_time < submit_at + 1.0, client_log

    (_tag, final_status, finished_time) = _finished(client_log)
    assert final_status == "completed", client_log
    completion_ceiling = submit_at + _LATE_COMPLETION_AFTER_SUBMIT_SECONDS
    assert submitted_time < finished_time <= completion_ceiling, (
        f"late-submission completion at {finished_time} outside the "
        f"backoff-ladder design bound (measured 20.66): {client_log}"
    )

    # The rebooted generation executed it.
    generation_two_activations = _activation_times(results["worker"])
    assert generation_two_activations, results["worker"]
    assert generation_two_activations[0] > submit_at, results["worker"]

    _assert_oracle_clean(client_log)
    _assert_no_unswapped_imports(results)


def test_late_submission_after_worker_restart_is_replay_deterministic():
    assert (
        _run_late_submission_after_worker_restart()
        == _run_late_submission_after_worker_restart()
    )
