"""
AD-44 late datacenter results, under each ``BEST_EFFORT_LATE_RESULT_POLICY``,
over the L3 two-datacenter topology: one durable gate (ledger on a
SimFilesystem), dc-east and dc-west each a real manager + 2-core worker,
a client submitting one two-datacenter best-effort job (``min_dcs=1``).

The straggler is made by the run, not by a clock: once dc-west's worker
reports the job's workflow active, dc-west's manager is frozen (SIGSTOP
model) for the workflow's declared duration plus twice the terminal
slack -- dc-east completes the job meanwhile -- and then thawed, when it
reports dc-west's final result late.

Pinned outcomes:

* ``log`` (default): the job completes with dc-east alone, final, naming
  dc-west unreported; the ledger records its terminal then. dc-west's
  late result is logged (``LateDatacenterResult``, ``logged``) and not
  aggregated: the client's result never changes.
* ``update``: the client gets the dc-east result at once, provisional
  (``is_final`` False, dc-west unreported) and the ledger holds the job
  open. dc-west's late result is folded in (``LateDatacenterResult``,
  ``updated``): the client's result becomes final with both
  datacenters, and only then does the ledger record the job's terminal,
  with both datacenters' totals.
* ``update`` with dc-west lost outright (killed on the same event): the
  provisional result stands until the job's deadline, then goes out
  final and the ledger records the terminal -- within one deadline check
  of the deadline.
* ``update`` with the gate restarted once the client holds the
  provisional result (and dc-east, whose result it holds, lost at that
  instant): the restarted gate resumes the window from the job's
  committed replica, and the final result -- on dc-west's late result --
  holds every datacenter result the client received.
* ``update`` with the gate restarted once the client holds the
  provisional result (and dc-east, whose result it holds, lost at that
  instant): the restarted gate resumes the window from the job's
  committed replica, and the final result -- on dc-west's late result --
  holds every datacenter result the client received.
* Mutation checks: with the straggler fold removed, the late-result
  judgement removed, or the restarted gate's restore of the window
  removed, the scenario's own checks report the breakage.
"""

from hyperscale.distributed.env import Env
from tests.simulation.harness.sim.multiprocess import SimulationCoordinator
from tests.simulation.harness.sim.multiprocess.best_effort_late_result_demo import (
    late_result_client_entry,
    late_result_gate_entry,
)
from tests.simulation.harness.sim.multiprocess.multi_dc_fault_demo import (
    faulted_multi_gate_manager_entry,
)
from tests.simulation.harness.sim.multiprocess.worker_manager_demo import worker_entry
from tests.unit.simulation.sim.test_multiprocess_best_effort import _SUBMIT_AT
from tests.unit.simulation.sim.test_multiprocess_dc_loss import (
    _DC_VICTIMS,
    _LATENCY,
    _SEED,
    _TERMINAL_SLACK_SECONDS,
    _assert_no_unswapped_imports,
    _assert_oracle_clean,
)

_DURATION_SECONDS = 4.0
_JOB_TIMEOUT_SECONDS = 60.0
# Long enough that dc-west's late result, not the deadline, ends the
# released job's window.
_DEADLINE_SECONDS = 60.0
# dc-west's manager stays frozen through dc-east's whole run (its
# workflow's declared duration) and the delivery of the job's result.
_FREEZE_SECONDS = _DURATION_SECONDS + 2 * _TERMINAL_SLACK_SECONDS
_CEILING = _SUBMIT_AT + _JOB_TIMEOUT_SECONDS
# A lost datacenter never reports: the released job's window closes at
# its deadline (checked every BEST_EFFORT_DEADLINE_CHECK_INTERVAL).
_LOSS_DEADLINE_SECONDS = 20.0
_DEADLINE_CHECK_INTERVAL_SECONDS = Env().BEST_EFFORT_DEADLINE_CHECK_INTERVAL
_TERMINAL_LEDGER_STATUSES = frozenset({"completed", "failed", "cancelled", "timeout", "timed_out"})


def _is_provisional_result(row: tuple) -> bool:
    """The client holds a result the gate may still update (``is_final`` False)."""
    return row[0] == "result-seen" and row[4] is False


def _is_west_running(row: tuple) -> bool:
    return row[0] == "workflows-active" and row[1] > 0


def _run(
    late_result_policy: str,
    mutation: str | None = None,
    lose_west: bool = False,
    deadline_seconds: float = _DEADLINE_SECONDS,
    restart_gate: bool = False,
) -> tuple[dict, dict[str, float]]:
    coordinator = SimulationCoordinator(latency=_LATENCY, max_virtual_time=_CEILING, seed=_SEED)
    coordinator.add_process(
        "sim-gate-a",
        late_result_gate_entry,
        "sim-gate-a",
        9000,
        9001,
        {"dc-east": [("sim-mgr-east", 9000)], "dc-west": [("sim-mgr-west", 9000)]},
        {"dc-east": [("sim-mgr-east", 9001)], "dc-west": [("sim-mgr-west", 9001)]},
        late_result_policy,
        mutation,
    )
    for datacenter_id, manager_host, worker_host in (
        ("dc-east", "sim-mgr-east", "sim-wkr-east"),
        ("dc-west", "sim-mgr-west", "sim-wkr-west"),
    ):
        coordinator.add_process(
            f"manager-{datacenter_id}",
            faulted_multi_gate_manager_entry,
            manager_host,
            9000,
            9001,
            datacenter_id,
            [("sim-gate-a", 9000)],
            [("sim-gate-a", 9001)],
            (),
        )
        coordinator.add_process(
            f"worker-{datacenter_id}",
            worker_entry,
            worker_host,
            9000,
            9001,
            datacenter_id,
            (manager_host, 9000),
            2,
        )
    coordinator.add_process(
        "client-a",
        late_result_client_entry,
        "sim-cli-a",
        9500,
        ("sim-gate-a", 9000),
        _DURATION_SECONDS,
        _JOB_TIMEOUT_SECONDS,
        _SUBMIT_AT,
        1,
        deadline_seconds,
    )
    freeze_window: dict[str, float] = {}

    def freeze_west_manager(running_row: tuple) -> None:
        freeze_at = running_row[2] + _LATENCY
        freeze_window["freeze_at"] = freeze_at
        freeze_window["thaw_at"] = freeze_at + _FREEZE_SECONDS
        coordinator.schedule_pause("manager-dc-west", freeze_at, freeze_window["thaw_at"])

    def kill_west(running_row: tuple) -> None:
        loss_at = running_row[2] + _LATENCY
        freeze_window["freeze_at"] = loss_at
        freeze_window["thaw_at"] = float("inf")
        for victim_id in _DC_VICTIMS["dc-west"]:
            coordinator.schedule_kill(victim_id, loss_at)

    def restart_gate_after_provisional_push(provisional_row: tuple) -> None:
        # The gate goes down once the client holds the provisional result
        # and is back halfway to dc-west's thaw: dc-west's late result
        # reaches the restarted generation. dc-east -- whose result the
        # client holds -- is lost at the same instant, so nothing but the
        # gate's own durable state can bring that result back.
        restart_at = provisional_row[-1] + _LATENCY
        freeze_window["restart_at"] = restart_at
        coordinator.schedule_restart(
            "sim-gate-a", restart_at, down_seconds=(freeze_window["thaw_at"] - restart_at) / 2
        )
        for victim_id in _DC_VICTIMS["dc-east"]:
            coordinator.schedule_kill(victim_id, restart_at)

    coordinator.schedule_on_event("worker-dc-west", _is_west_running, kill_west if lose_west else freeze_west_manager)
    if restart_gate:
        coordinator.schedule_on_event("client-a", _is_provisional_result, restart_gate_after_provisional_push)
    return coordinator.run(), freeze_window


def _rows(log: list, tag: str) -> list[tuple]:
    return [row for row in log if row[0] == tag]


def _results_seen(client_log: list) -> list[tuple]:
    """``(status, reason, unreported, is_final, reported, total_completed, t)``
    for the result the client held after ``wait_for_job`` and each change."""
    return [row[1:] for row in _rows(client_log, "result-seen")]


def _common_violations(results: dict, freeze_window: dict[str, float]) -> list[str]:
    """What both policies share: the job completed with dc-east, while
    dc-west's manager was frozen, never by a timeout."""
    violations: list[str] = []
    finished = _rows(results["client-a"], "job-finished")
    if [row[1] for row in finished] != ["completed"]:
        violations.append(f"job did not finish completed exactly once: {finished}")
    elif not freeze_window["freeze_at"] < finished[0][2] < freeze_window["thaw_at"]:
        violations.append(f"job did not finish while dc-west was frozen: {finished}, {freeze_window}")
    return violations


def _late_result_rows(gate_log: list, freeze_window: dict[str, float]) -> list[tuple]:
    """``(datacenter, outcome, job_status)`` of each late result logged after the thaw."""
    return [row[1:4] for row in _rows(gate_log, "late-datacenter-result") if row[4] >= freeze_window["thaw_at"]]


def _ledger_terminals(gate_log: list) -> list[tuple]:
    """``(status, completed_count, t)`` of each terminal ledger record of the job."""
    return [row[1:] for row in _rows(gate_log, "ledger-job") if row[1] in _TERMINAL_LEDGER_STATUSES]


def _log_policy_violations(results: dict, freeze_window: dict[str, float]) -> list[str]:
    violations = _common_violations(results, freeze_window)
    gate_log = results["sim-gate-a"]
    results_seen = _results_seen(results["client-a"])
    if [seen[:5] for seen in results_seen] != [
        ("completed", "best_effort: min_dcs_reached (1/1)", ("dc-west",), True, ("dc-east",))
    ]:
        violations.append(f"client result was not the final dc-east result, unchanged: {results_seen}")
    if _late_result_rows(gate_log, freeze_window) != [("dc-west", "logged", "completed")]:
        violations.append(f"dc-west's late result was not logged once: {_rows(gate_log, 'late-datacenter-result')}")
    terminals = _ledger_terminals(gate_log)
    if not (terminals and terminals[0][2] < freeze_window["thaw_at"]):
        violations.append(f"ledger terminal not recorded at completion: {terminals}")
    if len({terminal[:2] for terminal in terminals}) != 1:
        violations.append(f"ledger terminal changed after completion: {terminals}")
    return violations


def _update_policy_violations(results: dict, freeze_window: dict[str, float]) -> list[str]:
    violations = _common_violations(results, freeze_window)
    gate_log = results["sim-gate-a"]
    results_seen = _results_seen(results["client-a"])
    expected_shapes = [
        ("completed", "best_effort: min_dcs_reached (1/1)", ("dc-west",), False, ("dc-east",)),
        ("completed", "best_effort: all_dcs_reported", (), True, ("dc-east", "dc-west")),
    ]
    if [seen[:5] for seen in results_seen] != expected_shapes:
        violations.append(f"client did not get the provisional then the updated final result: {results_seen}")
    elif not (results_seen[1][5] > results_seen[0][5] and results_seen[1][6] >= freeze_window["thaw_at"]):
        violations.append(f"the update did not add dc-west's work after the thaw: {results_seen}")
    if _late_result_rows(gate_log, freeze_window) != [("dc-west", "updated", "completed")]:
        violations.append(f"dc-west's late result was not folded in once: {_rows(gate_log, 'late-datacenter-result')}")
    terminals = _ledger_terminals(gate_log)
    final_total = results_seen[-1][5] if results_seen else None
    if not terminals or terminals[0][2] < freeze_window["thaw_at"] or terminals[0][:2] != ("completed", final_total):
        violations.append(
            f"ledger terminal not recorded once, after the straggler, with the final totals: {terminals}, {final_total}"
        )
    return violations


def _completion_logs(gate_log: list) -> list[tuple]:
    """``(reason, final, unreported)`` of each ``BestEffortCompletion``."""
    return [row[1:4] for row in _rows(gate_log, "best-effort-completion")]


def test_log_policy_logs_the_late_result_and_keeps_the_job_result():
    results, freeze_window = _run("log")

    assert not _log_policy_violations(results, freeze_window), (
        _log_policy_violations(results, freeze_window),
        results["client-a"],
        results["sim-gate-a"],
    )
    assert _completion_logs(results["sim-gate-a"]) == [
        ("best_effort: min_dcs_reached (1/1)", True, ("dc-west",))
    ], results["sim-gate-a"]
    _assert_oracle_clean(results["client-a"])
    _assert_no_unswapped_imports(results)


def test_update_policy_folds_the_late_result_and_records_the_terminal_once():
    results, freeze_window = _run("update")

    assert not _update_policy_violations(results, freeze_window), (
        _update_policy_violations(results, freeze_window),
        results["client-a"],
        results["sim-gate-a"],
    )
    assert _completion_logs(results["sim-gate-a"]) == [
        ("best_effort: min_dcs_reached (1/1)", False, ("dc-west",)),
        ("best_effort: all_dcs_reported", True, ()),
    ], results["sim-gate-a"]
    _assert_oracle_clean(results["client-a"])
    _assert_no_unswapped_imports(results)


def test_update_policy_run_is_replay_deterministic():
    assert _run("update") == _run("update")


def test_mutation_without_the_straggler_fold_is_caught():
    """With the fold removed, a released job's straggler is judged like a
    result for any ended job: the client never gets the update and the
    ledger never records the terminal -- the update checks say so."""
    results, freeze_window = _run("update", mutation="never-folds-stragglers")

    assert _update_policy_violations(results, freeze_window), (results["client-a"], results["sim-gate-a"])


def test_mutation_without_the_late_judgement_is_caught():
    """With no result judged late, the log policy's check misses the
    ``LateDatacenterResult`` -- and says so."""
    results, freeze_window = _run("log", mutation="never-late")

    assert _log_policy_violations(results, freeze_window), (results["client-a"], results["sim-gate-a"])


def test_update_policy_closes_a_lost_datacenters_window_at_the_deadline():
    """dc-west is lost outright: the provisional result stands until the
    job's deadline, then goes out final (dc-west still unreported) and the
    ledger records the terminal then -- never held open past the bound
    the job declared."""
    results, loss = _run("update", lose_west=True, deadline_seconds=_LOSS_DEADLINE_SECONDS)
    client_log = results["client-a"]
    results_seen = _results_seen(client_log)

    assert [seen[:5] for seen in results_seen] == [
        ("completed", "best_effort: min_dcs_reached (1/1)", ("dc-west",), False, ("dc-east",)),
        ("completed", "best_effort: deadline_expired (completed: 1)", ("dc-west",), True, ("dc-east",)),
    ], client_log
    earliest = _rows(client_log, "job-submitted")[0][1] + _LOSS_DEADLINE_SECONDS
    latest = earliest + _DEADLINE_CHECK_INTERVAL_SECONDS + _TERMINAL_SLACK_SECONDS
    assert earliest <= results_seen[1][6] <= latest, (earliest, latest, results_seen)
    terminals = _ledger_terminals(results["sim-gate-a"])
    assert terminals and terminals[0][:2] == ("completed", results_seen[1][5]), results["sim-gate-a"]
    assert earliest <= terminals[0][2] <= latest + _TERMINAL_SLACK_SECONDS, (terminals, earliest, latest)
    assert loss["freeze_at"] < results_seen[0][6], (loss, results_seen)
    _assert_oracle_clean(client_log)
    _assert_no_unswapped_imports(results)


def _restart_violations(results: dict, window: dict[str, float]) -> list[str]:
    """After a gate restart between the provisional push and the deadline,
    the final result holds every datacenter result of the provisional one
    (and their work), completes when dc-west's late result reaches the
    restarted gate, and the ledger records the job's terminal once."""
    violations: list[str] = []
    results_seen = _results_seen(results["client-a"])
    provisional = [seen for seen in results_seen if seen[3] is False]
    final = [seen for seen in results_seen if seen[3] is True]
    if not (provisional and final and provisional[0][6] < window["restart_at"]):
        return [f"no provisional result before the restart and final after it: {results_seen}, {window}"]
    if not (set(final[-1][4]) >= set(provisional[-1][4]) and final[-1][5] >= provisional[-1][5]):
        violations.append(f"final result lost provisional datacenter results: {provisional[-1]} -> {final[-1]}")
    if final[-1][:5] != ("completed", "best_effort: all_dcs_reported", (), True, ("dc-east", "dc-west")):
        violations.append(f"final result is not the job completed with both datacenters: {final[-1]}")
    if not window["thaw_at"] <= final[-1][6] <= window["thaw_at"] + _TERMINAL_SLACK_SECONDS:
        violations.append(f"final result not delivered on dc-west's late result: {final[-1]}, {window}")
    terminals = _ledger_terminals(results["sim-gate-a"])
    if not terminals or terminals[0][:2] != ("completed", final[-1][5]):
        violations.append(f"ledger terminal missing or not the final totals: {terminals}")
    return violations


def test_update_policy_final_result_holds_the_provisional_one_across_a_gate_restart():
    results, window = _run("update", restart_gate=True)

    assert not _restart_violations(results, window), (
        _restart_violations(results, window),
        results["client-a"],
        results["sim-gate-a"],
    )
    _assert_oracle_clean(results["client-a"])
    _assert_no_unswapped_imports(results)


def test_gate_restart_run_is_replay_deterministic():
    assert _run("update", restart_gate=True) == _run("update", restart_gate=True)


def test_mutation_without_the_provisional_restore_is_caught():
    """With the restarted gate not resuming the window from the replica,
    dc-east's result -- which the client held -- is gone (dc-east is lost
    too): the restart checks say so."""
    results, window = _run("update", restart_gate=True, mutation="never-restores-provisional")

    assert _restart_violations(results, window), (results["client-a"], results["sim-gate-a"])
