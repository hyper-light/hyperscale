"""
L2 — completion-push loss vs the poll fallback: a ONE-WAY partition
(manager->client only) covering the completion instant in the gateless
topology. The sharper asymmetric variant of the push-loss scenario:
the client can still REACH the manager the whole time — its poll
requests arrive and are processed — but every response and every push
sent during the window drops (the coordinator's partition rules key on
SEND time and direction).

Traced mechanism the bounds derive from:

* The manager's terminal ``job_status_push`` is a SINGLE
  ``send_tcp(..., timeout=5.0)`` with no retry
  (``_push_job_status_to_client``); a cut across the completion
  instant loses it PERMANENTLY. Gateless deployments have no other
  re-push source (the windowed-stats flush targets gates), so the
  poll fallback is the ONLY convergence path — exactly what L2 must
  prove works.
* ``ClientJobTracker.wait_for_job`` polls ``job_status`` every 5s
  (``DEFAULT_POLL_INTERVAL_SECONDS``) with a 5s request timeout; the
  manager answers from live state or its durable ledger. During the
  cut each poll's response drops and the request times out (the
  pooled transport is closed and re-dialed; the re-dial's SYN-ACK
  also drops). Worst case around the heal H: a poll started at H-e
  times out at H+5-e, sleeps 5, and the next poll at H+10-e
  converges — the terminal must land in (H, H + 10.5].

Measured on seed 97 (30s workflow, cut [27, 47), re-probed 2026-10-04):
baseline completion push at 32.729669 (submit 2.649669, execution 2.75
-> 33.0). Under the cut the terminal push is lost and the poll fallback
learns ``completed`` at 48.15 (heal + 1.15); the workflow's result push
was lost in the cut too, so ``wait_for_job`` waits its result drain,
asks the manager to replay what it kept for the client (results are
kept until delivered), and returns at 52.77 -- heal + 5.77, inside the
10.5s budget. The worker's drain sample moves 33.0 -> 38.25: the
manager's completion handler awaits its (cut) client push inline for
the full 5s send timeout, and the worker's final-result ack rides behind
it -- the cut client link back-pressures the worker drain by one push
timeout, a mechanism this scenario deliberately leaves visible in the
pinned worker log. (Submission was 15.74 while the lone manager waited
out a full pre-vote and vote wait for a majority its own vote already
made; the cut was then [40, 60).)
"""

from tests.simulation.harness.sim.multiprocess import SimulationCoordinator
from tests.simulation.harness.sim.multiprocess.l2_workload_demo import (
    dag_worker_entry,
    SUSTAINED_ACTION_STEP_COUNT,
    sustained_client_entry,
)
from tests.simulation.harness.sim.multiprocess.worker_manager_demo import (
    manager_entry,
)
from tests.simulation.oracle import ClusterTraceOracle, JobStatusOracle

_SEED = 97
_WORKFLOW_DURATION_SECONDS = 30.0
# Baseline (fault-free) completion push instant for this seed, probed:
# the cut is placed to STRADDLE it.
_BASELINE_COMPLETION = 32.729669
_CUT_AT = 27.0
_HEAL_AT = 47.0
_CEILING = 130.0
# Poll-fallback convergence budget after heal: one straddling poll
# timeout (5) + one poll sleep (5) + RTT slop.
_MAX_CONVERGENCE_AFTER_HEAL = 10.5


def _run_one_way_completion_cut() -> dict:
    coordinator = SimulationCoordinator(
        latency=0.01, max_virtual_time=_CEILING, seed=_SEED
    )
    coordinator.add_process(
        "manager", manager_entry, "sim-mgr", 9000, 9001, "sim-dc"
    )
    coordinator.add_process(
        "worker",
        dag_worker_entry,
        "sim-wkr",
        9000,
        9001,
        "sim-dc",
        ("sim-mgr", 9000),
        2,
    )
    coordinator.add_process(
        "client",
        sustained_client_entry,
        "sim-cli",
        9500,
        ("sim-mgr", 9000),
        _WORKFLOW_DURATION_SECONDS,
        1,
        60.0,
        90.0,
    )
    coordinator.schedule_partition(
        "manager",
        "client",
        _CUT_AT,
        heal_time=_HEAL_AT,
        bidirectional=False,
    )
    return coordinator.run()


def test_poll_fallback_converges_terminal_after_one_way_push_loss():
    results = _run_one_way_completion_cut()
    client_log = results["client"]

    submitted = [entry for entry in client_log if entry[0] == "job-submitted"]
    assert len(submitted) == 1, client_log
    # Probe-pinned scheduling: submission and the RUNNING observation
    # land BEFORE the cut; the completion instant lands INSIDE it.
    assert submitted[0][1] < _CUT_AT, client_log
    running_seen = [
        entry
        for entry in client_log
        if entry[0] == "status-seen" and entry[1] == "running"
    ]
    assert running_seen and running_seen[0][2] < _CUT_AT, client_log

    # The worker's execution drained INSIDE the cut window — the
    # terminal push was provably sent into the dead direction.
    worker_log = results["worker"]
    executed = [
        entry for entry in worker_log if entry[0] == "workflow-executed"
    ]
    assert len(executed) == 1, worker_log
    assert _CUT_AT < executed[0][2] < _HEAL_AT, worker_log

    finished = [entry for entry in client_log if entry[0] == "job-finished"]
    assert len(finished) == 1, client_log
    assert finished[0][1] == "completed", client_log
    finished_time = finished[0][2]
    # NOTHING terminal was observed during the cut (the push is gone
    # for good), and the poll fallback converges inside its traced
    # budget after heal — bounded, never silence.
    assert finished_time > _HEAL_AT, (
        f"terminal observed during the cut — the one-way partition "
        f"failed to drop the push: {client_log}"
    )
    assert finished_time <= _HEAL_AT + _MAX_CONVERGENCE_AFTER_HEAL, (
        f"poll fallback took {finished_time - _HEAL_AT}s after heal — "
        f"outside the 5s-cadence + 5s-timeout budget: {client_log}"
    )

    # History linearizes; the final stats are the ACTION chain's total --
    # one completed action per step.
    assert JobStatusOracle().check_client_log(client_log) == [], client_log
    final_stats = [entry for entry in client_log if entry[0] == "final-stats"]
    assert final_stats == [
        ("final-stats", SUSTAINED_ACTION_STEP_COUNT, 0, finished_time)
    ], client_log

    # G3/G4: exactly-once execution evidence and audit absence.
    trace_oracle = ClusterTraceOracle(
        worker_process_ids_by_datacenter={"sim-dc": ("worker",)},
        retry_budget=0,
    )
    assert trace_oracle.check_workflow_execution(results) == [], results
    assert (
        ClusterTraceOracle.check_determinism_audit_absence(results) == []
    ), results


def test_one_way_completion_cut_is_replay_deterministic():
    assert _run_one_way_completion_cut() == _run_one_way_completion_cut()
