"""
K4/L3 — the manager->client PUSH-STREAM under stress in the gateless
L2 topology: jittered delay reordering separate push exchanges, heavy
UDP duplication/loss churning the membership plane under a LIVE 30s
workflow, and the client's 5s poll fallback racing every delayed push.

Fault scoping honesty (the A9/J1 ledger, enforced at the coordinator
chokepoint): status pushes, poll responses, and dispatch all ride TCP
STREAMS, which ``schedule_drop_rate``/``schedule_duplicate``
deliberately exempt (real TCP masks loss via retransmission and never
delivers a frame twice) — so wire-level duplication of pushes is a
non-production schedule. The duplication pressure that IS
production-real and modeled here:

* ``schedule_delay`` with jitter applies to stream frames — separate
  request/response exchanges on the manager->client link reorder
  against each other (each ``send_tcp`` is its own framed exchange),
  exactly the late-push-racing-poll interleaving L3 targets;
* the client's ``wait_for_job`` poll (5s cadence,
  ``ClientJobTracker.DEFAULT_POLL_INTERVAL_SECONDS``) re-reads
  status/stats it may already have seen from a push — a semantic
  duplicate absorbed by ``JobStatusApplier``'s order guard;
* heavy UDP duplication/loss on manager<->worker churns the SWIM
  heartbeat plane (dedup-eligible gossip, idempotent control
  messages) while the workflow runs — the K4 composite;
* ``schedule_duplicate("manager", "client", ...)`` is ALSO armed to
  keep the datagram-scoping exclusion visible in the schedule: the
  client/manager exchange is stream-only, so the rule is inert by
  design (documented, not accidental).

The oracle battery: client history linearizes (``JobStatusOracle``),
observed stats are NEVER wound back (``stats-seen`` monotone), the
final stats equal the ACTION chain's deterministic total (one completed
action per step, ``SUSTAINED_ACTION_STEP_COUNT`` — VU
counters do not tick for one-shot action DAGs), and the terminal is
exactly-once at execution length + push latency.

Measured (probe scripts in the L2 workload series):

* K4 composite (seed 107, delay 0.25+j0.2 on manager->client over
  [8, 60), duplicate 0.7 + drop 0.2 on manager<->worker): submit
  14.441652, running seen 14.941652, execution 14.25 -> 45.0 on the
  worker, completion 44.851528 (submit + 30.41).
* L3 heavy duplication + jitter (seed 127, delay 0.3+j0.6 on
  manager->client over [8, 60), duplicate 0.9 manager->client armed
  inert): submit 9.46379, running seen 10.46379 (one jittered leg
  late), completion 40.569162 (submit + 31.11), worker drain sample
  9.25 -> 40.75 (the 1.5s stretch is the delayed terminal-push leg
  backpressuring the final-result ack).
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

_WORKFLOW_DURATION_SECONDS = 30.0
_STRESS_WINDOW_START = 8.0
_STRESS_WINDOW_END = 60.0
_CEILING = 130.0


def _build_topology(seed: int) -> SimulationCoordinator:
    coordinator = SimulationCoordinator(
        latency=0.01, max_virtual_time=_CEILING, seed=seed
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
    return coordinator


def _run_push_stress_composite() -> dict:
    """K4: jittered delay on the push link + heavy UDP duplication and
    loss on the membership plane, all overlapping live execution."""
    coordinator = _build_topology(seed=107)
    coordinator.schedule_delay(
        "manager",
        "client",
        0.25,
        at_time=_STRESS_WINDOW_START,
        until_time=_STRESS_WINDOW_END,
        jitter_seconds=0.2,
    )
    coordinator.schedule_duplicate("manager", "client", 0.7)
    coordinator.schedule_duplicate("manager", "worker", 0.7)
    coordinator.schedule_duplicate("worker", "manager", 0.7)
    coordinator.schedule_drop_rate("manager", "worker", 0.2)
    coordinator.schedule_drop_rate("worker", "manager", 0.2)
    return coordinator.run()


def _run_heavy_duplication_jitter() -> dict:
    """L3: duplication at 0.9 (inert on the stream-only client link by
    the documented datagram scoping) + jitter LARGER than the delay
    base, maximizing reorder of adjacent push/poll exchanges."""
    coordinator = _build_topology(seed=127)
    coordinator.schedule_delay(
        "manager",
        "client",
        0.3,
        at_time=_STRESS_WINDOW_START,
        until_time=_STRESS_WINDOW_END,
        jitter_seconds=0.6,
    )
    coordinator.schedule_duplicate("manager", "client", 0.9)
    return coordinator.run()


def _assert_clean_observation_under_stress(
    results: dict, max_push_latency_seconds: float
) -> None:
    client_log = results["client"]

    submitted = [entry for entry in client_log if entry[0] == "job-submitted"]
    assert len(submitted) == 1, client_log
    submitted_time = submitted[0][1]

    finished = [entry for entry in client_log if entry[0] == "job-finished"]
    assert len(finished) == 1, client_log
    assert finished[0][1] == "completed", client_log
    # The terminal lands at execution length + (possibly delayed) push
    # legs — the delay budget bounds it, and anything EARLIER than the
    # execution length would mean a phantom completion.
    terminal_latency = finished[0][2] - submitted_time
    assert (
        _WORKFLOW_DURATION_SECONDS
        <= terminal_latency
        <= _WORKFLOW_DURATION_SECONDS + max_push_latency_seconds
    ), (
        f"terminal latency {terminal_latency}s outside the "
        f"execution+push-delay budget: {client_log}"
    )

    # The RUNNING transition was observed BEFORE the terminal — the
    # push stream delivered mid-run state, not just the terminal.
    running_seen = [
        entry
        for entry in client_log
        if entry[0] == "status-seen" and entry[1] == "running"
    ]
    assert running_seen, client_log
    assert running_seen[0][2] < finished[0][2], client_log

    # Client-observed history linearizes: rank-monotone statuses,
    # absorbing terminal, exactly-once result delivery.
    assert JobStatusOracle().check_client_log(client_log) == [], client_log

    # The stats view is NEVER wound back by late/reordered/duplicated
    # pushes racing polls, starts at zero, and never exceeds the ACTION
    # chain's deterministic total (one completed action per step) -- a
    # count past it, or any failure, is a phantom.
    stats_sequence = [
        (entry[1], entry[2])
        for entry in client_log
        if entry[0] == "stats-seen"
    ]
    for earlier, later in zip(stats_sequence, stats_sequence[1:]):
        assert later[0] >= earlier[0] and later[1] >= earlier[1], (
            f"stats wound back: {earlier} -> {later}: {client_log}"
        )
    assert stats_sequence[0] == (0, 0), client_log
    assert all(
        completed <= SUSTAINED_ACTION_STEP_COUNT and failed == 0
        for completed, failed in stats_sequence
    ), client_log
    final_stats = [entry for entry in client_log if entry[0] == "final-stats"]
    assert final_stats == [
        ("final-stats", SUSTAINED_ACTION_STEP_COUNT, 0, finished[0][2])
    ], client_log

    # The worker executed the workflow exactly once, for its full
    # parameterized window (no stress-induced re-execution). The drain
    # sample can lag the execution length by the DELAYED terminal-push
    # leg: the manager's completion handler awaits its client push
    # inline, and the worker's final-result ack rides behind it — so
    # the same per-scenario delay budget bounds the span.
    worker_log = results["worker"]
    started = [
        entry for entry in worker_log if entry[0] == "workflow-started"
    ]
    executed = [
        entry for entry in worker_log if entry[0] == "workflow-executed"
    ]
    assert len(started) == 1 and len(executed) == 1, worker_log
    execution_span = executed[0][2] - started[0][2]
    assert (
        _WORKFLOW_DURATION_SECONDS - 0.5
        <= execution_span
        <= _WORKFLOW_DURATION_SECONDS + max_push_latency_seconds
    ), worker_log

    # G3/G4 cross-node checks: execution-start count within the
    # declared no-retry budget, and no unswapped-seam audit rows in
    # any process result.
    trace_oracle = ClusterTraceOracle(
        worker_process_ids_by_datacenter={"sim-dc": ("worker",)},
        retry_budget=0,
    )
    assert trace_oracle.check_workflow_execution(results) == [], results
    assert (
        ClusterTraceOracle.check_determinism_audit_absence(results) == []
    ), results


def test_job_observation_stays_clean_through_push_stress_composite():
    results = _run_push_stress_composite()
    # Delay base 0.25 + jitter 0.2 per leg; dispatch + terminal legs
    # plus watcher granularity bound the extra latency at ~2s.
    _assert_clean_observation_under_stress(
        results, max_push_latency_seconds=2.0
    )


def test_push_stress_composite_is_replay_deterministic():
    assert _run_push_stress_composite() == _run_push_stress_composite()


def test_job_observation_stays_clean_through_heavy_duplication_jitter():
    results = _run_heavy_duplication_jitter()
    # Delay base 0.3 + jitter 0.6 per leg over multiple legs bounds the
    # extra latency at ~3s.
    _assert_clean_observation_under_stress(
        results, max_push_latency_seconds=3.0
    )


def test_heavy_duplication_jitter_is_replay_deterministic():
    assert (
        _run_heavy_duplication_jitter() == _run_heavy_duplication_jitter()
    )
