"""
AD-26 H6 fed from the progress path: a workflow whose own action rate
collapses is denied at the controlled α, and healthy throughput is falsely
denied no more often than that α allows.

VOPR-style over many seeds, with the manager's production witness
configuration (``throughput_witness_config`` from the default ``Env``: the
FPR budget, the 0.625 s sampling interval and the 192-run window derived
from the trigger poll and base deadline). A simulated worker runs two
workflows whose actions complete as Poisson processes and reports each one's
``completed_count`` / ``elapsed_seconds`` every
``WORKER_PROGRESS_FLUSH_INTERVAL``; the reports reach the witness through
``WorkerHealthManager.ingest_workflow_progress`` (what the manager's
``workflow_progress`` handler calls), and each workflow asks for an extension
every ``HYPERSCALE_EXTENSION_TRIGGER_INTERVAL``.
"""

from __future__ import annotations

import random
from types import SimpleNamespace

import pytest

from hyperscale.distributed.env.env import Env
from hyperscale.distributed.health import WorkerHealthManager, WorkerHealthManagerConfig
from hyperscale.distributed.health.extension_decision import (
    ExtensionDecision,
    ExtensionDecisionConfig,
    ExtensionDenialCode,
)
from hyperscale.distributed.health.progress_witness import (
    BayesianOnlineChangePointDetector,
    BOCPDConfig,
    ThroughputWitness,
)
from hyperscale.distributed.health.progress_witness.witness_feed_derivation import throughput_witness_config
from hyperscale.distributed.health.workflow_progress_snapshot import WorkflowProgressSnapshot
from hyperscale.distributed.models import HealthcheckExtensionRequest, WorkflowProgress
from hyperscale.distributed.nodes.manager.server import ManagerServer
from hyperscale.distributed.taskex.util.time_parser import TimeParser

SEEDS = range(40)
SETTINGS = Env()
REPORT_INTERVAL_SECONDS = SETTINGS.WORKER_PROGRESS_FLUSH_INTERVAL
TRIGGER_INTERVAL_SECONDS = TimeParser(SETTINGS.HYPERSCALE_EXTENSION_TRIGGER_INTERVAL).time
HEALTHY_WORKFLOW = "dc:manager:job:wf-healthy:worker-1"
COLLAPSING_WORKFLOW = "dc:manager:job:wf-collapsing:worker-1"
WORKER_ID = "worker-1"
ACTIVE_WORKFLOWS = 2


class SteppedClock:
    """The manager's monotonic clock, advanced by the scenario."""

    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now

    def monotonic_ns(self) -> int:
        return int(self.now * 1_000_000_000)


def production_witness(clock: SteppedClock) -> ThroughputWitness:
    return ThroughputWitness(
        throughput_witness_config(
            SETTINGS.HYPERSCALE_EXTENSION_FPR_BUDGET,
            TRIGGER_INTERVAL_SECONDS,
            SETTINGS.EXTENSION_BASE_DEADLINE,
        ),
        clock=clock,
    )


def build_health_manager(clock: SteppedClock) -> WorkerHealthManager:
    """Production witness and α; the extension cap and spacing are lifted
    so every trigger poll reaches the throughput witness (the evaluator's
    spacing check reads the wall clock, not this scenario's)."""
    return WorkerHealthManager(
        config=WorkerHealthManagerConfig(max_extensions=100_000),
        throughput_witness=production_witness(clock),
        decision_config=ExtensionDecisionConfig(min_between_extensions_seconds=0.0),
        clock=clock,
    )


def request_extension(
    health_manager: WorkerHealthManager, workflow_id: str, request_index: int, completed_count: int, now: float
) -> ExtensionDecision:
    request = HealthcheckExtensionRequest(
        worker_id=WORKER_ID,
        reason="autonomous-trigger",
        current_progress=0.5,
        completed_items=request_index,
        total_items=1_000_000,
        estimated_completion=10.0,
        active_workflow_count=ACTIVE_WORKFLOWS,
        workflow_id=workflow_id,
        step_transitions=request_index,
        actions_completed=completed_count,
        snapshot_time=now,
    )
    snapshot = WorkflowProgressSnapshot(
        workflow_id=workflow_id,
        cores_completed=request_index,
        cores_total=1_000_000,
        step_transitions=request_index,
        actions_completed=completed_count,
        snapshot_time=now,
    )
    _, decision, _ = health_manager.handle_extension_request_with_witnesses(
        request=request,
        current_deadline=now + 1_000.0,
        snapshot=snapshot,
        last_snapshot=health_manager.ledger.latest_progress_snapshot(workflow_id),
        throughput=0.0,
        overload_state="healthy",
        active_in_cluster=ACTIVE_WORKFLOWS,
        active_in_dc=ACTIVE_WORKFLOWS,
        active_on_manager=ACTIVE_WORKFLOWS,
        active_on_worker=ACTIVE_WORKFLOWS,
        workflow_class=workflow_id.split(":")[3],
        job_id="job",
        fence_token=1,
        leader_term=1,
    )
    return decision


def run_seed(seed: int) -> dict[str, object]:
    """One worker, two workflows at the same healthy action rate; one of
    them collapses to a fraction of it partway through."""
    draw = random.Random(seed)
    clock = SteppedClock()
    health_manager = build_health_manager(clock)
    healthy_rate = draw.uniform(20.0, 500.0)
    collapsed_rate = healthy_rate * draw.uniform(0.05, 0.4)
    collapse_at = draw.uniform(40.0, 120.0)
    run_seconds = collapse_at + 3 * TRIGGER_INTERVAL_SECONDS
    completed = {HEALTHY_WORKFLOW: 0, COLLAPSING_WORKFLOW: 0}
    decisions: dict[str, list[tuple[float, ExtensionDecision]]] = {HEALTHY_WORKFLOW: [], COLLAPSING_WORKFLOW: []}
    reports_per_poll = round(TRIGGER_INTERVAL_SECONDS / REPORT_INTERVAL_SECONDS)

    for report_index in range(1, round(run_seconds / REPORT_INTERVAL_SECONDS) + 1):
        clock.now = elapsed = report_index * REPORT_INTERVAL_SECONDS
        for workflow_id in completed:
            rate = collapsed_rate if workflow_id == COLLAPSING_WORKFLOW and elapsed > collapse_at else healthy_rate
            completed[workflow_id] += poisson(draw, rate * REPORT_INTERVAL_SECONDS)
            health_manager.ingest_workflow_progress(WORKER_ID, workflow_id, completed[workflow_id], elapsed)
        if report_index % reports_per_poll == 0:
            request_index = report_index // reports_per_poll
            for workflow_id in completed:
                decisions[workflow_id].append(
                    (elapsed, request_extension(health_manager, workflow_id, request_index, completed[workflow_id], elapsed))
                )
    return {"collapse_at": collapse_at, "decisions": decisions, "health_manager": health_manager}


def poisson(draw: random.Random, mean: float) -> int:
    """Knuth's Poisson sampler on the product of uniforms, in log space."""
    count = 0
    remaining = mean
    while (remaining := remaining + random_log(draw)) > 0.0:
        count += 1
    return count


def random_log(draw: random.Random) -> float:
    return -draw.expovariate(1.0)


def denials(decisions: list[tuple[float, ExtensionDecision]]) -> list[tuple[float, ExtensionDecision]]:
    return [(at, decision) for at, decision in decisions if not decision.granted]


def controlled_alpha(health_manager: WorkerHealthManager) -> float:
    """The H6 α for this load; no outcomes are learned, so the H8 weight is 1."""
    return health_manager.throughput_witness.budget.workflow_alpha_from_counts(*(ACTIVE_WORKFLOWS,) * 4)


@pytest.mark.parametrize("seed", SEEDS)
def test_a_collapsed_workflow_is_denied_at_the_controlled_alpha(seed: int) -> None:
    """Denied after the collapse, by the second trigger poll after it (the
    derived interval puts the confirmation's 8 samples inside one poll),
    by the throughput witness, at the controlled α."""
    outcome = run_seed(seed)
    collapse_at: float = outcome["collapse_at"]
    decisions: dict[str, list[tuple[float, ExtensionDecision]]] = outcome["decisions"]

    collapsed_denials = [(at, decision) for at, decision in denials(decisions[COLLAPSING_WORKFLOW]) if at > collapse_at]
    assert collapsed_denials, f"collapse at {collapse_at:.2f}s never denied"
    first_denied_at, first_denial = collapsed_denials[0]
    assert first_denied_at <= collapse_at + 2 * TRIGGER_INTERVAL_SECONDS
    assert all(decision.denial_reason_code is ExtensionDenialCode.THROUGHPUT_REGIME_DOWN for _, decision in collapsed_denials)
    assert first_denial.evidence.throughput_alpha_workflow == controlled_alpha(outcome["health_manager"])


def test_healthy_throughput_is_falsely_denied_at_no_more_than_the_controlled_alpha() -> None:
    """Every extension decision on healthy throughput -- the healthy
    workflow throughout, the other one before its collapse -- over 200
    seeds. Each is one test at α, so false denials are bounded by
    Binomial(n, α): the count must stay under its mean plus three standard
    deviations. ("Never" is not the property of a test at α > 0: measured
    over 400 seeds, 4 of 7,483 healthy decisions were denied, 5.3e-4
    against α = 5e-3.)"""
    healthy_decisions = 0
    false_denials = 0
    alpha = 0.0
    for seed in range(200):
        outcome = run_seed(seed)
        alpha = controlled_alpha(outcome["health_manager"])
        decisions: dict[str, list[tuple[float, ExtensionDecision]]] = outcome["decisions"]
        healthy = decisions[HEALTHY_WORKFLOW] + [
            (at, decision) for at, decision in decisions[COLLAPSING_WORKFLOW] if at <= outcome["collapse_at"]
        ]
        healthy_decisions += len(healthy)
        false_denials += len(denials(healthy))

    binomial_mean = alpha * healthy_decisions
    assert false_denials <= binomial_mean + 3.0 * (binomial_mean * (1.0 - alpha)) ** 0.5, (
        false_denials,
        healthy_decisions,
    )


def test_the_manager_feeds_only_in_flight_workflows() -> None:
    """``ManagerServer._feed_throughput_witness``: a report for an in-flight
    sub-workflow reaches the witness; one arriving after the workflow's end
    does not reopen the stream its end forgot."""
    clock = SteppedClock()
    health_manager = build_health_manager(clock)
    in_flight: set[str] = {HEALTHY_WORKFLOW}
    manager = SimpleNamespace(
        _job_manager=SimpleNamespace(sub_workflow_in_flight=in_flight.__contains__),
        _worker_health_manager=health_manager,
    )

    def report(workflow_id: str, completed_count: int, elapsed_seconds: float) -> WorkflowProgress:
        return WorkflowProgress(
            job_id="job",
            workflow_id=workflow_id,
            workflow_name="Workflow",
            status="running",
            completed_count=completed_count,
            failed_count=0,
            rate_per_second=0.0,
            elapsed_seconds=elapsed_seconds,
        )

    ManagerServer._feed_throughput_witness(manager, WORKER_ID, report(HEALTHY_WORKFLOW, 10, 1.0))
    ManagerServer._feed_throughput_witness(manager, WORKER_ID, report(COLLAPSING_WORKFLOW, 10, 1.0))
    ManagerServer._feed_throughput_witness(manager, None, report(HEALTHY_WORKFLOW, 20, 2.0))
    assert health_manager.throughput_witness.stream_count == 1

    in_flight.discard(HEALTHY_WORKFLOW)
    health_manager.forget_workflow(HEALTHY_WORKFLOW)
    ManagerServer._feed_throughput_witness(manager, WORKER_ID, report(HEALTHY_WORKFLOW, 30, 3.0))
    assert health_manager.throughput_witness.stream_count == 0


@pytest.mark.parametrize("seed", range(12))
def test_the_run_length_cap_never_signals_a_change_the_uncapped_detector_does_not(seed: int) -> None:
    """A stationary stream four times the production cap long: past the
    cap, every sample whose capped MAP run length reads fresh also reads
    fresh to a detector that never reaches its cap. (Dropping the longest
    runs at the cap instead discarded the stream's own regime and read
    2.4-12.5% of those samples as fresh change points, measured
    2026-10-06.)"""
    run_length_max = production_witness(SteppedClock()).config.bocpd.run_length_max
    fresh_threshold = run_length_max // 4
    draw = random.Random(seed)
    samples = [draw.gauss(100.0, 5.0) for _ in range(4 * run_length_max)]
    capped = BayesianOnlineChangePointDetector(BOCPDConfig(run_length_max=run_length_max))
    uncapped = BayesianOnlineChangePointDetector(BOCPDConfig(run_length_max=len(samples) + 1))
    for index, sample in enumerate(samples):
        capped_map = capped.observe(sample).maximum_a_posteriori_run_length()
        uncapped_map = uncapped.observe(sample).maximum_a_posteriori_run_length()
        if index >= run_length_max and capped_map <= fresh_threshold:
            assert uncapped_map <= fresh_threshold, (index, capped_map, uncapped_map)
