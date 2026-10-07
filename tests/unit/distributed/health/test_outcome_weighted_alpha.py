"""
AD-26 H8 closes on the H6 decision: the per-workflow-class outcome
posterior sets the significance level of the throughput witness's test.

The H5 evaluator composes the H6 hierarchical α with the class's learned
failure rate through ``HierarchicalAlphaTuner.alpha_budget`` (p-value
weighting, Genovese, Roeder & Wasserman 2006), and the witness confirms a
BOCPD-proposed change point with a two-sample K-S test at that level. So
a class whose workflows fail gets denied on less evidence than a class
whose workflows complete -- on the identical throughput trace.

Randomized (VOPR-style) over seeds and driven through the production
``WorkerHealthManager`` paths: outcomes enter through
``record_workflow_outcome`` / ``ingest_remote_outcome_event`` and decisions
come out of ``handle_extension_request_with_witnesses``.
"""

from __future__ import annotations

import math
import random
from operator import attrgetter

import pytest

from hyperscale.distributed.health import WorkerHealthManager, WorkerHealthManagerConfig
from hyperscale.distributed.health.alpha_posterior import HierarchicalAlphaTuner, HierarchicalAlphaTunerConfig
from hyperscale.distributed.health.applied_outcome_window import AppliedOutcomeWindow
from hyperscale.distributed.health.extension_decision import (
    ExtensionDecision,
    ExtensionDecisionConfig,
    ExtensionDenialCode,
)
from hyperscale.distributed.health.extension_outcome import ExtensionOutcomeEvent, ExtensionOutcomeKind
from hyperscale.distributed.health.progress_witness import ThroughputWitness
from hyperscale.distributed.health.workflow_progress_snapshot import WorkflowProgressSnapshot
from hyperscale.distributed.models import HealthcheckExtensionRequest

SEEDS = range(12)
FAILING_CLASS = "CheckoutFlowThatTimesOut"
COMPLETING_CLASS = "HomepageThatCompletes"
FAILURE_KINDS = (ExtensionOutcomeKind.FAILED, ExtensionOutcomeKind.TIMED_OUT, ExtensionOutcomeKind.EVICTED)
H6_FLOOR = ThroughputWitness().budget.config.alpha_workflow_floor
H6_CEILING = ThroughputWitness().budget.config.alpha_workflow_ceiling


def outcome_event(
    workflow_id: str,
    workflow_class: str,
    outcome_kind: ExtensionOutcomeKind,
    final_progress_fraction: float,
    completed_at: float,
) -> ExtensionOutcomeEvent:
    return ExtensionOutcomeEvent(
        job_id="job-outcomes",
        workflow_id=workflow_id,
        workflow_class=workflow_class,
        worker_id="worker-outcomes",
        outcome_kind=outcome_kind,
        granted_extension_count=0,
        denied_extension_count=0,
        total_extended_seconds=0.0,
        final_progress_fraction=final_progress_fraction,
        completed_at=completed_at,
        fence_token=1,
        leader_term=1,
    )


def random_outcome(draw: random.Random, workflow_id: str, workflow_class: str, completed_at: float) -> ExtensionOutcomeEvent:
    if draw.random() < 0.5:
        return outcome_event(workflow_id, workflow_class, ExtensionOutcomeKind.COMPLETED, 1.0, completed_at)
    return outcome_event(workflow_id, workflow_class, draw.choice(FAILURE_KINDS), draw.random(), completed_at)


def pooled_evidence(tuner: HierarchicalAlphaTuner) -> tuple[float, float]:
    posteriors = list(tuner)
    success = math.fsum(map(attrgetter("alpha"), posteriors)) - tuner.config.alpha_prior * len(posteriors)
    failure = math.fsum(map(attrgetter("beta"), posteriors)) - tuner.config.beta_prior * len(posteriors)
    return success, failure


# ============================================================================
# The tuner's α budget
# ============================================================================


@pytest.mark.parametrize("seed", SEEDS)
def test_alpha_budget_invariants_under_random_outcomes_eviction_and_restore(seed: int) -> None:
    """Pooled evidence always equals the sum over held classes (through
    eviction and snapshot restore), every budget lies in [floor, ceiling],
    an unseen class keeps the H6 α, and a restored tuner budgets exactly
    as the one it was snapshotted from."""
    draw = random.Random(seed)
    tuner = HierarchicalAlphaTuner(HierarchicalAlphaTunerConfig(max_classes=6, stale_after_seconds=50.0))
    classes = [f"WorkflowClass{index}" for index in range(10)]
    clock = 0.0

    for step in range(600):
        clock += draw.expovariate(1.0)
        tuner.apply_outcome(random_outcome(draw, f"workflow-{step}", draw.choice(classes), clock))

        expected_success, expected_failure = pooled_evidence(tuner)
        assert math.isclose(tuner._pooled_success_evidence, expected_success, abs_tol=1e-6)
        assert math.isclose(tuner._pooled_failure_evidence, expected_failure, abs_tol=1e-6)
        assert len(tuner) <= tuner.config.max_classes or all(
            clock - posterior.last_outcome_at < tuner.config.stale_after_seconds for posterior in tuner
        )

        alpha_workflow = draw.uniform(0.0, 2.0 * H6_CEILING)
        for workflow_class in classes:
            budget = tuner.alpha_budget(workflow_class, alpha_workflow, H6_FLOOR, H6_CEILING)
            assert H6_FLOOR <= budget <= H6_CEILING
        assert tuner.alpha_budget("NeverSeenClass", alpha_workflow, H6_FLOOR, H6_CEILING) == min(
            max(alpha_workflow, H6_FLOOR), H6_CEILING
        )

        if step % 97 == 0:
            restored = HierarchicalAlphaTuner(tuner.config)
            restored.restore(tuner.snapshot())
            for workflow_class in classes:
                assert math.isclose(
                    restored.alpha_budget(workflow_class, alpha_workflow, H6_FLOOR, H6_CEILING),
                    tuner.alpha_budget(workflow_class, alpha_workflow, H6_FLOOR, H6_CEILING),
                    rel_tol=1e-9,
                )


@pytest.mark.parametrize("seed", SEEDS)
def test_more_failures_at_equal_count_earn_more_alpha(seed: int) -> None:
    """Two classes with the same number of outcomes: the one with more
    failures among them never gets the smaller α (strictly larger while
    neither is clamped)."""
    draw = random.Random(seed)
    tuner = HierarchicalAlphaTuner()
    outcome_count = draw.randint(1, 40)
    worse_failures = draw.randint(1, outcome_count)
    better_failures = draw.randint(0, worse_failures - 1)
    for workflow_class, failures in (("WorseClass", worse_failures), ("BetterClass", better_failures)):
        kinds = [draw.choice(FAILURE_KINDS)] * failures + [ExtensionOutcomeKind.COMPLETED] * (outcome_count - failures)
        draw.shuffle(kinds)
        for index, kind in enumerate(kinds):
            progress = 1.0 if kind is ExtensionOutcomeKind.COMPLETED else draw.random()
            tuner.apply_outcome(outcome_event(f"{workflow_class}-{index}", workflow_class, kind, progress, float(index)))

    unclamped_floor, unclamped_ceiling = 0.0, math.inf
    alpha_workflow = draw.uniform(H6_FLOOR, H6_CEILING)
    worse = tuner.alpha_budget("WorseClass", alpha_workflow, unclamped_floor, unclamped_ceiling)
    better = tuner.alpha_budget("BetterClass", alpha_workflow, unclamped_floor, unclamped_ceiling)
    assert worse > better
    assert tuner.alpha_budget("WorseClass", alpha_workflow, H6_FLOOR, H6_CEILING) >= tuner.alpha_budget(
        "BetterClass", alpha_workflow, H6_FLOOR, H6_CEILING
    )


@pytest.mark.parametrize("seed", SEEDS)
def test_weights_redistribute_the_budget_without_inflating_it(seed: int) -> None:
    """Averaged over classes by their evidence, the weights are 1 up to
    the two priors' pseudo-counts: with every class holding ``m_min`` or
    more evidence, the average lies in
    [m_min/(m_min+1) · B/(B+1/2), 1 + (N+1)/M] (N classes, M total
    evidence, B failure evidence) -- derived from the weight formula."""
    draw = random.Random(seed)
    tuner = HierarchicalAlphaTuner()
    class_count = draw.randint(2, 8)
    for class_index in range(class_count):
        failure_probability = draw.random()
        for index in range(draw.randint(200, 400)):
            event = (
                outcome_event(f"{class_index}-{index}", f"Class{class_index}", draw.choice(FAILURE_KINDS), draw.random(), 1.0)
                if draw.random() < failure_probability
                else outcome_event(f"{class_index}-{index}", f"Class{class_index}", ExtensionOutcomeKind.COMPLETED, 1.0, 1.0)
            )
            tuner.apply_outcome(event)

    unit_alpha = 1.0
    evidence_by_class = {
        posterior.workflow_class: (posterior.alpha - tuner.config.alpha_prior) + (posterior.beta - tuner.config.beta_prior)
        for posterior in tuner
    }
    total_evidence = math.fsum(evidence_by_class.values())
    weighted_average = math.fsum(
        evidence * tuner.alpha_budget(workflow_class, unit_alpha, 0.0, math.inf)
        for workflow_class, evidence in evidence_by_class.items()
    ) / total_evidence
    _, failure_evidence = pooled_evidence(tuner)
    smallest_evidence = min(evidence_by_class.values())
    lower_bound = smallest_evidence / (smallest_evidence + 1.0) * failure_evidence / (failure_evidence + 0.5)
    upper_bound = 1.0 + (class_count + 1.0) / total_evidence
    assert lower_bound <= weighted_average <= upper_bound
    assert upper_bound - lower_bound < 0.02


def test_a_new_class_at_the_cap_is_kept_not_evicted_as_the_stalest() -> None:
    """Regression: the cap check ran before the new class's first outcome
    stamped ``last_outcome_at``, so at 0.0 it was always the stalest and
    evicted itself."""
    tuner = HierarchicalAlphaTuner(HierarchicalAlphaTunerConfig(max_classes=2, stale_after_seconds=10.0))
    tuner.apply_outcome(outcome_event("a", "OldClass", ExtensionOutcomeKind.COMPLETED, 1.0, 100.0))
    tuner.apply_outcome(outcome_event("b", "MiddleClass", ExtensionOutcomeKind.COMPLETED, 1.0, 105.0))
    tuner.apply_outcome(outcome_event("c", "NewClass", ExtensionOutcomeKind.COMPLETED, 1.0, 1000.0))

    assert tuner.get("NewClass") is not None
    assert tuner.get("OldClass") is None
    assert len(tuner) == 2


# ============================================================================
# One observation per workflow, however many gossip copies arrive
# ============================================================================


@pytest.mark.parametrize("seed", SEEDS)
def test_applied_outcome_window_admits_each_id_once_per_retention(seed: int) -> None:
    draw = random.Random(seed)
    retention_seconds = draw.uniform(1.0, 20.0)
    window = AppliedOutcomeWindow(retention_seconds)
    expiry_by_workflow_id: dict[str, float] = {}
    now = 0.0
    for _ in range(2000):
        now += draw.expovariate(2.0)
        workflow_id = f"workflow-{draw.randrange(30)}"
        held = expiry_by_workflow_id.get(workflow_id, -math.inf) > now
        assert window.admit(workflow_id, now) is (not held)
        if not held:
            expiry_by_workflow_id[workflow_id] = now + retention_seconds
        live_count = sum(1 for expiry in expiry_by_workflow_id.values() if expiry > now)
        assert len(window) == live_count


@pytest.mark.parametrize("seed", SEEDS)
def test_every_manager_counts_each_outcome_once_under_duplicated_gossip(seed: int) -> None:
    """The leader records each outcome, then every manager (the leader
    too: its own echo) receives a random number of copies in random
    order. Each tuner must hold exactly one observation per workflow, and
    all managers the same posterior."""
    draw = random.Random(seed)
    leader = WorkerHealthManager()
    followers = [WorkerHealthManager() for _ in range(3)]
    events = []
    for index in range(draw.randint(5, 40)):
        source = random_outcome(draw, f"workflow-{index}", draw.choice((FAILING_CLASS, COMPLETING_CLASS)), float(index))
        events.append(
            leader.record_workflow_outcome(
                job_id=source.job_id,
                workflow_id=source.workflow_id,
                workflow_class=source.workflow_class,
                worker_id=source.worker_id,
                outcome_kind=source.outcome_kind,
                final_progress_fraction=source.final_progress_fraction,
                completed_at=source.completed_at,
                fence_token=source.fence_token,
                leader_term=source.leader_term,
            )
        )

    deliveries = [
        (manager, event) for manager in [leader, *followers] for event in events for _ in range(draw.randint(1, 6))
    ]
    draw.shuffle(deliveries)
    first_copies: dict[tuple[int, str], int] = {}
    for manager, event in deliveries:
        if manager.ingest_remote_outcome_event(event):
            key = (id(manager), event.workflow_id)
            first_copies[key] = first_copies.get(key, 0) + 1

    assert all(count == 1 for count in first_copies.values())
    assert len([key for key in first_copies if key[0] == id(leader)]) == 0
    for workflow_class in (FAILING_CLASS, COMPLETING_CLASS):
        leader_posterior = leader.alpha_tuner.get(workflow_class)
        expected_seen = sum(1 for event in events if event.workflow_class == workflow_class)
        if leader_posterior is None:
            assert expected_seen == 0
            continue
        assert leader_posterior.total_seen == expected_seen
        for follower in followers:
            follower_posterior = follower.alpha_tuner.get(workflow_class)
            assert follower_posterior.total_seen == expected_seen
            assert math.isclose(follower_posterior.alpha, leader_posterior.alpha, rel_tol=1e-12)
            assert math.isclose(follower_posterior.beta, leader_posterior.beta, rel_tol=1e-12)


# ============================================================================
# The loop is closed: learned outcomes change the extension decision
# ============================================================================


def build_health_manager() -> WorkerHealthManager:
    """A manager-side health manager with the production witness and α
    defaults; only the extension cap and spacing are lifted so every
    request reaches the throughput witness."""
    return WorkerHealthManager(
        config=WorkerHealthManagerConfig(max_extensions=100_000),
        throughput_witness=ThroughputWitness(),
        decision_config=ExtensionDecisionConfig(min_between_extensions_seconds=0.0),
    )


def teach_outcomes(health_manager: WorkerHealthManager, failing_count: int, completing_count: int) -> None:
    for index in range(failing_count):
        health_manager.record_workflow_outcome(
            job_id="job-history",
            workflow_id=f"history-failing-{index}",
            workflow_class=FAILING_CLASS,
            worker_id="worker-history",
            outcome_kind=ExtensionOutcomeKind.TIMED_OUT,
            final_progress_fraction=0.0,
            completed_at=float(index),
            fence_token=index,
            leader_term=1,
        )
    for index in range(completing_count):
        health_manager.record_workflow_outcome(
            job_id="job-history",
            workflow_id=f"history-completing-{index}",
            workflow_class=COMPLETING_CLASS,
            worker_id="worker-history",
            outcome_kind=ExtensionOutcomeKind.COMPLETED,
            final_progress_fraction=1.0,
            completed_at=float(index),
            fence_token=index,
            leader_term=1,
        )


def throughput_trace(draw: random.Random) -> list[float]:
    """A stationary throughput regime, then a sustained drop."""
    baseline = draw.uniform(50.0, 500.0)
    noise = baseline * draw.uniform(0.001, 0.02)
    dropped = baseline * draw.uniform(0.05, 0.3)
    before = [draw.gauss(baseline, noise) for _ in range(draw.randint(30, 60))]
    after = [draw.gauss(dropped, noise) for _ in range(20)]
    return before + after


def run_extension_requests(
    health_manager: WorkerHealthManager, workflow_class: str, trace: list[float]
) -> list[ExtensionDecision]:
    """One progress-rate sample (the manager's progress-path feed) and one
    extension request per trace entry, with progress advancing on every
    request so only the throughput witness can deny."""
    decisions: list[ExtensionDecision] = []
    for index, throughput in enumerate(trace, start=1):
        health_manager.throughput_witness.ingest("worker-1", "workflow-under-test", throughput)
        request = HealthcheckExtensionRequest(
            worker_id="worker-1",
            reason="autonomous-trigger",
            current_progress=0.5,
            completed_items=index,
            total_items=len(trace) + 1,
            estimated_completion=10.0,
            active_workflow_count=1,
            workflow_id="workflow-under-test",
            step_transitions=index,
            actions_completed=index * 10,
            snapshot_time=float(index),
        )
        snapshot = WorkflowProgressSnapshot(
            workflow_id="workflow-under-test",
            cores_completed=index,
            cores_total=len(trace) + 1,
            step_transitions=index,
            actions_completed=index * 10,
            snapshot_time=float(index),
        )
        _, decision, _ = health_manager.handle_extension_request_with_witnesses(
            request=request,
            current_deadline=1_000.0,
            snapshot=snapshot,
            last_snapshot=health_manager.ledger.latest_progress_snapshot("workflow-under-test"),
            throughput=throughput,
            overload_state="healthy",
            active_in_cluster=1,
            active_in_dc=1,
            active_on_manager=1,
            active_on_worker=1,
            workflow_class=workflow_class,
            job_id="job-under-test",
            fence_token=1,
            leader_term=1,
        )
        decisions.append(decision)
    return decisions


def throughput_denied(decisions: list[ExtensionDecision]) -> list[bool]:
    assert all(
        decision.granted or decision.denial_reason_code is ExtensionDenialCode.THROUGHPUT_REGIME_DOWN
        for decision in decisions
    ), "only the throughput witness may deny in this harness"
    return [not decision.granted for decision in decisions]


def first_denial(denied: list[bool]) -> int:
    return denied.index(True) if True in denied else len(denied)


@pytest.mark.parametrize("seed", SEEDS)
def test_learned_failures_deny_a_throughput_drop_sooner_than_learned_completions(seed: int) -> None:
    """Identical throughput traces, identical H6 α: the workflow of the
    class with a failure history is denied on every request the workflow
    of the class with a completion history is, and the failing class's
    test runs at the larger α. Before any outcome both are identical."""
    draw = random.Random(seed)
    trace = throughput_trace(draw)
    failing_count = draw.randint(5, 60)
    completing_count = draw.randint(200, 2000)

    taught = {}
    for workflow_class in (FAILING_CLASS, COMPLETING_CLASS):
        health_manager = build_health_manager()
        teach_outcomes(health_manager, failing_count, completing_count)
        taught[workflow_class] = run_extension_requests(health_manager, workflow_class, trace)

    failing_denied = throughput_denied(taught[FAILING_CLASS])
    completing_denied = throughput_denied(taught[COMPLETING_CLASS])
    assert all(failing or not completing for failing, completing in zip(failing_denied, completing_denied))
    assert True in failing_denied, "the failing class must be denied on a sustained drop"
    # Strictly sooner: the failing class's α sits at the H6 ceiling (0.05)
    # and the completing class's at most α_H6/(c+1) <= 5e-5, below the
    # K-S p-value (~6e-5 to 8e-5 for 30-60 pre-drop samples) at the first
    # split the N_e >= 4 guard admits.
    assert first_denial(failing_denied) < first_denial(completing_denied)
    assert all(
        failing.evidence.throughput_alpha_workflow > completing.evidence.throughput_alpha_workflow
        for failing, completing in zip(taught[FAILING_CLASS], taught[COMPLETING_CLASS])
        if failing.evidence.throughput_alpha_workflow > 0.0
    )

    untaught = [
        throughput_denied(run_extension_requests(build_health_manager(), workflow_class, trace))
        for workflow_class in (FAILING_CLASS, COMPLETING_CLASS)
    ]
    assert untaught[0] == untaught[1]


def test_learned_outcomes_flip_a_decision() -> None:
    """The decisive run: the same request, the same throughput history --
    denied for the class that times out, granted for the class that
    completes, because the learned outcomes moved α from the H6 ceiling to
    its floor. Without any outcomes the two decisions are identical."""
    trace = [100.0 + 0.1 * (index % 3) for index in range(40)] + [10.0 + 0.1 * (index % 3) for index in range(20)]

    def denials(workflow_class: str, taught: bool) -> list[bool]:
        health_manager = build_health_manager()
        if taught:
            teach_outcomes(health_manager, failing_count=40, completing_count=1000)
        return throughput_denied(run_extension_requests(health_manager, workflow_class, trace))

    failing_first = first_denial(denials(FAILING_CLASS, taught=True))
    completing_first = first_denial(denials(COMPLETING_CLASS, taught=True))
    assert failing_first < len(trace)
    assert failing_first < completing_first, (failing_first, completing_first)

    assert first_denial(denials(FAILING_CLASS, taught=False)) == first_denial(denials(COMPLETING_CLASS, taught=False))
