"""
AD-44 late-result policy decisions and observability, below the SIM pins
(``tests/unit/simulation/sim/test_multiprocess_best_effort_late_results.py``):

* the ``update`` policy releases a best-effort job provisionally exactly
  once, on ``min_dcs`` before its deadline, then completes it for good
  when every datacenter reported -- or at its deadline (the deadline
  check); the ``log`` policy never releases provisionally;
* a released job's undecided result keeps the reason it was released for;
* a refused retry logs ``RetryBudgetExhausted`` (job or workflow scope)
  and counts per job, held while the job's budget is;
* ``BestEffortMetrics`` counts a job once and forgets its ratio with it;
* ``hyperscale cluster --metrics`` prints the AD-44 metrics;
* the client never lets a provisional result overwrite a final or a
  fuller one (pushes can cross);
* ``BEST_EFFORT_LATE_RESULT_POLICY`` is a real Env field, ``log`` by
  default, refusing any other value.
"""

from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from hyperscale.commands.cluster import _prometheus_text
from hyperscale.distributed.cluster.models import ClusterMetricsReply
from hyperscale.distributed.env import Env
from hyperscale.distributed.models import ClientJobResult, GlobalJobResult, JobFinalResult
from hyperscale.distributed.nodes.client.handlers.global_job_result_handler import GlobalJobResultHandler
from hyperscale.distributed.reliability.best_effort_manager import BestEffortManager
from hyperscale.distributed.reliability.best_effort_metrics import BestEffortMetrics
from hyperscale.distributed.reliability.late_result_policy import LateResultPolicy
from hyperscale.distributed.reliability.reliability_config import create_reliability_config_from_env
from hyperscale.distributed.reliability.retry_budget_manager import RetryBudgetManager
from hyperscale.logging.hyperscale_logging_models import RetryBudgetExhausted

JOB_ID = "job-1"
DEADLINE_SECONDS = 30.0


class SteppedClock:
    """A monotonic clock the test moves by hand."""

    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now


class RecordingLogger:
    def __init__(self) -> None:
        self.entries: list = []

    async def log(self, entry) -> None:
        self.entries.append(entry)


def make_best_effort_manager(policy: str, clock: SteppedClock) -> BestEffortManager:
    async def unused_completion_handler(job_id, decision) -> None:
        raise AssertionError("the deadline loop is not started here")

    return BestEffortManager(
        task_runner=SimpleNamespace(),
        config=create_reliability_config_from_env(Env(BEST_EFFORT_LATE_RESULT_POLICY=policy)),
        clock=clock,
        completion_handler=unused_completion_handler,
    )


@pytest.mark.asyncio
async def test_update_policy_releases_once_then_completes_when_every_datacenter_reported() -> None:
    clock = SteppedClock()
    manager = make_best_effort_manager("update", clock)
    await manager.create_state(JOB_ID, min_dcs=1, deadline_seconds=DEADLINE_SECONDS, target_dcs={"dc-a", "dc-b"})

    released = await manager.record_result(JOB_ID, "dc-a", True)
    assert (released.should_complete, released.provisional) == (True, True)
    assert released.reason == "min_dcs_reached (1/1)"
    assert manager.is_released(JOB_ID)
    # Released: min_dcs is not judged again; the standing result keeps its reason.
    assert await manager.check_all_completions() == []
    assert manager.completion_ratio(JOB_ID) == 0.5

    final = await manager.record_result(JOB_ID, "dc-b", True)
    assert (final.should_complete, final.provisional, final.reason) == (True, False, "all_dcs_reported")


@pytest.mark.asyncio
async def test_a_released_jobs_undecided_result_keeps_its_release_reason() -> None:
    clock = SteppedClock()
    manager = make_best_effort_manager("update", clock)
    await manager.create_state(
        JOB_ID, min_dcs=1, deadline_seconds=DEADLINE_SECONDS, target_dcs={"dc-a", "dc-b", "dc-c"}
    )
    await manager.record_result(JOB_ID, "dc-a", True)

    waiting = await manager.record_result(JOB_ID, "dc-b", True)

    assert (waiting.should_complete, waiting.reason, waiting.success) == (False, "min_dcs_reached (1/1)", True)


@pytest.mark.asyncio
async def test_update_policy_closes_a_released_job_at_its_deadline() -> None:
    clock = SteppedClock()
    manager = make_best_effort_manager("update", clock)
    await manager.create_state(JOB_ID, min_dcs=1, deadline_seconds=DEADLINE_SECONDS, target_dcs={"dc-a", "dc-b"})
    await manager.record_result(JOB_ID, "dc-a", True)

    clock.now = DEADLINE_SECONDS
    ((job_id, decision),) = await manager.check_all_completions()

    assert job_id == JOB_ID
    assert (decision.should_complete, decision.provisional, decision.success) == (True, False, True)
    assert decision.reason == "deadline_expired (completed: 1)"


@pytest.mark.asyncio
async def test_update_policy_never_releases_once_the_deadline_passed() -> None:
    clock = SteppedClock()
    manager = make_best_effort_manager("update", clock)
    await manager.create_state(JOB_ID, min_dcs=1, deadline_seconds=DEADLINE_SECONDS, target_dcs={"dc-a", "dc-b"})
    clock.now = DEADLINE_SECONDS

    decision = await manager.record_result(JOB_ID, "dc-a", True)

    assert (decision.should_complete, decision.provisional) == (True, False)
    assert not manager.is_released(JOB_ID)


@pytest.mark.asyncio
async def test_log_policy_never_releases_provisionally() -> None:
    clock = SteppedClock()
    manager = make_best_effort_manager("log", clock)
    await manager.create_state(JOB_ID, min_dcs=1, deadline_seconds=DEADLINE_SECONDS, target_dcs={"dc-a", "dc-b"})

    decision = await manager.record_result(JOB_ID, "dc-a", True)

    assert (decision.should_complete, decision.provisional) == (True, False)
    assert not manager.is_released(JOB_ID)


def make_retry_budget_manager(logger: RecordingLogger) -> RetryBudgetManager:
    return RetryBudgetManager(
        config=create_reliability_config_from_env(Env()),
        logger=logger,
        node_id="manager-1",
        datacenter="dc-a",
    )


@pytest.mark.asyncio
async def test_a_spent_workflow_cap_is_logged_and_counted() -> None:
    logger = RecordingLogger()
    manager = make_retry_budget_manager(logger)
    await manager.create_budget(JOB_ID, total=10, per_workflow=2)

    outcomes = [await manager.check_and_consume(JOB_ID, "workflow-a") for _ in range(3)]

    assert [allowed for allowed, _reason in outcomes] == [True, True, False]
    (entry,) = logger.entries
    assert isinstance(entry, RetryBudgetExhausted)
    assert (entry.job_id, entry.workflow_id, entry.scope, entry.consumed, entry.budget) == (
        JOB_ID,
        "workflow-a",
        "workflow",
        2,
        2,
    )
    assert manager.consumed_by_job() == {JOB_ID: 2}
    assert manager.exhausted_by_job() == {JOB_ID: 1}


@pytest.mark.asyncio
async def test_a_spent_job_budget_is_logged_with_job_scope_and_released_with_the_job() -> None:
    logger = RecordingLogger()
    manager = make_retry_budget_manager(logger)
    await manager.create_budget(JOB_ID, total=2, per_workflow=2)

    for workflow_id in ("workflow-a", "workflow-b", "workflow-c"):
        await manager.check_and_consume(JOB_ID, workflow_id)

    (entry,) = logger.entries
    assert (entry.workflow_id, entry.scope, entry.consumed, entry.budget) == ("workflow-c", "job", 2, 2)

    await manager.cleanup(JOB_ID)
    assert manager.consumed_by_job() == {} and manager.exhausted_by_job() == {}


@pytest.mark.asyncio
async def test_a_job_without_a_budget_is_refused_without_an_exhaustion_log() -> None:
    logger = RecordingLogger()
    manager = make_retry_budget_manager(logger)

    assert await manager.check_and_consume(JOB_ID, "workflow-a") == (False, "retry_budget_missing")
    assert logger.entries == []


def test_best_effort_metrics_count_a_job_once_and_forget_its_ratio() -> None:
    metrics = BestEffortMetrics()
    metrics.record_completion(JOB_ID, "best_effort: min_dcs_reached (1/2)", 0.5)
    metrics.record_ratio(JOB_ID, "best_effort: all_dcs_reported", 1.0)
    metrics.record_late_result("updated")
    metrics.record_late_result("logged")
    metrics.record_late_result("logged")

    assert metrics.completions_by_reason() == {"min_dcs_reached": 1}
    assert metrics.completion_ratio_by_job() == {JOB_ID: 1.0}
    assert metrics.late_results_by_outcome() == {"logged": 2, "updated": 1}

    metrics.forget_job(JOB_ID)
    assert metrics.completion_ratio_by_job() == {}


def test_cluster_metrics_print_the_ad44_metrics() -> None:
    reply = ClusterMetricsReply(
        member_id="gate-1",
        formation="formed",
        is_leader=True,
        retry_budget_consumed={JOB_ID: 3},
        retry_budget_exhausted={JOB_ID: 1},
        best_effort_completions={"min_dcs_reached": 2},
        best_effort_completion_ratio={JOB_ID: 0.5},
        best_effort_late_results={"logged": 4},
    )

    text = _prometheus_text(reply)

    for line in (
        "# TYPE retry_budget_consumed_total counter",
        f'retry_budget_consumed_total{{member="gate-1",job_id="{JOB_ID}"}} 3',
        f'retry_budget_exhausted_total{{member="gate-1",job_id="{JOB_ID}"}} 1',
        'best_effort_completions_total{member="gate-1",reason="min_dcs_reached"} 2',
        "# TYPE best_effort_completion_ratio gauge",
        f'best_effort_completion_ratio{{member="gate-1",job_id="{JOB_ID}"}} 0.5',
        'best_effort_late_results_total{member="gate-1",outcome="logged"} 4',
    ):
        assert line in text, text
    assert "retry_budget" not in _prometheus_text(
        ClusterMetricsReply(member_id="gate-1", formation="formed", is_leader=True)
    )


def datacenter_result(datacenter: str) -> JobFinalResult:
    return JobFinalResult(job_id=JOB_ID, datacenter=datacenter, status="completed")


def make_client_handler(job: ClientJobResult) -> GlobalJobResultHandler:
    state = SimpleNamespace(_jobs={JOB_ID: job}, _job_results_events={}, _job_events={})
    return GlobalJobResultHandler(state=state, logger=RecordingLogger(), workflow_results=SimpleNamespace())


def test_a_provisional_result_never_overwrites_a_final_or_fuller_one() -> None:
    job = ClientJobResult(job_id=JOB_ID, status="running")
    handler = make_client_handler(job)
    provisional = GlobalJobResult(
        job_id=JOB_ID,
        status="completed",
        per_datacenter_results=[datacenter_result("dc-a")],
        unreported_datacenters=["dc-b"],
        is_final=False,
    )
    final = GlobalJobResult(
        job_id=JOB_ID,
        status="completed",
        per_datacenter_results=[datacenter_result("dc-a"), datacenter_result("dc-b")],
        is_final=True,
    )

    handler._apply_global_result(job, provisional)
    assert (job.is_final, job.unreported_datacenters) == (False, ["dc-b"])

    handler._apply_global_result(job, final)
    handler._apply_global_result(job, provisional)
    assert (job.is_final, job.unreported_datacenters, len(job.per_datacenter_results)) == (True, [], 2)


def test_the_late_result_policy_is_an_env_field_defaulting_to_log() -> None:
    assert Env().BEST_EFFORT_LATE_RESULT_POLICY == "log"
    assert create_reliability_config_from_env(Env()).best_effort_late_result_policy is LateResultPolicy.LOG
    assert (
        create_reliability_config_from_env(Env(BEST_EFFORT_LATE_RESULT_POLICY="update")).best_effort_late_result_policy
        is LateResultPolicy.UPDATE
    )
    with pytest.raises(ValidationError):
        Env(BEST_EFFORT_LATE_RESULT_POLICY="aggregate")


async def charge_worker_losses(manager: RetryBudgetManager, workflow_count: int, losses: int) -> tuple[int, int]:
    """Lose a worker running every live workflow of the job ``losses``
    times: (retries granted, workflows failed for good)."""
    await manager.create_budget(JOB_ID, total=0, per_workflow=0)
    live_workflows = [f"workflow-{index}" for index in range(workflow_count)]
    granted = 0
    for _ in range(losses):
        outcomes = [(workflow_id, await manager.check_and_consume(JOB_ID, workflow_id)) for workflow_id in live_workflows]
        granted += sum(1 for _workflow_id, (allowed, _reason) in outcomes if allowed)
        live_workflows = [workflow_id for workflow_id, (allowed, _reason) in outcomes if allowed]
    return granted, workflow_count - len(live_workflows)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("workflow_count", "losses", "expected"),
    [
        # Up to 10 workflows survive the loss of a worker running them all.
        (10, 1, (10, 0)),
        # Small jobs are bounded by the per-workflow cap of 3 attempts.
        (3, 3, (9, 0)),
        # From 4 workflows the job budget is the storm bound: 10 extra dispatches.
        (4, 3, (10, 2)),
        (11, 1, (10, 1)),
    ],
)
async def test_the_default_retry_budget_bounds_a_jobs_retry_storm(
    workflow_count: int, losses: int, expected: tuple[int, int]
) -> None:
    """RETRY_BUDGET_DEFAULT (see its Env comment): the default budgets'
    behaviour under repeated whole-worker losses."""
    manager = make_retry_budget_manager(RecordingLogger())

    assert await charge_worker_losses(manager, workflow_count, losses) == expected
