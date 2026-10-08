"""Workflow stats merged across cores, workers and datacenters equal the stats of the union.

Each seed runs a workflow's steps on several sources: a TEST step whose
responses go through ``Results.aggregate_result`` (the per-result path a
worker runs), a CHECK step and COUNT / DISTRIBUTION / SAMPLE metric steps.
Each source's stats are processed alone, then merged with
``merge_results`` in a random hierarchy (groups of sources merged, their
merges merged, and so on). The result must equal processing every source's
results as one: same timings, digests, counts, statuses, contexts, checks,
metrics, the longest elapsed, and the actions per second over it.
"""

import random
from fractions import Fraction

import pytest

from hyperscale.core.engines.client.shared.models import RequestType
from hyperscale.core.hooks import HookType
from hyperscale.reporting.common.results_types import WorkflowStats
from hyperscale.reporting.models.metric import COUNT, DISTRIBUTION, RATE, SAMPLE
from hyperscale.reporting.results import Results
from hyperscale.reporting.time_aligned_results import TimeAlignedResults
from hyperscale.reporting.timestamped_stats import TimestampedStats

SEEDS = range(30)
WORKFLOW = "MergeWorkflow"


class StepHook:
    """The hook attributes ``Results`` reads."""

    def __init__(
        self,
        hook_type: HookType,
        engine_type: RequestType | None = None,
        metric_type: object = None,
    ) -> None:
        self.hook_type = hook_type
        self.engine_type = engine_type
        self.metric_type = metric_type
        self.tags = ["merge"]


class RecordedResponse:
    """An HTTP response's fields that results are aggregated from."""

    def __init__(self, timings: dict[str, float], successful: bool, status: int, context: str | None) -> None:
        self.timings = timings
        self.successful = successful
        self.status = status
        self._context = context

    def context(self) -> str | None:
        return self._context


HOOKS = {
    "request": StepHook(HookType.TEST, RequestType.HTTP),
    "request_again": StepHook(HookType.TEST, RequestType.HTTP),
    "verify": StepHook(HookType.CHECK),
    "bytes_sent": StepHook(HookType.METRIC, metric_type=COUNT),
    "payload_size": StepHook(HookType.METRIC, metric_type=DISTRIBUTION),
    "queue_depth": StepHook(HookType.METRIC, metric_type=SAMPLE),
}


def draw_response(generator: random.Random) -> RecordedResponse | Exception:
    if generator.random() < 0.05:
        return ConnectionError(generator.choice(["reset", "refused"]))

    start = generator.uniform(0.0, 1e4)
    connect = generator.lognormvariate(-7.0, 1.0)
    write = generator.lognormvariate(-9.0, 1.0)
    read = generator.lognormvariate(-5.0, 1.5)
    timings = {
        "request_start": start,
        "connect_start": start,
        "connect_end": start + connect,
        "write_start": start + connect,
        "write_end": start + connect + write,
        "read_start": start + connect + write,
        "read_end": start + connect + write + read,
        "request_end": start + connect + write + read,
    }
    status = generator.choice([200, 200, 200, 404, 500])
    return RecordedResponse(timings, status < 400, status, generator.choice([None, None, "slow"]))


def draw_source_results(generator: random.Random) -> dict[str, list[object]]:
    results_count = generator.randrange(1, 120)
    return {
        "request": [draw_response(generator) for _ in range(results_count)],
        "request_again": [draw_response(generator) for _ in range(results_count)],
        "verify": [generator.choice([None, None, AssertionError("bad body")]) for _ in range(results_count)],
        "bytes_sent": [generator.randrange(2**62, 2**64) for _ in range(generator.randrange(1, 20))],
        "payload_size": [generator.randrange(0, 2**20) for _ in range(generator.randrange(1, 20))],
        "queue_depth": [generator.uniform(-5.0, 5.0) for _ in range(generator.randrange(1, 20))],
    }


def process(results_by_step: dict[str, list[object]], elapsed: float) -> WorkflowStats:
    results = Results(HOOKS)
    aggregates = results.create_aggregates(HOOKS)
    for step, step_results in results_by_step.items():
        for result in step_results:
            results.aggregate_result(aggregates, step, result)
    return results.process_aggregates(WORKFLOW, aggregates, elapsed)


def merge_in_random_hierarchy(generator: random.Random, workflow_stats: list[WorkflowStats]) -> WorkflowStats:
    """Merge random groups of the pending stats until one is left (cores, workers, datacenters)."""
    pending = workflow_stats[:]
    generator.shuffle(pending)
    while len(pending) > 1:
        group_size = generator.randrange(2, len(pending) + 1)
        group = [pending.pop(generator.randrange(len(pending))) for _ in range(group_size)]
        pending.insert(generator.randrange(len(pending) + 1), Results().merge_results(group))
    return pending[0]


def comparable(workflow_stats: WorkflowStats) -> dict[str, object]:
    """The stats with every list keyed by step and contexts as counts, so order does not matter."""
    return {
        "workflow": workflow_stats["workflow"],
        "stats": workflow_stats["stats"],
        "elapsed": workflow_stats["elapsed"],
        "aps": workflow_stats["aps"],
        "results": {
            result_set["step"]: {
                "timings": result_set["timings"],
                "digests": result_set["digests"],
                "counts": result_set["counts"],
                "contexts": context_counts(result_set["contexts"]),
            }
            for result_set in workflow_stats["results"]
        },
        "checks": {
            check_set["step"]: (check_set["counts"], context_counts(check_set["contexts"]))
            for check_set in workflow_stats["checks"]
        },
        "metrics": {
            metric_set["step"]: (metric_set["metric_type"], metric_set["stats"], metric_set["digest"])
            for metric_set in workflow_stats["metrics"]
        },
    }


def context_counts(contexts: list[dict[str, str | int]]) -> dict[str, int]:
    return {context["context"]: context["count"] for context in contexts}


@pytest.mark.parametrize("seed", SEEDS)
def test_merging_in_any_hierarchy_equals_processing_the_union(seed: int) -> None:
    generator = random.Random(seed)
    sources = [draw_source_results(generator) for _ in range(generator.randrange(1, 9))]
    elapsed_times = [generator.uniform(1.0, 120.0) for _ in sources]

    merged = merge_in_random_hierarchy(
        generator,
        [process(source, elapsed) for source, elapsed in zip(sources, elapsed_times)],
    )
    union = process(
        {step: [result for source in sources for result in source[step]] for step in HOOKS},
        max(elapsed_times),
    )

    assert comparable(merged) == comparable(union)


@pytest.mark.parametrize("seed", SEEDS)
def test_workflow_counts_sum_every_test_step(seed: int) -> None:
    """executed = succeeded + failed over every TEST step, and the rate counts them all."""
    generator = random.Random(seed)
    elapsed = generator.uniform(1.0, 60.0)
    workflow_stats = process(draw_source_results(generator), elapsed)

    step_counts = [result_set["counts"] for result_set in workflow_stats["results"]]
    executed = sum(counts["executed"] for counts in step_counts)

    assert workflow_stats["stats"] == {
        "executed": executed,
        "succeeded": sum(counts["succeeded"] for counts in step_counts),
        "failed": sum(counts["failed"] for counts in step_counts),
    }
    assert workflow_stats["stats"]["executed"] == (
        workflow_stats["stats"]["succeeded"] + workflow_stats["stats"]["failed"]
    )
    assert workflow_stats["aps"] == float(Fraction(executed) / Fraction(elapsed))


def test_counters_past_int64_merge_exactly() -> None:
    """
    Counts past 2**63 add exactly, and the rate over them is rounded once:
    for this count, converting it to a float before dividing rounds twice
    and lands one ulp off.
    """
    huge_count = 2**64 + 171
    sources = [
        {
            "workflow": WORKFLOW,
            "elapsed": elapsed,
            "stats": {"executed": huge_count, "succeeded": huge_count - 1, "failed": 1},
            "results": [],
            "checks": [],
            "metrics": [],
        }
        for elapsed in (3.0, 7.0, 5.0)
    ]

    merged = Results().merge_results(sources)

    assert merged["stats"] == {
        "executed": 3 * huge_count,
        "succeeded": 3 * (huge_count - 1),
        "failed": 3,
    }
    assert merged["elapsed"] == 7.0
    assert merged["aps"] == float(Fraction(3 * huge_count, 7))
    assert merged["aps"] != 3 * huge_count / 7.0


def test_count_metrics_past_int64_stay_exact_ints() -> None:
    generator = random.Random(11)
    sources = [draw_source_results(generator) for _ in range(4)]

    merged = Results().merge_results([process(source, 10.0) for source in sources])

    (count_metric,) = [metric for metric in merged["metrics"] if metric["step"] == "bytes_sent"]
    assert count_metric["stats"]["count"] == sum(
        value for source in sources for value in source["bytes_sent"]
    )
    assert isinstance(count_metric["stats"]["count"], int)


@pytest.mark.parametrize("seed", range(10))
def test_rate_metrics_add_across_sources_in_any_order(seed: int) -> None:
    """Concurrent sources' rates add: the merged rate is the exact sum of each source's, rounded once."""
    generator = random.Random(seed)
    hooks = {"throughput": StepHook(HookType.METRIC, metric_type=RATE)}
    source_rates: list[Fraction] = []
    workflow_stats: list[WorkflowStats] = []

    for _ in range(generator.randrange(2, 8)):
        samples = [
            (generator.uniform(0.0, 1e3), generator.uniform(0.0, 1e4))
            for _ in range(generator.randrange(2, 30))
        ]
        timestamps = [timestamp for _, timestamp in samples]
        exact_rate = sum(Fraction(value) for value, _ in samples) / (
            Fraction(max(timestamps)) - Fraction(min(timestamps))
        )
        source_rates.append(Fraction(float(exact_rate)))

        results = Results(hooks)
        workflow_stats.append(
            results.process_aggregates(WORKFLOW, {"throughput": samples}, 1.0)
        )

    for _ in range(5):
        merged = merge_in_random_hierarchy(generator, workflow_stats)
        assert merged["metrics"][0]["stats"]["rate"] == float(sum(source_rates))


def test_a_check_step_after_the_test_step_keeps_the_rate() -> None:
    """The rate counts the TEST steps whatever step the workflow ends with."""
    generator = random.Random(3)
    hooks = {
        "request": StepHook(HookType.TEST, RequestType.HTTP),
        "verify": StepHook(HookType.CHECK),
    }
    results = Results(hooks)
    aggregates = results.create_aggregates(hooks)
    for _ in range(10):
        results.aggregate_result(aggregates, "request", draw_response(generator))
        results.aggregate_result(aggregates, "verify", None)

    workflow_stats = results.process_aggregates(WORKFLOW, aggregates, 2.0)

    assert workflow_stats["stats"]["executed"] == 10
    assert workflow_stats["aps"] == 5.0


def test_merged_metrics_are_returned() -> None:
    generator = random.Random(5)
    sources = [draw_source_results(generator) for _ in range(2)]

    merged = Results().merge_results([process(source, 1.0) for source in sources])

    assert {metric["step"] for metric in merged["metrics"]} == {"bytes_sent", "payload_size", "queue_depth"}


def test_a_source_without_digests_is_refused_loudly() -> None:
    generator = random.Random(9)
    workflow_stats = process(draw_source_results(generator), 1.0)
    del workflow_stats["results"][0]["digests"]

    with pytest.raises(ValueError, match="digests"):
        Results().merge_results([workflow_stats, process(draw_source_results(generator), 1.0)])


def test_scp_transfer_timing_is_reported_without_touching_the_response() -> None:
    timings = {
        "request_start": 1.0,
        "request_end": 4.0,
        "connect_start": 1.0,
        "connect_end": 1.5,
        "initialization_start": 1.5,
        "initialization_end": 2.0,
        "transfer_start": 2.0,
        "transfer_end": 4.0,
    }
    response = RecordedResponse(dict(timings), True, 0, None)

    scp_timings = Results()._process_scp_timings(response)

    assert scp_timings == {"total": 3.0, "connecting": 0.5, "initializing": 0.5, "transferring": 2.0}
    assert response.timings == timings


@pytest.mark.parametrize("seed", range(10))
def test_time_aligned_merge_does_not_depend_on_collection_times(seed: int) -> None:
    """When a source's stats were collected changes neither its samples nor its elapsed time."""
    generator = random.Random(seed)
    workflow_stats = [process(draw_source_results(generator), generator.uniform(1.0, 30.0)) for _ in range(3)]

    merged, metadata = TimeAlignedResults().merge_with_time_alignment(
        [
            TimestampedStats(stats=stats, collected_at=generator.uniform(0.0, 100.0), source=f"worker-{index}")
            for index, stats in enumerate(workflow_stats)
        ]
    )

    assert comparable(merged) == comparable(Results().merge_results(workflow_stats))
    assert metadata.sources_count == 3


@pytest.mark.parametrize("seed", range(10))
def test_time_aligned_progress_rates_add_in_any_order(seed: int) -> None:
    generator = random.Random(seed)
    updates = [
        {
            "collected_at": generator.uniform(0.0, 10.0),
            "completed_count": generator.randrange(0, 2**64),
            "failed_count": generator.randrange(0, 100),
            "rate_per_second": generator.uniform(0.0, 1e5),
            "elapsed_seconds": generator.uniform(0.0, 60.0),
        }
        for _ in range(generator.randrange(1, 20))
    ]
    exact_rate = float(sum(Fraction(update["rate_per_second"]) for update in updates))

    for _ in range(5):
        generator.shuffle(updates)
        aggregated = TimeAlignedResults().aggregate_progress_stats(updates, reference_time=10.0)
        assert aggregated["rate_per_second"] == exact_rate
        assert aggregated["completed_count"] == sum(update["completed_count"] for update in updates)
