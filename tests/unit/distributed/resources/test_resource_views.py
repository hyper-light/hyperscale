"""
AD-41 resource views: the job leader's led workload and the gate's
per-datacenter aggregation, on a stepped clock.

Rules pinned:
- A manager counts only workflows it leads, and only their latest
  estimate; a terminal workflow, a cleaned-up job, a job taken over by
  another manager, and an estimate gone stale all stop counting and leave
  no state.
- A gate sums the managers' led workloads (a partition, so nothing is
  counted twice), takes capacity from the newest report, adds variances,
  caps pressure at 1.0, and drops a manager whose report went stale.
- Without a fresh report that knows capacity there is no view.
"""

import math

from hyperscale.distributed.resources.datacenter_resource_aggregator import (
    DatacenterResourceAggregator,
)
from hyperscale.distributed.resources.led_workflow_resources import LedWorkflowResources
from hyperscale.distributed.resources.manager_resource_report import ManagerResourceReport
from hyperscale.distributed.resources.resource_metrics import ResourceMetrics
from hyperscale.distributed.resources.workload_resource_totals import WorkloadResourceTotals

STALENESS_SECONDS = 30.0
MANAGER_A = ("10.0.0.1", 9000)
MANAGER_B = ("10.0.0.2", 9000)
CORES = 8
CPU_CAPACITY = 100.0 * CORES
MEMORY_CAPACITY = 32 * 1024**3


class SteppedClock:
    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now


def make_led(led_jobs: set[str]) -> tuple[LedWorkflowResources, SteppedClock]:
    clock = SteppedClock()
    return LedWorkflowResources(clock, STALENESS_SECONDS, leads_job=led_jobs.__contains__), clock


def record(led: LedWorkflowResources, workflow_id: str, job_id: str, cpu: float, memory: float) -> None:
    led.record(workflow_id, job_id, cpu, cpu / 10.0, memory, memory / 10.0)


def report(cpu: float, memory: float, cpu_variance: float = 0.0, capacity: float = CPU_CAPACITY) -> ManagerResourceReport:
    return ManagerResourceReport(
        manager_metrics=ResourceMetrics(
            cpu_percent=5.0,
            cpu_uncertainty=1.0,
            memory_bytes=100,
            memory_uncertainty=1.0,
            memory_percent=0.1,
            file_descriptor_count=10,
        ),
        workload=WorkloadResourceTotals(
            cpu_percent=cpu,
            cpu_variance=cpu_variance,
            memory_bytes=memory,
            memory_variance=0.0,
            workflow_count=1,
        ),
        cpu_capacity_percent=capacity,
        memory_capacity_bytes=MEMORY_CAPACITY,
    )


def test_led_totals_keep_only_each_workflows_latest_estimate() -> None:
    led, _clock = make_led({"job-1"})
    record(led, "wf-1", "job-1", cpu=100.0, memory=1_000.0)
    record(led, "wf-1", "job-1", cpu=300.0, memory=3_000.0)
    record(led, "wf-2", "job-1", cpu=50.0, memory=500.0)

    totals = led.totals()

    assert (totals.cpu_percent, totals.memory_bytes, totals.workflow_count) == (350.0, 3_500.0, 2)
    assert totals.cpu_variance == 30.0**2 + 5.0**2


def test_terminal_workflow_and_cleaned_job_leave_no_state() -> None:
    led, _clock = make_led({"job-1", "job-2"})
    record(led, "wf-1", "job-1", cpu=100.0, memory=1.0)
    record(led, "wf-2", "job-2", cpu=100.0, memory=1.0)
    record(led, "wf-3", "job-2", cpu=100.0, memory=1.0)

    led.release_workflow("wf-1")
    led.release_job("job-2")

    assert led.totals() == WorkloadResourceTotals()
    assert (led.workflow_count, led.job_count) == (0, 0)


def test_job_taken_over_by_another_manager_stops_counting() -> None:
    led_jobs = {"job-1", "job-2"}
    led, _clock = make_led(led_jobs)
    record(led, "wf-1", "job-1", cpu=100.0, memory=1.0)
    record(led, "wf-2", "job-2", cpu=200.0, memory=1.0)

    led_jobs.discard("job-1")

    assert led.totals().cpu_percent == 200.0
    assert (led.workflow_count, led.job_count) == (1, 1)


def test_stale_estimate_is_dropped_at_the_threshold_not_before() -> None:
    led, clock = make_led({"job-1"})
    record(led, "wf-1", "job-1", cpu=100.0, memory=1.0)

    clock.now = STALENESS_SECONDS
    assert led.totals().workflow_count == 1

    clock.now = math.nextafter(STALENESS_SECONDS, math.inf)
    assert led.totals() == WorkloadResourceTotals()
    assert (led.workflow_count, led.job_count) == (0, 0)


def test_gate_sums_led_workloads_and_takes_capacity_from_newest_report() -> None:
    clock = SteppedClock()
    aggregator = DatacenterResourceAggregator(clock, STALENESS_SECONDS)
    aggregator.record("dc-1", MANAGER_A, report(cpu=200.0, memory=4 * 1024**3, cpu_variance=9.0, capacity=400.0), age_seconds=0.0)
    clock.now = 1.0
    aggregator.record("dc-1", MANAGER_B, report(cpu=200.0, memory=4 * 1024**3, cpu_variance=16.0), age_seconds=0.0)

    view = aggregator.view("dc-1")

    assert view.reporting_manager_count == 2
    assert view.workload_cpu_percent == 400.0
    assert view.workload_cpu_uncertainty == 5.0
    assert view.cpu_capacity_percent == CPU_CAPACITY
    assert view.cpu_pressure == 400.0 / CPU_CAPACITY
    assert view.memory_pressure == (8 * 1024**3) / MEMORY_CAPACITY
    assert (view.manager_cpu_percent, view.manager_memory_bytes) == (10.0, 200)


def test_pressure_is_capped_at_one() -> None:
    aggregator = DatacenterResourceAggregator(SteppedClock(), STALENESS_SECONDS)
    aggregator.record("dc-1", MANAGER_A, report(cpu=10 * CPU_CAPACITY, memory=2 * MEMORY_CAPACITY), age_seconds=0.0)

    view = aggregator.view("dc-1")

    assert (view.cpu_pressure, view.memory_pressure) == (1.0, 1.0)


def test_stale_manager_report_stops_counting_and_is_dropped() -> None:
    clock = SteppedClock()
    aggregator = DatacenterResourceAggregator(clock, STALENESS_SECONDS)
    aggregator.record("dc-1", MANAGER_A, report(cpu=100.0, memory=1.0), age_seconds=0.0)
    clock.now = 10.0
    aggregator.record("dc-1", MANAGER_B, report(cpu=300.0, memory=1.0), age_seconds=0.0)

    clock.now = math.nextafter(STALENESS_SECONDS, math.inf)
    assert aggregator.view("dc-1").workload_cpu_percent == 300.0

    clock.now = math.nextafter(10.0 + STALENESS_SECONDS, math.inf)
    assert aggregator.view("dc-1") is None
    assert aggregator._reports == {}


def test_a_forwarded_report_ages_from_when_its_manager_made_it() -> None:
    clock = SteppedClock()
    aggregator = DatacenterResourceAggregator(clock, STALENESS_SECONDS)
    clock.now = 10.0
    # Gossiped on by a peer: made 4s ago, not now.
    aggregator.record("dc-1", MANAGER_A, report(cpu=100.0, memory=1.0), age_seconds=4.0)

    ((address, _, age),) = aggregator.fresh_reports("dc-1")
    assert (address, age) == (MANAGER_A, 4.0)

    clock.now = math.nextafter(10.0 - 4.0 + STALENESS_SECONDS, math.inf)
    assert aggregator.fresh_reports("dc-1") == []


def test_the_freshest_copy_of_a_managers_report_wins() -> None:
    clock = SteppedClock()
    aggregator = DatacenterResourceAggregator(clock, STALENESS_SECONDS)
    clock.now = 10.0
    aggregator.record("dc-1", MANAGER_A, report(cpu=300.0, memory=1.0), age_seconds=1.0)
    # An older copy arriving later (a longer gossip path) changes nothing.
    aggregator.record("dc-1", MANAGER_A, report(cpu=100.0, memory=1.0), age_seconds=5.0)
    assert aggregator.view("dc-1").workload_cpu_percent == 300.0

    aggregator.record("dc-1", MANAGER_A, report(cpu=200.0, memory=1.0), age_seconds=0.0)
    assert aggregator.view("dc-1").workload_cpu_percent == 200.0


def test_no_view_without_known_capacity() -> None:
    aggregator = DatacenterResourceAggregator(SteppedClock(), STALENESS_SECONDS)
    assert aggregator.view("dc-1") is None

    aggregator.record("dc-1", MANAGER_A, report(cpu=100.0, memory=1.0, capacity=0.0), age_seconds=0.0)
    assert aggregator.view("dc-1") is None
