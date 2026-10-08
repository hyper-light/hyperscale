"""
The continuous invariant catalog against a live L2 cluster running a job.

The harness's ``InvariantChecker`` already ticks the whole catalog in
every scenario, but its loop drops an exception raised by an invariant
(it treats checker bugs as non-fatal), so a check that reads a field the
live nodes do not have would pass silently forever. This scenario calls
each catalog invariant's ``evaluate`` directly against the real nodes on
the checker's own cadence while a workload runs, and requires every
check to evaluate without raising and to hold -- and the state it reads
to be non-empty (fence tokens, a running workflow and its cores, SWIM
members), so a pass is not a vacuous one.
"""

import asyncio

import pytest

from hyperscale.distributed.testing.workflows import SimpleWorkflow

from tests.simulation.harness import (
    ClusterHarness,
    ClusterSpec,
    DCSpec,
    EnvOverrides,
    ExecutionMode,
    ExpectAllWorkflowsComplete,
    HarnessTimeouts,
    Submission,
    SubmissionPattern,
    WorkloadSpec,
    continuous_catalog,
)
from tests.simulation.harness.invariants import SafetyInvariant


def _spec() -> ClusterSpec:
    return ClusterSpec(
        gates=0,
        datacenters={"main": DCSpec(managers=3, workers=2, cores_per_worker=2)},
        env=EnvOverrides(request_timeout="5s", log_level="error"),
        timeouts=HarnessTimeouts(stabilization_default=60.0),
    )


def _workload() -> WorkloadSpec:
    return WorkloadSpec(
        submissions=[Submission(workflows=[([], SimpleWorkflow)], dc_count=1, timeout_seconds=30.0, vus=1)],
        pattern=SubmissionPattern.SINGLE,
        expectations=[ExpectAllWorkflowsComplete(expected_workflow_names=["SimpleWorkflow"])],
    )


def _evaluate_catalog(catalog: list[SafetyInvariant], cluster: ClusterHarness, breaches: list[str]) -> None:
    for invariant in catalog:
        if not (result := invariant.evaluate(cluster)).holds:
            breaches.append(f"{invariant.name}: {result.detail}")


def _note_observed_state(cluster: ClusterHarness, observed: set[str]) -> None:
    sightings = (
        ("manager lease fence token", _manager_holds_fence_token(cluster)),
        ("worker holding cores for a workflow", _worker_holds_cores(cluster)),
        ("SWIM members", _node_holds_members(cluster)),
    )
    observed.update(label for label, seen in sightings if seen)


def _manager_holds_fence_token(cluster: ClusterHarness) -> bool:
    return any(handle.instance._manager_state._job_fencing_tokens for handle in cluster.managers("main"))


def _worker_holds_cores(cluster: ClusterHarness) -> bool:
    return any(handle.instance._core_allocator._workflow_cores for handle in cluster.workers("main"))


def _node_holds_members(cluster: ClusterHarness) -> bool:
    return any(handle.instance._incarnation_tracker.get_all_nodes() for handle in cluster.all_handles())


async def _sample(
    cluster: ClusterHarness,
    catalog: list[SafetyInvariant],
    breaches: list[str],
    observed: set[str],
    stop: asyncio.Event,
) -> int:
    evaluations = 0
    while not stop.is_set():
        _evaluate_catalog(catalog, cluster, breaches)
        _note_observed_state(cluster, observed)
        evaluations += 1
        await asyncio.sleep(cluster.spec.timeouts.invariant_poll_interval)
    return evaluations


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_continuous_catalog_evaluates_and_holds_on_a_live_cluster() -> None:
    catalog = continuous_catalog()
    breaches: list[str] = []
    observed: set[str] = set()
    stop = asyncio.Event()
    async with ClusterHarness(
        _spec(), mode=ExecutionMode.REAL, scenario_name="continuous_invariant_catalog"
    ) as cluster:
        sampler = asyncio.create_task(_sample(cluster, catalog, breaches, observed, stop))
        try:
            async with cluster.workload(_workload()) as driver:
                await driver.submit_and_wait()
                results = driver.evaluate_expectations()
        finally:
            stop.set()
            evaluations = await sampler

    assert all(result.holds for result in results), [(result.name, result.detail) for result in results]
    assert evaluations > 1
    assert breaches == []
    assert observed == {
        "manager lease fence token",
        "worker holding cores for a workflow",
        "SWIM members",
    }, observed
