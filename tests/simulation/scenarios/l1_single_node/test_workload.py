"""
L1 workload smoke.

Phase 2 deliverable for the workload pipeline lives in two pieces:

1. ``test_workload_framework_evaluates`` — exercises the framework
   itself: submission spec construction, observations aggregation, and
   expectation evaluation. No real cluster required; validates the
   contract every higher-level scenario relies on.

2. ``test_l1_workload_submission_pipeline`` — full integration: real
   ``HyperscaleClient`` against an L1 cluster, submission via
   ``client.submit_job``, callback-driven observation collection.
   Currently skipped: the production execute → final-result push path
   leaves uncancellable client tasks at workload exit, which hangs
   pytest's loop teardown. Tracked as a Phase 3 prerequisite. When the
   production pipeline lands, the assertions become
   ``ExpectAllWorkflowsComplete`` + ``ExpectCompletionWithin``.
"""

import pytest

from tests.simulation.harness import (
    ClusterHarness,
    ClusterSpec,
    DCSpec,
    EnvOverrides,
    ExecutionMode,
    ExpectAllWorkflowsComplete,
    ExpectCompletionWithin,
    HarnessTimeouts,
    Submission,
    SubmissionPattern,
    WorkloadObservations,
    WorkloadSpec,
)
from tests.simulation.workflows import SimpleWorkflow


@pytest.mark.simulation
def test_workload_framework_evaluates() -> None:
    """Spec construction + observations + expectation evaluator are
    self-consistent without any cluster, client, or network involvement.
    """
    spec = WorkloadSpec(
        submissions=[
            Submission(
                workflows=[([], SimpleWorkflow)],
                dc_count=1,
                timeout_seconds=10.0,
                vus=1,
            ),
        ],
        pattern=SubmissionPattern.SINGLE,
        expectations=[
            ExpectAllWorkflowsComplete(expected_workflow_names=["SimpleWorkflow"]),
            ExpectCompletionWithin(seconds=10.0),
        ],
    )

    # The expectation evaluator returns one result per registered
    # expectation, regardless of cluster state. That contract is what
    # downstream scenarios (Phase 3+) build on.
    obs = WorkloadObservations(
        workflow_results={"SimpleWorkflow": "completed"},
        completion_seconds=1.5,
    )
    results = [exp.evaluate(obs) for exp in spec.expectations]
    assert all(r.holds for r in results), (
        f"all expectations should hold for a clean observation: "
        f"{[(r.name, r.detail) for r in results]}"
    )

    # Negative shape: expectation reports the missing workflow by name.
    obs_missing = WorkloadObservations(workflow_results={})
    missing_result = spec.expectations[0].evaluate(obs_missing)
    assert not missing_result.holds
    assert "SimpleWorkflow" in missing_result.detail

    # Negative shape: completion-within reports the budget breach.
    obs_slow = WorkloadObservations(
        workflow_results={"SimpleWorkflow": "completed"},
        completion_seconds=11.0,
    )
    slow_result = spec.expectations[1].evaluate(obs_slow)
    assert not slow_result.holds
    assert "11.0" in slow_result.detail or "budget" in slow_result.detail


def _l1_spec() -> ClusterSpec:
    return ClusterSpec(
        gates=0,
        datacenters={
            "local": DCSpec(managers=1, workers=1, cores_per_worker=2),
        },
        env=EnvOverrides(request_timeout="5s", log_level="error"),
        base_port=19500,
        timeouts=HarnessTimeouts(stabilization_default=45.0),
    )


@pytest.mark.asyncio
@pytest.mark.simulation
@pytest.mark.skip(
    reason=(
        "Submit path now reaches the manager (Phase 2 closure: leader "
        "election started in manager.start, workflow allowlist registered "
        "for tests.simulation.workflows.*, send_tcp tuple unpacking on "
        "dispatch path, and load-shedder enum signature). However the "
        "manager's WorkflowDispatcher → worker execution → "
        "WorkflowResultPush → client callback round-trip is still not "
        "delivering completion: the job is accepted (submit_errors is "
        "empty, job_id returned) but no workflow_result push reaches the "
        "client within budget. This is the Phase 3 prerequisite the "
        "original skip reason called out, scoped down. Re-enable once "
        "WorkflowDispatcher actually sends dispatch + worker pushes "
        "result back end-to-end at L1."
    ),
)
async def test_l1_workload_submission_pipeline() -> None:
    spec = _l1_spec()
    workload = WorkloadSpec(
        submissions=[
            Submission(
                workflows=[([], SimpleWorkflow)],
                dc_count=1,
                timeout_seconds=10.0,
                vus=1,
            ),
        ],
        pattern=SubmissionPattern.SINGLE,
        expectations=[
            ExpectAllWorkflowsComplete(expected_workflow_names=["SimpleWorkflow"]),
            ExpectCompletionWithin(seconds=10.0),
        ],
    )
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="l1_workload_submission_pipeline",
    ) as cluster:
        async with cluster.workload(workload) as driver:
            await driver.submit_and_wait()
            results = driver.evaluate_expectations()
            assert all(r.holds for r in results), (
                f"expectations failed: {[(r.name, r.detail) for r in results]}"
            )
    assert cluster.supervisor.cleanup_errors == [], cluster.supervisor.cleanup_errors
