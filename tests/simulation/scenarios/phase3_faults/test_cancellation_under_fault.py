"""
Phase 3 cancellation-under-fault scenarios.

The simulation companions to the integration suite's
``test_cancellation_failover.py``. Both scenarios exercise the
cancellation push chain (client → manager / gate → workers) under
adverse conditions, asserting clean termination rather than hangs.

* ``test_cancel_running_workflow`` — submit a workflow, wait for it
  to reach RUNNING, cancel it, assert cancellation completes within
  budget. Baseline cancellation path with no fault injection.

* ``test_cancel_during_leader_failover`` — submit, wait_until_running,
  kill the manager leader, cancel the job. The cancellation must
  reach the *new* leader (or its forwarding path) and complete
  within budget. Mirrors the ``TestCancellationDuringLeadershipFailover``
  integration test — cancellation must be resilient to the leader
  flipping mid-flight.
"""

import pytest

from hyperscale.distributed.testing.workflows import SimpleWorkflow
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
    WorkloadSpec,
    dc_has_leader,
    wait_until,
)
from tests.simulation.scenarios.phase3_faults.test_leader_faults import (
    _find_leader,
)


def _l2_spec(base_port: int) -> ClusterSpec:
    return ClusterSpec(
        gates=0,
        datacenters={
            "main": DCSpec(managers=3, workers=2, cores_per_worker=2),
        },
        env=EnvOverrides(request_timeout="5s", log_level="error"),
        base_port=base_port,
        timeouts=HarnessTimeouts(stabilization_default=60.0),
    )


def _cancellable_workload(timeout_seconds: float) -> WorkloadSpec:
    """Workload spec for cancellation tests.

    Note: ExpectAllWorkflowsComplete asserts every expected workflow
    name appears in observations.workflow_results — for a cancelled
    workflow that's the CANCELLED status push. The expectation
    doesn't require COMPLETED specifically, so a clean cancellation
    satisfies it.
    """
    return WorkloadSpec(
        submissions=[
            Submission(
                workflows=[([], SimpleWorkflow)],
                dc_count=1,
                timeout_seconds=timeout_seconds,
                vus=1,
            ),
        ],
        pattern=SubmissionPattern.SINGLE,
        expectations=[
            # Expectation evaluation runs at workload __aexit__ — after
            # the test body has cancelled and confirmed completion. We
            # rely on submit_errors being empty and submitted_job_ids
            # being populated as the success criteria; the workflow
            # itself does NOT need to reach COMPLETED for cancellation
            # tests.
            ExpectCompletionWithin(seconds=timeout_seconds * 2),
        ],
    )


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_cancel_running_workflow() -> None:
    """Baseline: cancel a running workflow.

    No faults — just verifies the cancellation push chain works
    end-to-end against a stable cluster. The scenario fails loudly
    (HarnessError) if cancellation does not complete within
    budget; success means the cancel + await round-trip returned
    without timeout.
    """
    spec = _l2_spec(base_port=21500)
    workload = _cancellable_workload(timeout_seconds=30.0)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="cancel_running_workflow",
    ) as cluster:
        async with cluster.workload(workload) as driver:
            await driver.submit()
            await driver.wait_until_running(timeout=30.0)

            success, errors = await driver.cancel(timeout=30.0)
            assert success, (
                f"cancellation reported failure: {errors}"
            )


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_cancel_during_leader_failover() -> None:
    """Cancel a workflow while the manager leader is being killed.

    Sequence:
      1. Wait for initial leader.
      2. Submit workflow; wait_until_running.
      3. Kill the leader.
      4. Wait for a new leader to emerge.
      5. Cancel the job — request must route to the new leader (or
         survive a redirect) and the cancel-complete push must
         propagate back through the new leader's chain.

    The cancellation client uses ``max_redirects=3`` by default, so
    a single leadership transition is well within tolerance. The
    scenario fails if cancellation does not complete within budget,
    which would indicate the cancellation push got stuck on the
    dead leader's queue or never re-routed.
    """
    spec = _l2_spec(base_port=21600)
    workload = _cancellable_workload(timeout_seconds=45.0)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="cancel_during_leader_failover",
    ) as cluster:
        managers = cluster.managers("main")
        await wait_until(
            dc_has_leader(managers),
            timeout=30.0,
            poll=0.5,
            description="initial leader before cancellation test",
        )

        async with cluster.workload(workload) as driver:
            await driver.submit()
            await driver.wait_until_running(timeout=30.0)

            leader_before = _find_leader(managers)
            await cluster.faults.kill(leader_before)

            await wait_until(
                dc_has_leader(
                    [m for m in managers if m.node_id != leader_before.node_id]
                ),
                timeout=30.0,
                poll=0.5,
                description="new leader emerges after kill",
                on_fail=lambda: cluster.dump_diagnostics(
                    reason=(
                        "no new leader after killing original; "
                        "cancellation cannot complete"
                    )
                ),
            )

            success, errors = await driver.cancel(timeout=45.0)
            assert success, (
                f"cancellation under failover reported failure: {errors}"
            )
