"""
Phase 3 fault-injection-during-workload scenarios.

Stresses the dispatch / result-push pipeline against simultaneous node
loss. Each scenario submits workload via ``WorkloadDriver``, then
injects a fault during execution.

* ``test_worker_kill_mid_workload`` — kill a worker while a workflow is
  running on it. Manager must surface the failure (job marked failed
  with a reason) — *not* hang waiting for results that won't arrive.

* ``test_leader_kill_mid_workload`` — kill the manager leader while a
  workflow is queued. New leader must take over and either redispatch
  or surface failure within budget.
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


def _simple_workload(timeout_seconds: float) -> WorkloadSpec:
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
            ExpectAllWorkflowsComplete(
                expected_workflow_names=["SimpleWorkflow"]
            ),
            ExpectCompletionWithin(seconds=timeout_seconds),
        ],
    )


@pytest.mark.asyncio
@pytest.mark.simulation
@pytest.mark.skip(
    reason=(
        "Requires the harness's WorkloadDriver to expose a "
        "'wait until first workflow has dispatched' hook so the kill "
        "lands MID-execution, not before submit. The driver currently "
        "exposes only submit_and_wait (which blocks until completion or "
        "budget exhaustion). Phase 3.5 work item: add an "
        "``after_dispatch`` callback to WorkloadDriver. Until then, "
        "racing submit_and_wait against an out-of-band kill is flaky."
    ),
)
async def test_worker_kill_mid_workload() -> None:
    """Kill a worker while a workflow is running on it; assert failure
    reason rather than hang.
    """
    spec = _l2_spec(base_port=21300)
    workload = _simple_workload(timeout_seconds=30.0)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="worker_kill_mid_workload",
    ) as cluster:
        async with cluster.workload(workload) as driver:
            # TODO: hook into driver.observations once dispatch-detected
            # before kill, so the kill lands during execution.
            await driver.submit_and_wait()


@pytest.mark.asyncio
@pytest.mark.simulation
@pytest.mark.skip(
    reason=(
        "Same WorkloadDriver hook gap as test_worker_kill_mid_workload. "
        "Re-enable once driver.after_dispatch lands."
    ),
)
async def test_leader_kill_mid_workload() -> None:
    """Kill manager leader while a workflow is queued; new leader takes
    over or job fails with reason.
    """
    spec = _l2_spec(base_port=21400)
    workload = _simple_workload(timeout_seconds=30.0)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="leader_kill_mid_workload",
    ) as cluster:
        managers = cluster.managers("main")
        await wait_until(
            dc_has_leader(managers),
            timeout=30.0,
            poll=0.5,
            description="initial leader before workload",
        )
        leader_before = _find_leader(managers)

        async with cluster.workload(workload) as driver:
            await cluster.faults.kill(leader_before)
            # Wait for new leader to emerge before the workload's budget
            # expires, so submit retries land on the new leader.
            await wait_until(
                dc_has_leader(
                    [m for m in managers if m.node_id != leader_before.node_id]
                ),
                timeout=20.0,
                poll=0.5,
                description="new leader after kill mid-workload",
            )
            await driver.submit_and_wait()
