"""
Phase 3 fault-injection-during-workload scenarios.

Stresses the dispatch / result-push pipeline against simultaneous node
loss. Each scenario submits workload via ``WorkloadDriver``, lands the
fault between dispatch and completion, and asserts the cluster either
recovers or surfaces a failure with a reason — never hangs.

The mid-workload landing is achieved with the Phase 3.5
``WorkloadDriver`` API:

  await driver.submit()
  await driver.wait_until_running(timeout=30)
  await cluster.faults.kill(...)        # lands MID-execution
  await driver.wait_for_completion()

Scenarios:

* ``test_worker_kill_mid_workload`` — kill a worker while a workflow is
  running on it.
* ``test_leader_kill_mid_workload`` — kill the manager leader while a
  workflow is in flight.
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
async def test_worker_kill_mid_workload() -> None:
    """Kill a worker mid-execution; cluster must surface failure or
    redispatch — never hang waiting for results.

    Sequence:
      1. Submit a workflow (1 VU so it lands on exactly one worker).
      2. ``wait_until_running`` — first JobStatusPush.RUNNING observed.
      3. Pick the worker the workload is running on (or any worker;
         with a 2-worker DC we have a 50/50 chance of killing the
         executing one — either way the cluster must complete the
         workload or surface failure).
      4. ``faults.kill`` the worker.
      5. ``wait_for_completion`` — bounded by the spec's
         ``timeout_seconds``. ``ExpectCompletionWithin`` on the spec
         enforces the budget; ``ExpectAllWorkflowsComplete`` either
         passes (redispatch succeeded) or surfaces the failed
         workflow's status (clear failure rather than hang).

    The ``__aexit__`` of ``cluster.workload`` runs the registered
    expectations and raises ``WorkloadFailure`` if any fail. The
    harness's diagnostic dump fires automatically on failure.
    """
    spec = _l2_spec(base_port=21300)
    workload = _simple_workload(timeout_seconds=30.0)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="worker_kill_mid_workload",
    ) as cluster:
        async with cluster.workload(workload) as driver:
            await driver.submit()
            await driver.wait_until_running(timeout=30.0)

            workers = cluster.workers("main")
            assert workers, "no workers in cluster"
            await cluster.faults.kill(workers[0])

            await driver.wait_for_completion()


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_leader_kill_mid_workload() -> None:
    """Kill the manager leader mid-execution; new leader must take over
    and either redispatch or surface failure within budget.

    Sequence:
      1. Wait for an initial leader.
      2. Submit a workflow.
      3. ``wait_until_running`` — first JobStatusPush.RUNNING observed
         (proves the workflow has been dispatched and is executing).
      4. Identify the current leader, ``faults.kill`` it.
      5. ``wait_until`` a new leader from the survivor set.
      6. ``wait_for_completion`` — same budget enforcement as above.
    """
    spec = _l2_spec(base_port=21400)
    workload = _simple_workload(timeout_seconds=45.0)
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
                description="new leader after kill mid-workload",
                on_fail=lambda: cluster.dump_diagnostics(
                    reason="no new leader after killing original mid-workload"
                ),
            )

            await driver.wait_for_completion()
