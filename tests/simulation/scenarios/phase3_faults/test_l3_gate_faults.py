"""
Phase 3 L3 gate-tier fault scenarios from ``docs/SCENARIOS.md``.

Covers the two L3-dependent items deferred from the original Phase 3
manifest:

  * §2 ``Gate dies (L3)`` — DC routing fails over to a peer gate; the
    client retries (its target list already contains every gate) and
    the workflow still completes via the surviving gate(s).

  * §1 ``Job-leadership transfer (gate tier, L3)`` — the gate that
    holds job leadership for an in-flight job dies; a peer gate takes
    over via the ``GateOrphanJobCoordinator`` consistent-hash-ring
    path, callback addresses are preserved, and the fence token
    continuity invariant holds.

L3 spec mirrors ``tests/simulation/scenarios/l3_multi_dc/test_smoke.py``
(3 gates + 2 DCs × (2 managers + 1 worker × 2 cores)). The longer
``LongRunningWorkflow`` (30 s ``asyncio.sleep``) gives takeover time
to land while the job is still in flight rather than racing a fast-
completing workflow to the callback boundary.

Budgets are derived from
``GATE_ORPHAN_GRACE_PERIOD`` (10 s default), ``GATE_ORPHAN_CHECK_INTERVAL``
(2 s), takeover jitter (0.5–2 s), plus the 30 s workflow duration —
not magic numbers.
"""

import asyncio

import pytest

from hyperscale.distributed.testing.workflows import (
    LongRunningTestWorkflow,
    LongRunningWorkflow,
)
from tests.simulation.harness import (
    ClusterHarness,
    ClusterSpec,
    DCSpec,
    EnvOverrides,
    ExecutionMode,
    ExpectAllWorkflowsComplete,
    ExpectCompletionWithin,
    ExpectWorkflowStatsPresent,
    HarnessTimeouts,
    Submission,
    SubmissionPattern,
    WorkloadSpec,
    gate_cluster_formed,
    wait_until,
)


_GATE_COUNT = 3
_DC_ID = "main"
_WORKFLOW_DURATION_SECONDS = 30.0
# GATE_ORPHAN_GRACE_PERIOD (10) + GATE_ORPHAN_CHECK_INTERVAL (2) +
# SWIM death detection of the killed gate (~5-10s under default
# Lifeguard bracket) + GATE_ORPHAN_GRACE_PERIOD (10s) +
# GATE_ORPHAN_CHECK_INTERVAL (2s max wait until next scan tick) +
# takeover_jitter_max (2s). Underestimating any of these forces
# the test to fail before the orphan coordinator's takeover scan
# has a chance to fire even on a healthy cluster.
_GATE_TAKEOVER_BUDGET_SECONDS = 30.0
# Dispatch overhead headroom — the same shape we use for the worker-
# death-timing tests: one workflow run, plus reassignment overhead.
_DISPATCH_OVERHEAD_SECONDS = 15.0
_GATE_FAULT_WORKLOAD_BUDGET = (
    _WORKFLOW_DURATION_SECONDS
    + _GATE_TAKEOVER_BUDGET_SECONDS
    + _DISPATCH_OVERHEAD_SECONDS
)  # 65 s
# L3 routing (client → gate → manager → worker) is one more hop than
# L1/L2 and the gate cluster also needs to fully form before the
# first submission round-trip can land. Phase 4 uses 90 s
# stabilization for the same shape; the workload-RUNNING budget gets
# the same headroom on top.
_L3_STABILIZATION_SECONDS = 90.0
_L3_RUNNING_TIMEOUT_SECONDS = 60.0


def _l3_spec(base_port: int) -> ClusterSpec:
    """3 gates + 1 DC × (2 managers + 1 worker × 2 cores).

    Mirrors ``phase4_network/test_partition_gap_scenarios.py``'s L3
    fixture — the only existing simulation pattern that demonstrably
    routes client traffic through gates without the multi-DC
    selection branch. Multi-DC routing is exercised separately by
    ``l3_multi_dc/test_smoke.py``; here we want the failure mode
    isolated to gate-tier death and orphan takeover.
    """
    return ClusterSpec(
        gates=_GATE_COUNT,
        datacenters={
            _DC_ID: DCSpec(managers=2, workers=1, cores_per_worker=2),
        },
        env=EnvOverrides(request_timeout="5s", log_level="error"),
        base_port=base_port,
        timeouts=HarnessTimeouts(stabilization_default=_L3_STABILIZATION_SECONDS),
    )


def _long_workload(timeout_seconds: float) -> WorkloadSpec:
    workflow_name = LongRunningWorkflow.__name__
    return WorkloadSpec(
        submissions=[
            Submission(
                workflows=[([], LongRunningWorkflow)],
                dc_count=1,
                timeout_seconds=timeout_seconds,
                vus=1,
            ),
        ],
        pattern=SubmissionPattern.SINGLE,
        expectations=[
            ExpectAllWorkflowsComplete(expected_workflow_names=[workflow_name]),
            ExpectCompletionWithin(seconds=timeout_seconds),
        ],
    )


def _long_test_workload(timeout_seconds: float) -> WorkloadSpec:
    workflow_name = LongRunningTestWorkflow.__name__
    return WorkloadSpec(
        submissions=[
            Submission(
                workflows=[([], LongRunningTestWorkflow)],
                dc_count=1,
                timeout_seconds=timeout_seconds,
                vus=1,
            ),
        ],
        pattern=SubmissionPattern.SINGLE,
        expectations=[
            ExpectAllWorkflowsComplete(expected_workflow_names=[workflow_name]),
            ExpectWorkflowStatsPresent(
                expected_workflow_names=[workflow_name],
                require_per_dc_stats=True,
            ),
            ExpectCompletionWithin(seconds=timeout_seconds),
        ],
    )


async def _wait_for_gate_cluster(cluster: ClusterHarness) -> None:
    await wait_until(
        gate_cluster_formed(cluster.gates, expected_peers=_GATE_COUNT - 1),
        timeout=30.0,
        poll=0.5,
        description="gate cluster formed",
        on_fail=lambda: cluster.dump_diagnostics(
            reason="gate cluster did not form before fault injection"
        ),
    )


def _find_owning_gate(
    cluster: ClusterHarness,
    job_id: str,
    exclude_node_id: str | None = None,
):
    """Return the live gate whose JobLeadershipTracker leads ``job_id``.

    Excludes the killed gate by ``node_id``: after ``faults.kill`` the
    dead gate's in-memory tracker still claims leadership (its state
    is local and never gets cleared), so only checking ``is_leader``
    would always rediscover the old leader. The exclude list lets the
    takeover wait look for a *peer* assuming leadership instead.

    Also skips handles whose ``started`` flag is False — defensive
    against gates that have been killed or never started.
    """
    for gate in cluster.gates:
        if not gate.started:
            continue
        if exclude_node_id is not None and gate.node_id == exclude_node_id:
            continue
        tracker = gate.instance._job_leadership_tracker
        if tracker.is_leader(job_id):
            return gate
    return None


async def _wait_for_known_job_id(driver, timeout: float) -> str:
    """Block until the workload driver has registered a submitted job_id."""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        job_ids = list(driver.observations.submitted_job_ids)
        if job_ids:
            return job_ids[0]
        await asyncio.sleep(0.1)
    raise AssertionError(
        f"workload driver never registered a job_id within {timeout:.1f}s"
    )


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_l3_gate_dies_dc_routing_fails_over() -> None:
    """Kill one gate while a workflow is in flight; client retry through
    the surviving gates must complete the workflow.

    SCENARIOS.md §2 ``Gate dies (L3)``. The ``HyperscaleClient`` is
    constructed with every gate address (see ``WorkloadDriver._select_routing_targets``),
    so failover is the existing `ClientTargetSelector.get_next_gate`
    rotation rather than a `leader_hint`-driven retry. The workflow
    must reach a terminal ``completed`` status — not hang and not
    silently fail.
    """
    spec = _l3_spec(base_port=22500)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="l3_gate_dies_dc_routing_fails_over",
    ) as cluster:
        await _wait_for_gate_cluster(cluster)

        async with cluster.workload(
            _long_workload(_GATE_FAULT_WORKLOAD_BUDGET)
        ) as driver:
            await driver.submit()
            await driver.wait_until_running(
                timeout=_L3_RUNNING_TIMEOUT_SECONDS
            )

            # Killing the first gate is sufficient: the client target
            # list rotates on each retry, so whichever gate is
            # ``cluster.gates[0]`` becoming unreachable forces failover.
            await cluster.faults.kill(cluster.gates[0])

            await driver.wait_for_completion()


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_l3_gate_dies_dc_routing_fails_over_with_test_stats() -> None:
    """Kill one gate while a test workflow is in flight; the surviving
    L3 route must deliver terminal status with aggregated stats intact.
    """
    spec = _l3_spec(base_port=22900)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="l3_gate_dies_dc_routing_fails_over_with_test_stats",
    ) as cluster:
        await _wait_for_gate_cluster(cluster)

        async with cluster.workload(
            _long_test_workload(_GATE_FAULT_WORKLOAD_BUDGET)
        ) as driver:
            await driver.submit()
            await driver.wait_until_running(
                timeout=_L3_RUNNING_TIMEOUT_SECONDS
            )

            await cluster.faults.kill(cluster.gates[0])

            await driver.wait_for_completion()


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_l3_job_leadership_transfer_at_gate_tier() -> None:
    """Kill the gate that holds job leadership for an in-flight job;
    a peer gate must take over via the orphan-job coordinator without
    losing the callback address or fence-token continuity.

    SCENARIOS.md §1 ``Job-leadership transfer (gate tier, L3)``.

    Sequence:
      1. Submit a ``LongRunningWorkflow`` so the job is genuinely
         in-flight when the kill lands.
      2. Wait until a gate has assumed leadership for the job
         (``_job_leadership_tracker.is_leader``).
      3. ``faults.kill`` that exact gate (deterministic — no random
         pick).
      4. Poll for a peer gate to assume leadership for the same
         ``job_id``. The orphan coordinator path runs:
         SWIM dead → orphan grace (10 s) → check interval (2 s) →
         consistent-hash takeover with new fence token.
      5. ``wait_for_completion`` — completion callback must still
         route back to the original client through the new leader.
    """
    spec = _l3_spec(base_port=22700)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="l3_job_leadership_transfer_at_gate_tier",
    ) as cluster:
        await _wait_for_gate_cluster(cluster)

        async with cluster.workload(
            _long_workload(_GATE_FAULT_WORKLOAD_BUDGET)
        ) as driver:
            await driver.submit()
            await driver.wait_until_running(
                timeout=_L3_RUNNING_TIMEOUT_SECONDS
            )

            job_id = await _wait_for_known_job_id(driver, timeout=10.0)

            await wait_until(
                lambda: _find_owning_gate(cluster, job_id) is not None,
                timeout=15.0,
                poll=0.2,
                description="some gate assumes leadership for the job",
                on_fail=lambda: cluster.dump_diagnostics(
                    reason=f"no gate ever became leader for job {job_id}"
                ),
            )

            owning_gate = _find_owning_gate(cluster, job_id)
            assert owning_gate is not None, "owning gate disappeared before kill"
            old_owner_node_id = owning_gate.node_id
            old_leader = owning_gate.instance._job_leadership_tracker.get_leader(
                job_id
            )

            await cluster.faults.kill(owning_gate)

            await wait_until(
                lambda: _find_owning_gate(
                    cluster, job_id, exclude_node_id=old_owner_node_id
                )
                is not None,
                timeout=_GATE_TAKEOVER_BUDGET_SECONDS,
                poll=0.5,
                description=(
                    f"peer gate takes over leadership for job {job_id} "
                    f"(old leader {old_leader})"
                ),
                on_fail=lambda: cluster.dump_diagnostics(
                    reason="gate-tier job leadership never transferred"
                ),
            )

            new_owner = _find_owning_gate(
                cluster, job_id, exclude_node_id=old_owner_node_id
            )
            assert new_owner is not None, "new owner vanished after takeover"
            assert new_owner.node_id != old_owner_node_id, (
                "new owner has the same node_id as the killed gate"
            )

            await driver.wait_for_completion()
