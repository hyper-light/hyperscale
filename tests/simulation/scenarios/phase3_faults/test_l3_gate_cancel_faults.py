"""
Phase 3 L3 gate-tier cancellation-under-fault scenario.

The multi-DC companion to the L2
``test_cancellation_under_fault.test_cancel_during_leader_failover``.
Here a gate sits in the cancel path: the client's cancel routes
client -> gate -> DC manager. The scenario kills the DC's manager
leader while the workflow is in flight, then issues a cancel, and
asserts the gate forwards it to the DC's *new* manager leader and the
cancellation completes.

Why this is a distinct scenario from the L2 version:

* The L2 test exercises the client -> manager redirect + manager-side
  Raft-authoritative takeover directly. The gate is absent, so it
  cannot catch a gate-side forwarding regression.

* The gate's ``handle_cancel_job`` forwards to a DC's managers and,
  before the fix this test guards, accepted the *first* parseable
  manager response as "DC done" — without checking ``success`` or
  following the manager's ``leader_addr`` redirect. Right after a
  manager-leader failover the gate's cached manager list frequently
  puts a non-leader (or the just-killed leader) first, so the cancel
  silently under-cancelled and the client's ``await_job_cancellation``
  timed out. This test reproduces exactly that window.

The gate now follows the manager's leader redirect and carries the
failover context (``callback_addr`` = the gate, ``unreachable_addrs``)
on every forwarded ``JobCancelRequest``, mirroring the client's own
cancel path — so the cancel reaches a manager that performs it and the
completion push routes back through the gate to the client.

L3 spec, gate SWIM bracket, and takeover-budget derivation mirror
``test_l3_gate_faults`` (which this module imports from) so the two
gate-tier fault families stay in lockstep.
"""

import pytest

from hyperscale.distributed.testing.workflows import LongRunningWorkflow
from tests.simulation.harness import (
    ClusterHarness,
    ClusterSpec,
    DCSpec,
    EnvOverrides,
    ExecutionMode,
    HarnessTimeouts,
    Submission,
    SubmissionPattern,
    WorkloadSpec,
    dc_has_leader,
    wait_until,
)
from tests.simulation.scenarios.phase3_faults.test_l3_gate_faults import (
    _DC_ID,
    _GATE_COUNT,
    _L3_RUNNING_TIMEOUT_SECONDS,
    _L3_STABILIZATION_SECONDS,
    _wait_for_gate_cluster,
)
from tests.simulation.scenarios.phase3_faults.test_leader_faults import (
    _find_leader,
)


# The workflow must stay in flight across the kill + new-leader-wait +
# cancel window. ``LongRunningWorkflow`` sleeps 30 s; the cancel budget
# below is generous enough that the cancel lands mid-flight rather than
# racing the workflow to completion (which would exercise the
# already-completed short-circuit instead of the mid-flight cancel).
_CANCEL_WORKLOAD_TIMEOUT_SECONDS = 60.0
_CANCEL_BUDGET_SECONDS = 45.0
_NEW_MANAGER_LEADER_BUDGET_SECONDS = 30.0


def _l3_cancel_spec() -> ClusterSpec:
    """3 gates + 1 DC × (3 managers + 1 worker × 2 cores).

    Deliberately **3 managers**, not the 2 that ``test_l3_gate_faults``
    uses. The gate-forwarding regression this scenario guards only
    manifests when, after the manager leader is killed, at least one
    *live non-leader* manager remains: the gate can then forward the
    cancel to that non-leader first and receive a
    ``JobCancelResponse(success=False, leader_addr=...)`` redirect. The
    unfixed gate accepted that redirect as "DC done" and under-
    cancelled. With only 2 managers the single survivor *is* the new
    leader, so no live non-leader exists to return a redirect and the
    bug window never opens — the test would pass regardless of the fix
    and guard nothing. Three managers guarantee a redirect-capable
    survivor.

    Gate SWIM bracket and stabilization budget mirror
    ``test_l3_gate_faults._l3_spec`` so the two gate-tier fault
    families stay in lockstep.
    """
    return ClusterSpec(
        gates=_GATE_COUNT,
        datacenters={
            _DC_ID: DCSpec(managers=3, workers=1, cores_per_worker=2),
        },
        env=EnvOverrides(
            request_timeout="5s",
            log_level="error",
            gate_swim_global_min_timeout=5.0,
            gate_swim_global_max_timeout=15.0,
        ),
        timeouts=HarnessTimeouts(stabilization_default=_L3_STABILIZATION_SECONDS),
    )


def _cancellable_l3_workload() -> WorkloadSpec:
    """Single ``LongRunningWorkflow`` submission, no expectations.

    Same shape as the L2 cancellation workload: the cancel path ends
    in a ``workflow_cancellation_complete`` push (a separate callback
    from ``_on_workflow_result``), so ``_all_complete_event`` never
    fires on a clean cancel and an ``ExpectCompletionWithin`` would
    mistake the cancel for a hang. The test asserts cancellation
    success directly via ``driver.cancel``.
    """
    return WorkloadSpec(
        submissions=[
            Submission(
                workflows=[([], LongRunningWorkflow)],
                dc_count=1,
                timeout_seconds=_CANCEL_WORKLOAD_TIMEOUT_SECONDS,
                vus=1,
            ),
        ],
        pattern=SubmissionPattern.SINGLE,
        expectations=[],
    )


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_l3_cancel_during_manager_leader_failover_via_gate() -> None:
    """Cancel a gate-routed job while the DC's manager leader is killed.

    Sequence:
      1. Form the gate cluster and elect an initial DC manager leader.
      2. Submit a ``LongRunningWorkflow`` through the gate; wait until
         it is RUNNING so the cancel lands mid-flight.
      3. Kill the DC's manager leader.
      4. Wait for a new manager leader to emerge among the survivors.
      5. Cancel the job through the gate. The gate must forward to the
         new manager leader (following the redirect the surviving
         non-leader managers hand back) and the cancellation must
         complete — the client's ``await_job_cancellation`` resolves
         via the completion push routed gate <- manager <- worker.

    The scenario fails if cancellation does not complete within budget,
    which is exactly the symptom the gate-side redirect-follow fix
    eliminates.
    """
    spec = _l3_cancel_spec()
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="l3_cancel_during_manager_leader_failover_via_gate",
    ) as cluster:
        await _wait_for_gate_cluster(cluster)

        managers = cluster.managers(_DC_ID)
        await wait_until(
            dc_has_leader(managers),
            timeout=45.0,
            poll=0.5,
            description="initial DC manager leader before cancel",
            on_fail=lambda: cluster.dump_diagnostics(
                reason="no DC manager leader before the cancel-failover run"
            ),
        )

        async with cluster.workload(_cancellable_l3_workload()) as driver:
            await driver.submit()
            await driver.wait_until_running(
                timeout=_L3_RUNNING_TIMEOUT_SECONDS
            )

            leader_before = _find_leader(managers)
            await cluster.faults.kill(leader_before)

            await wait_until(
                dc_has_leader(
                    [
                        manager
                        for manager in managers
                        if manager.node_id != leader_before.node_id
                    ]
                ),
                timeout=_NEW_MANAGER_LEADER_BUDGET_SECONDS,
                poll=0.5,
                description="new DC manager leader emerges after kill",
                on_fail=lambda: cluster.dump_diagnostics(
                    reason=(
                        "no new manager leader after killing the original; "
                        "gate-routed cancellation cannot complete"
                    )
                ),
            )

            success, errors = await driver.cancel(
                timeout=_CANCEL_BUDGET_SECONDS
            )
            assert success, (
                "gate-routed cancellation under manager-leader failover "
                f"reported failure: {errors}"
            )
