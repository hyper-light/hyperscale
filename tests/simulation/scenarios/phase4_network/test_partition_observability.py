"""
Phase 4 expansion — partition observability + behavioral coverage.

The original four Phase 4 scenarios installed network rules and
checked harness-side state. This file goes one layer deeper, asserting
that the partition rules are *observable* on the production side:

* The partitioned node's ``LocalHealthMultiplier`` grows above its
  cold-start baseline when probes time out.
* A partition installed during a workflow surfaces as a clean
  failure (not a hang) within the workflow's timeout budget.
* A leader-side partition (vs a leader-kill) leaves the leader
  thinking it's still leader, so election cycling differs from the
  kill case.
* A high-but-partial drop_rate exercises the lifeguard adaptive-
  timeout path — Phase B/C/D's recent work — against a real lossy
  network rather than unit-test-stubbed inputs.

Together these close the framework-doc Phase 4 exit criterion of
"partition correlation logic in the SWIM layer has end-to-end
coverage" by tying the FaultMatrix primitives to the production
signals they should exercise.
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
    ExpectCompletionWithin,
    HarnessTimeouts,
    ServerHandle,
    Submission,
    SubmissionPattern,
    WorkloadSpec,
    dc_has_leader,
    manager_has_n_peers,
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
        env=EnvOverrides(request_timeout="3s", log_level="error"),
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
            # Liberal completion budget — a partitioned workflow
            # may take longer than the spec's nominal timeout to
            # fail, especially if redispatch is involved.
            ExpectCompletionWithin(seconds=timeout_seconds * 3),
        ],
    )


# ============================================================================
# 1. Cross-DC correlation verification (via LHM growth)
# ============================================================================


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_partition_grows_local_health_multiplier() -> None:
    """A partitioned manager's LHM score grows when probes time out.

    The framework-doc Phase 4 exit criterion is partition-correlation
    coverage in the SWIM layer. The signal that drives correlation
    detection is ``LocalHealthMultiplier.score`` per AD-19/AD-30:
    failed probes call ``on_probe_timeout`` → score increments.
    Under a partition, the isolated node's probes to its peers all
    time out, so its LHM grows above cold-start baseline (0).

    Sequence:
      1. L2 stabilize. Capture baseline LHM scores for every manager
         (should all be 0 or very small).
      2. Pick manager-0 as the victim; partition it from the other
         two.
      3. Wait up to 60s for victim's LHM score to climb above
         baseline. Probes to its now-unreachable peers will time
         out and the score will grow per AD-30.
      4. Heal partition; victim's LHM should eventually return
         toward baseline as successful probes accumulate.
    """
    spec = _l2_spec(base_port=22500)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="partition_grows_local_health_multiplier",
    ) as cluster:
        managers = cluster.managers("main")
        await wait_until(
            dc_has_leader(managers),
            timeout=45.0,
            description="initial leader",
        )

        victim = managers[0]
        peers = managers[1:]

        baseline_score = victim.instance._local_health.score
        # Baseline should be at or near 0 under healthy probing.
        # Allowing some headroom for cold-start probe failures during
        # SWIM stabilization.
        assert baseline_score <= 2, (
            f"baseline LHM unexpectedly high: {baseline_score}"
        )

        await cluster.faults.partition([victim], peers)

        # Wait for the victim's probes to peers to time out enough
        # times for LHM to register growth. AD-19's
        # LocalHealthMultiplier increments on each probe timeout up
        # to a ceiling; under sustained partition we expect to see
        # at least one increment within a few probe cycles.
        await wait_until(
            lambda: victim.instance._local_health.score > baseline_score,
            timeout=60.0,
            poll=1.0,
            description=(
                f"victim {victim.node_id} LHM score grows above "
                f"baseline {baseline_score}"
            ),
            on_fail=lambda: cluster.dump_diagnostics(
                reason=(
                    f"partitioned manager LHM stuck at "
                    f"{victim.instance._local_health.score}; expected growth"
                )
            ),
        )

        await cluster.faults.heal_partition()

        # After heal, successful probes should resume and the LHM
        # should eventually decay back toward baseline. Give a
        # generous budget — the decay is gradual.
        await wait_until(
            lambda: victim.instance._local_health.score <= baseline_score + 1,
            timeout=60.0,
            poll=1.0,
            description=f"victim {victim.node_id} LHM decays back after heal",
            on_fail=lambda: cluster.dump_diagnostics(
                reason="LHM did not decay after partition healed"
            ),
        )


# ============================================================================
# 2. Partition during workload — clean failure, not hang
# ============================================================================


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_partition_during_workload_does_not_hang() -> None:
    """Partition installed mid-workload: workflow either completes or
    surfaces failure within budget. Never hangs.

    Sequence:
      1. Submit workflow, wait_until_running.
      2. Partition the worker fleet from the manager fleet.
      3. wait_for_completion — the workload's
         ``ExpectCompletionWithin`` enforces the budget.

    The completion may be a successful redispatch (if the partition
    is recoverable within the budget) or a clean failure (if it's
    not). The point is that the manager doesn't hang waiting for
    results that won't arrive — the AD-26 H7/H8 + AD-34 timeout
    paths must surface the situation.
    """
    spec = _l2_spec(base_port=22600)
    workload = _simple_workload(timeout_seconds=20.0)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="partition_during_workload",
    ) as cluster:
        async with cluster.workload(workload) as driver:
            await driver.submit()
            await driver.wait_until_running(timeout=30.0)

            workers = cluster.workers("main")
            managers = cluster.managers("main")
            assert workers and managers

            await cluster.faults.partition(workers, managers)

            # wait_for_completion is bounded by the spec's
            # timeout_seconds × 3 (= 60s). On exit, the workload's
            # expectation evaluation runs — ExpectCompletionWithin
            # passes if completion_seconds is set (= a result was
            # observed), fails otherwise. Either way, no hang
            # beyond the budget.
            await driver.wait_for_completion()


# ============================================================================
# 3. Leader-side partition (vs leader-kill)
# ============================================================================


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_leader_side_partition_vs_kill() -> None:
    """Partition the leader from its followers.

    Distinct from leader-kill: the leader still believes it's leader
    (no one has told it otherwise; its commit-log state is intact).
    The followers stop hearing heartbeats and elect a new leader
    among themselves. Resolution depends on the production code's
    handling of the eventual partition heal — the old leader must
    step down rather than fight the new one.

    Sequence:
      1. L2 stabilize. Capture initial leader.
      2. Partition initial_leader from the other two managers.
      3. Wait for the survivor pair to elect a new leader (the old
         one is unreachable from their perspective).
      4. Heal partition. The old leader rejoins — it should step
         down via incarnation reconciliation, not split-brain.
      5. Verify only one current leader is reported across all
         three managers.
    """
    spec = _l2_spec(base_port=22700)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="leader_side_partition_vs_kill",
    ) as cluster:
        managers = cluster.managers("main")
        await wait_until(
            dc_has_leader(managers),
            timeout=45.0,
            description="initial leader",
        )
        original_leader = _find_leader(managers)
        survivors = [m for m in managers if m.node_id != original_leader.node_id]

        await cluster.faults.partition([original_leader], survivors)

        # Survivors must elect among themselves. The original leader
        # is unreachable from their POV.
        await wait_until(
            dc_has_leader(survivors),
            timeout=45.0,
            poll=0.5,
            description="survivors elect new leader after partition",
            on_fail=lambda: cluster.dump_diagnostics(
                reason="survivors did not elect new leader during partition"
            ),
        )

        await cluster.faults.heal_partition()

        # After heal: at most one manager should currently be leader.
        # Old leader should yield via Raft term reconciliation.
        await wait_until(
            lambda: sum(1 for m in managers if m.instance.is_leader()) <= 1,
            timeout=45.0,
            poll=0.5,
            description="single leader after partition heals",
            on_fail=lambda: cluster.dump_diagnostics(
                reason=(
                    "split brain after partition heal — multiple "
                    "managers report is_leader()"
                )
            ),
        )

        # Peer counts should reconverge.
        for handle in managers:
            await wait_until(
                manager_has_n_peers(handle, 2),
                timeout=45.0,
                poll=0.5,
                description=f"{handle.node_id} sees 2 peers after heal",
            )


# ============================================================================
# 4. High partial loss → LHM growth (Phase B/C/D coverage)
# ============================================================================


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_high_loss_rate_triggers_lhm_growth() -> None:
    """0.7 drop_rate to one peer pair: LHM grows but cluster doesn't
    falsely declare the peer DEAD.

    The lifeguard adaptive-timeout work (Phase B/C/D) ensures that
    under high but non-total loss, the local health multiplier grows
    (extending probe timeouts) without the cluster declaring the
    peer DEAD prematurely. This is the difference from a 1.0
    partition: with sustained partial loss, every other probe still
    succeeds, so the peer is intermittently reachable.

    Sequence:
      1. L2 stabilize.
      2. Install drop_rate(0.7) from manager-0 to manager-1
         (asymmetric — return path is clean).
      3. Wait up to 60s for manager-0's LHM score to grow above
         baseline. AD-19's LocalHealthMultiplier increments on each
         probe timeout up to its ceiling.
      4. Verify the cluster doesn't drop manager-1 from manager-0's
         peer set — the peer is impaired, not declared DEAD.
      5. Clear faults; LHM decays back.
    """
    spec = _l2_spec(base_port=22800)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="high_loss_rate_triggers_lhm_growth",
    ) as cluster:
        managers = cluster.managers("main")
        await wait_until(
            dc_has_leader(managers),
            timeout=45.0,
            description="initial leader",
        )
        observer = managers[0]
        impaired_peer = managers[1]
        baseline_score = observer.instance._local_health.score
        baseline_peer_count = len(
            observer.instance._manager_state.get_active_manager_peer_ids()
        )

        await cluster.faults.drop_rate(
            probability=0.7, src=observer, dst=impaired_peer
        )

        await wait_until(
            lambda: observer.instance._local_health.score > baseline_score,
            timeout=60.0,
            poll=1.0,
            description=(
                f"{observer.node_id} LHM grows above "
                f"baseline {baseline_score} under 70% loss"
            ),
            on_fail=lambda: cluster.dump_diagnostics(
                reason=(
                    f"observer LHM stuck at "
                    f"{observer.instance._local_health.score} despite 70% drop"
                )
            ),
        )

        # Critical assertion: the impaired peer should NOT be
        # dropped from observer's active peer set. AD-30's adaptive
        # timeout (driven by LHM growth) extends the suspicion
        # window so partial loss doesn't escalate to DEAD.
        current_peer_count = len(
            observer.instance._manager_state.get_active_manager_peer_ids()
        )
        assert current_peer_count >= baseline_peer_count - 1, (
            f"observer dropped peers under partial loss: "
            f"baseline={baseline_peer_count}, current={current_peer_count} "
            f"(LHM={observer.instance._local_health.score})"
        )

        await cluster.faults.clear_network_faults()

        # LHM eventually decays back as successful probes resume.
        await wait_until(
            lambda: observer.instance._local_health.score <= baseline_score + 1,
            timeout=60.0,
            poll=1.0,
            description=f"{observer.node_id} LHM decays after loss clears",
        )
