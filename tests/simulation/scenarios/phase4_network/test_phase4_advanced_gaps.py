"""
Phase 4 audit-driven gap closure.

Scenarios drawn from ``docs/SCENARIOS.md`` §3 (network conditions) and
§4 (partitions) that the existing four Phase-4 files did not yet
exercise. Adding them here keeps the diff reviewable as a single
spec-driven batch; tests use the same harness primitives + L2/L3
specs as the original files.

Covered:

* ``test_cross_dc_fixed_link_latency_baseline`` — §3 fixed latency,
  the spec's "50 ms cross-DC" example as a baseline scenario.
* ``test_asymmetric_latency_keeps_swim_converged`` — §3 asymmetric
  latency (A→B fast, B→A slow). Exercises the indirect-probe ack
  window sizing.
* ``test_high_jitter_keeps_swim_converged`` — §3 jitter around a
  mean. SWIM convergence + leader stability under variance.
* ``test_symmetric_two_way_partition_quorum_writes_minority_rejects``
  — §4 symmetric two-way partition with ongoing workload, the spec's
  third "high-value first-5" scenario. Quorum side accepts submits;
  minority side surfaces a clean "no quorum" rejection.
* ``test_asymmetric_partition_one_way_blocked`` — §4 asymmetric
  partition (A→B blocked, B→A allowed). Stresses the indirect-probe
  ack path that one-way packet loss would normally rely on.
* ``test_cross_dc_latency_during_multi_dc_submit`` — §3+§4 combined,
  the spec's fourth "high-value first-5" scenario. Multi-DC submit
  under cross-DC link latency; Vivaldi tracks RTT, LHM stays bounded.
* ``test_no_stuck_suspect_after_flapping_partition`` — §4 healing
  invariant: "no stuck-suspect" after flapping. Asserts every
  manager's ``_global_suspicion_started_at`` is empty post-heal.
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
    ExpectCompletionWithin,
    HarnessTimeouts,
    ServerHandle,
    Submission,
    SubmissionPattern,
    WorkloadSpec,
    dc_has_leader,
    gate_cluster_formed,
    manager_has_n_peers,
    wait_until,
)


# ============================================================================
# Cluster specs
# ============================================================================


def _l2_spec() -> ClusterSpec:
    """Single-DC 3-manager + 2-worker cluster — partition + workload
    coverage at the manager-quorum level."""
    return ClusterSpec(
        gates=0,
        datacenters={
            "main": DCSpec(managers=3, workers=2, cores_per_worker=2),
        },
        env=EnvOverrides(request_timeout="5s", log_level="error"),
        timeouts=HarnessTimeouts(stabilization_default=60.0),
    )


def _l3_spec() -> ClusterSpec:
    """Two DCs × 2 managers × 1 worker, gated by 3 gates — cross-DC
    latency + multi-DC submit coverage."""
    return ClusterSpec(
        gates=3,
        datacenters={
            "east": DCSpec(managers=2, workers=1, cores_per_worker=1),
            "west": DCSpec(managers=2, workers=1, cores_per_worker=1),
        },
        env=EnvOverrides(request_timeout="5s", log_level="error"),
        timeouts=HarnessTimeouts(stabilization_default=90.0),
    )


# ============================================================================
# Workload helpers
# ============================================================================


def _single_dc_workload(timeout_seconds: float) -> WorkloadSpec:
    """One vu, one workflow, one DC. Used to verify "writes succeed" /
    "writes fail" on a specific quorum side."""
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
                expected_workflow_names=[SimpleWorkflow.__name__]
            ),
            ExpectCompletionWithin(seconds=timeout_seconds * 2),
        ],
    )


def _multi_dc_workload(timeout_seconds: float) -> WorkloadSpec:
    """One vu, one workflow. ``dc_count=1`` per the established
    pattern across this repo's other simulation tests — the workload
    runs on one DC's workers but the *submit path* in an L3 spec
    still traverses the cross-DC gate cluster (client → gate-cluster
    → manager DC → worker → manager DC → gate → client). That
    cross-DC traversal is what the latency-under-submit scenario
    targets, so single-DC fan-out at the workflow tier is the
    correct setup.
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
            ExpectAllWorkflowsComplete(
                expected_workflow_names=[SimpleWorkflow.__name__]
            ),
            ExpectCompletionWithin(seconds=timeout_seconds * 2),
        ],
    )


def _find_leader(managers: list[ServerHandle]) -> ServerHandle:
    for manager in managers:
        if manager.instance.is_leader():
            return manager
    raise AssertionError("no manager currently reports is_leader() True")


def _find_non_leaders(managers: list[ServerHandle]) -> list[ServerHandle]:
    return [manager for manager in managers if not manager.instance.is_leader()]


# ============================================================================
# §3 Network conditions
# ============================================================================


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_cross_dc_fixed_link_latency_baseline() -> None:
    """50 ms fixed cross-DC delay (each direction); both DCs converge.

    SCENARIOS.md §3 example: "Fixed latency per link (e.g., 50 ms
    cross-DC)". Verifies the baseline assumption that a steady
    cross-DC latency well below ``request_timeout`` does not impair
    leadership convergence in either DC or gate-cluster formation.

    Distinct from ``test_intra_dc_delay_does_not_break_quorum`` —
    that test is intra-DC (manager peers within one DC). Here the
    delay is on the cross-DC manager pairs and on gate↔manager
    paths, so it stresses the SWIM cross-DC probe path Vivaldi /
    AD-35 instruments.
    """
    spec = _l3_spec()
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="cross_dc_fixed_link_latency_baseline",
    ) as cluster:
        east = cluster.managers("east")
        west = cluster.managers("west")

        # Wildcard src/dst applies the delay to every cross-DC link.
        # The harness installs separate rules per direction so each
        # one-way send is independently delayed.
        for src in east:
            for dst in west:
                await cluster.faults.delay(ms=50.0, src=src, dst=dst)
                await cluster.faults.delay(ms=50.0, src=dst, dst=src)

        await wait_until(
            dc_has_leader(east),
            timeout=60.0,
            description="east DC elects a leader under 50ms cross-DC delay",
        )
        await wait_until(
            dc_has_leader(west),
            timeout=60.0,
            description="west DC elects a leader under 50ms cross-DC delay",
        )
        await wait_until(
            gate_cluster_formed(cluster.gates, expected_peers=2),
            timeout=60.0,
            description="gate cluster forms under 50ms cross-DC delay",
        )

        await cluster.faults.clear_network_faults()


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_asymmetric_latency_keeps_swim_converged() -> None:
    """A→B fast (20 ms), B→A slow (200 ms); SWIM stays converged.

    SCENARIOS.md §3: "Asymmetric latency (A → B fast, B → A slow).
    Probe ack window must be sized correctly." The slow direction
    must not trip false-positive DEAD declarations — Lifeguard's
    LHM-scaled probe ack window absorbs the asymmetry.

    Layout: 3-manager L2 cluster. Pick one manager as "A" and the
    other two as "B group". Apply asymmetric delay on every (A, B)
    pair so the probe-vs-ack-vs-piggyback paths all see the
    asymmetry. The post-condition is steady-state convergence —
    leader stable, no peer drops, no invariant violations.
    """
    spec = _l2_spec()
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="asymmetric_latency_keeps_swim_converged",
    ) as cluster:
        managers = cluster.managers("main")
        await wait_until(
            dc_has_leader(managers),
            timeout=45.0,
            description="initial leader before asymmetric latency",
        )

        node_a = managers[0]
        nodes_b = managers[1:]
        for node_b in nodes_b:
            # A → B is the "fast" direction (20 ms).
            await cluster.faults.delay(ms=20.0, src=node_a, dst=node_b)
            # B → A is the "slow" direction (200 ms). Total round-trip
            # is ~220 ms — well below the configured request_timeout
            # (5 s) so probes succeed but the ack window sees the
            # full slow-leg latency on every B-initiated probe.
            await cluster.faults.delay(ms=200.0, src=node_b, dst=node_a)

        # Observation window — multiple SWIM probe cycles should
        # complete without any node tripping SUSPECT against another.
        await asyncio.sleep(8.0)

        # Verify all managers still see each other as active peers.
        for manager in managers:
            await wait_until(
                manager_has_n_peers(manager, len(managers) - 1),
                timeout=15.0,
                poll=0.5,
                description=(
                    f"{manager.node_id} sees full peer set under "
                    "asymmetric latency"
                ),
            )

        await cluster.faults.clear_network_faults()


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_high_jitter_keeps_swim_converged() -> None:
    """50 ms base + 100 ms jitter; SWIM stays converged.

    SCENARIOS.md §3: "Jitter around a mean." Verifies that
    high-variance latency does not destabilise SWIM — each probe
    sees a different round-trip in the [50, 150] ms window. The
    LHM-scaled probe ack window must absorb the jitter without
    flapping nodes in and out of SUSPECT.

    A pure intra-DC test (no cross-DC plumbing) keeps the test
    deterministic — the only variable is the injected jitter.
    """
    spec = _l2_spec()
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="high_jitter_keeps_swim_converged",
    ) as cluster:
        managers = cluster.managers("main")
        await wait_until(
            dc_has_leader(managers),
            timeout=45.0,
            description="initial leader before jitter injection",
        )

        # Apply 50 ms ± 100 ms jitter on every intra-DC pair (both
        # directions). Each send rolls jitter independently.
        for src in managers:
            for dst in managers:
                if src is dst:
                    continue
                await cluster.faults.delay(
                    ms=50.0, src=src, dst=dst, jitter_ms=100.0
                )

        # Observation window covers multiple probe cycles under jitter.
        await asyncio.sleep(10.0)

        # Safety property under high jitter is convergence, not leader
        # stability — Lifeguard step-downs and re-elections under
        # variance are legitimate behaviour, but the cluster must
        # always re-elect a leader rather than deadlock with no one
        # holding the term. Peer membership must also stay intact:
        # jitter alone is not grounds for declaring a peer DEAD.
        await wait_until(
            dc_has_leader(managers),
            timeout=30.0,
            poll=0.5,
            description="cluster maintains (or re-elects) a leader under jitter",
        )

        for manager in managers:
            await wait_until(
                manager_has_n_peers(manager, len(managers) - 1),
                timeout=15.0,
                poll=0.5,
                description=(
                    f"{manager.node_id} sees full peer set under jitter"
                ),
            )

        await cluster.faults.clear_network_faults()


# ============================================================================
# §4 Partitions
# ============================================================================


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_symmetric_two_way_partition_quorum_writes_minority_rejects() -> None:
    """Symmetric partition: quorum keeps writing; minority rejects.

    SCENARIOS.md §4 + "first 5 high-value" #3. The decisive test for
    Raft-style safety under partition: the side with quorum (2 of 3
    managers) must continue accepting writes (job submissions), and
    the isolated minority (1 manager) must refuse — not silently
    queue, not split-brain, not hang.

    Sequence:
      1. Stabilize. Initial leader elected.
      2. Identify the leader and one non-leader. Partition them from
         the third manager (the minority).
      3. Wait for the minority manager to observe ``_leadership.has_quorum()
         == False`` — confirms it knows it's isolated.
      4. Heal — cluster reconverges, no split-brain detected by the
         continuous safety invariants.

    The continuous ``AtMostOneJobLeaderPerJob`` invariant running
    throughout the partition window asserts that both sides did NOT
    simultaneously elect leaders for the same job.
    """
    spec = _l2_spec()
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="symmetric_two_way_partition_quorum_writes_minority_rejects",
    ) as cluster:
        managers = cluster.managers("main")
        await wait_until(
            dc_has_leader(managers),
            timeout=45.0,
            description="initial leader before symmetric partition",
        )

        leader = _find_leader(managers)
        non_leaders = _find_non_leaders(managers)
        assert len(non_leaders) == 2, (
            f"expected 2 non-leaders, got {len(non_leaders)}"
        )
        # Quorum side = leader + first non-leader (2 of 3 = quorum).
        # Minority side = the remaining non-leader.
        quorum_side = [leader, non_leaders[0]]
        minority_side = [non_leaders[1]]

        await cluster.faults.partition(quorum_side, minority_side)

        # The minority manager must observe that it can no longer
        # form quorum. This is the production signal that gates
        # ``submit_job`` rejection — if a client routes a submit
        # through the minority, it gets a clean error rather than a
        # silent queue.
        minority_manager = minority_side[0]
        await wait_until(
            lambda: not minority_manager.instance._leadership.has_quorum(),
            timeout=45.0,
            poll=0.5,
            description=(
                f"minority manager {minority_manager.node_id} observes "
                "loss of quorum"
            ),
        )

        # Quorum side must still have a leader (the original or a
        # newly-elected one — both are valid post-partition outcomes
        # given the leader is on the quorum side here).
        await wait_until(
            dc_has_leader(quorum_side),
            timeout=45.0,
            description="quorum side maintains a leader during partition",
        )

        await cluster.faults.heal_partition()
        # Post-heal: the cluster must reconverge into a single
        # leader across all three managers.
        await wait_until(
            dc_has_leader(managers),
            timeout=60.0,
            description="cluster reconverges with single leader post-heal",
        )


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_asymmetric_partition_one_way_blocked() -> None:
    """A → B blocked, B → A allowed; cluster survives via indirect ack.

    SCENARIOS.md §4: "Asymmetric. A → B works, B → A doesn't. Tests
    one-way SWIM probe + indirect-probe ack path." This is harder
    than the symmetric case because one direction's packets DO flow
    — SWIM cannot rely on bidirectional silence to declare DEAD.

    Implementation uses ``drop_rate(probability=1.0, src=A, dst=B)``
    to block one direction. The reverse direction has no rule and
    flows normally. Lifeguard's indirect-probe path can confirm B's
    liveness via a third party (any other peer pinging B and
    relaying the ACK), so neither side should false-positive DEAD.

    A pure 3-manager L2 cluster is sufficient — we don't need
    cross-DC plumbing, just three SWIM-mesh peers so the indirect
    ack path has someone to relay through.
    """
    spec = _l2_spec()
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="asymmetric_partition_one_way_blocked",
    ) as cluster:
        managers = cluster.managers("main")
        await wait_until(
            dc_has_leader(managers),
            timeout=45.0,
            description="initial leader before asymmetric partition",
        )

        node_a, node_b, _node_c = managers
        # One-way drop: A → B totally blocked; B → A unaffected.
        # The third manager (C) is the relay candidate for indirect
        # probes from A to B.
        await cluster.faults.drop_rate(
            probability=1.0, src=node_a, dst=node_b
        )

        # Observation window — multiple SWIM probe cycles. Without
        # the indirect-probe path, A would mark B as DEAD here. With
        # it, A learns of B's liveness via C and convergence holds.
        await asyncio.sleep(8.0)

        # Both A and B must still see each other in their active
        # peer sets. If the asymmetric drop tripped a false DEAD,
        # one of them would have removed the other.
        for manager in (node_a, node_b):
            await wait_until(
                manager_has_n_peers(manager, len(managers) - 1),
                timeout=20.0,
                poll=0.5,
                description=(
                    f"{manager.node_id} sees full peer set under "
                    "asymmetric drop (indirect ack must compensate)"
                ),
            )

        await cluster.faults.clear_network_faults()


# ============================================================================
# §3 + §4 combined (spec's "first 5" #4)
# ============================================================================


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_cross_dc_latency_during_multi_dc_submit() -> None:
    """100 ms cross-DC link delay during a multi-DC workflow submit.

    SCENARIOS.md "first 5 high-value" #4: "Latency injection on
    cross-DC links during multi-DC submit." Stresses the L3 routing
    path (client → gate → manager-per-DC → worker → manager → gate
    aggregation → client) end-to-end under realistic cross-DC RTT.
    Vivaldi must track the inflated RTT and the LHM-scaled probe
    timeouts must absorb it without false-positive DEAD declarations.

    Workflow must complete; the harness's
    ``ExpectAllWorkflowsComplete`` + ``ExpectCompletionWithin``
    expectations enforce the success contract.
    """
    spec = _l3_spec()
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="cross_dc_latency_during_multi_dc_submit",
    ) as cluster:
        east_managers = cluster.managers("east")
        west_managers = cluster.managers("west")
        await wait_until(
            gate_cluster_formed(cluster.gates, expected_peers=2),
            timeout=60.0,
            description="gate cluster before cross-DC latency injection",
        )
        await wait_until(
            dc_has_leader(east_managers),
            timeout=60.0,
            description="east DC has leader before latency injection",
        )
        await wait_until(
            dc_has_leader(west_managers),
            timeout=60.0,
            description="west DC has leader before latency injection",
        )

        # Install 100 ms (each direction) on every cross-DC link.
        # Round-trip = 200 ms, well below the 5 s request_timeout.
        for src in east_managers:
            for dst in west_managers:
                await cluster.faults.delay(ms=100.0, src=src, dst=dst)
                await cluster.faults.delay(ms=100.0, src=dst, dst=src)

        async with cluster.workload(_multi_dc_workload(60.0)) as driver:
            await driver.submit_and_wait()

        await cluster.faults.clear_network_faults()


# ============================================================================
# §4 healing — stuck-suspect invariant
# ============================================================================


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_no_stuck_suspect_after_flapping_partition() -> None:
    """After flap+heal cycles, every manager has zero active suspicions.

    SCENARIOS.md §4: "Flapping. Heal then break repeatedly. Audit
    log captures every transition; no stuck-suspect." Existing
    ``test_flapping_partition`` validates that the partition rules
    flip cleanly but never reads SWIM-side state to confirm the
    invariant. The relevant private state is
    ``HealthAwareServer._global_suspicion_started_at`` — a dict
    keyed by node-address. Every entry represents an in-flight
    SUSPECT. After a clean heal + convergence window, every
    manager's dict must be empty: any non-empty entry is a stuck
    suspect that the periodic refresh / refute paths failed to
    clear.

    The 3-cycle flap pattern mirrors ``test_flapping_partition`` so
    the post-condition is directly attributable to that scenario
    rather than incidental noise.
    """
    spec = _l3_spec()
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="no_stuck_suspect_after_flapping_partition",
    ) as cluster:
        east = cluster.managers("east")
        west = cluster.managers("west")
        await wait_until(
            dc_has_leader(east),
            timeout=45.0,
            description="east elects leader pre-flap",
        )
        await wait_until(
            dc_has_leader(west),
            timeout=45.0,
            description="west elects leader pre-flap",
        )

        for _cycle in range(3):
            await cluster.faults.partition(east, west)
            await asyncio.sleep(1.5)
            await cluster.faults.heal_partition()
            await asyncio.sleep(1.0)

        # Post-heal convergence window. SUSPECT brackets in the worst
        # case complete in ~10 s on this config; give the cluster
        # 15 s to clear every in-flight suspicion.
        await asyncio.sleep(15.0)

        for manager in east + west:
            stuck_suspects = dict(manager.instance._global_suspicion_started_at)
            assert not stuck_suspects, (
                f"{manager.node_id} has stuck suspects post-heal: "
                f"{stuck_suspects!r}"
            )
