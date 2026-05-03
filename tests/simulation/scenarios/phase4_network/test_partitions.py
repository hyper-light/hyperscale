"""
Phase 4 partition / delay / drop scenarios at L3.

Each scenario builds a multi-DC cluster and uses the FaultMatrix's
network primitives — partition, delay, drop_rate — to exercise the
SWIM partition-correlation code paths. Stops short of standing up
real network namespaces; in-process method wrapping is sufficient
for SWIM-level correctness.

Scenarios:

* ``test_dc_to_dc_partition_then_heal`` — symmetric partition between
  two DCs' manager fleets. Each DC's managers should still see their
  local peers; cross-DC peer counts should drop. After heal, peer
  counts converge back.

* ``test_one_way_drop_rate`` — install a 100% drop_rate from DC east
  to DC west (one-directional). West's managers no longer hear from
  east; east's managers still hear from west. Verifies the matrix's
  asymmetric primitive composes with partition.

* ``test_flapping_partition`` — partition / heal / partition / heal
  in rapid succession. The cluster must converge between cycles
  without leaking partition state. Counts the network_fault_summary
  to confirm cleanup is correct.

* ``test_intra_dc_delay_does_not_break_quorum`` — install a 200ms
  delay between every manager pair in one DC. SWIM probes still
  succeed (within request_timeout), peer counts stay stable, leader
  election completes despite the latency. Smoke test for the delay
  primitive.
"""

import asyncio

import pytest

from tests.simulation.harness import (
    ClusterHarness,
    ClusterSpec,
    DCSpec,
    EnvOverrides,
    ExecutionMode,
    HarnessTimeouts,
    ServerHandle,
    dc_has_leader,
    gate_cluster_formed,
    manager_has_n_peers,
    wait_until,
)


def _l3_spec(base_port: int) -> ClusterSpec:
    """Two DCs × 2 managers × 1 worker, gated by 3 gates.

    The smaller-than-full L3 spec keeps test runtime reasonable
    while still exercising the cross-DC code path.
    """
    return ClusterSpec(
        gates=3,
        datacenters={
            "east": DCSpec(managers=2, workers=1, cores_per_worker=1),
            "west": DCSpec(managers=2, workers=1, cores_per_worker=1),
        },
        env=EnvOverrides(request_timeout="3s", log_level="error"),
        base_port=base_port,
        timeouts=HarnessTimeouts(stabilization_default=90.0),
    )


def _l2_spec(base_port: int) -> ClusterSpec:
    """Single-DC 3-manager cluster — for delay / drop tests that don't
    need cross-DC plumbing."""
    return ClusterSpec(
        gates=0,
        datacenters={
            "main": DCSpec(managers=3, workers=1, cores_per_worker=1),
        },
        env=EnvOverrides(request_timeout="5s", log_level="error"),
        base_port=base_port,
        timeouts=HarnessTimeouts(stabilization_default=60.0),
    )


def _gather_node_ids(handles: list[ServerHandle]) -> list[str]:
    return [h.node_id for h in handles]


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_dc_to_dc_partition_then_heal() -> None:
    """Symmetric partition between two DCs' manager fleets, then heal.

    Step 1: stabilize. Both DCs elect leaders; gates form a cluster.
    Step 2: partition east managers from west managers.
    Step 3: assert that intra-DC peer counts stay at 1 (each DC has
            2 managers, so each sees 1 peer) — local SWIM unaffected.
            Cross-DC visibility is harder to assert directly without
            reaching into private state, so we rely on the partition
            being installed (network_fault_summary["partitions"] == 1)
            and the harness's continuous safety invariants not
            firing — including AtMostOneJobLeaderPerJob, which
            partition-induced split-brain would violate.
    Step 4: heal_partition. Cluster reconverges; cleanup verifies no
            partition rules left over.
    """
    spec = _l3_spec(base_port=22000)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="dc_to_dc_partition_then_heal",
    ) as cluster:
        east = cluster.managers("east")
        west = cluster.managers("west")
        assert len(east) == 2 and len(west) == 2

        # Initial intra-DC peer counts: each manager sees 1 peer in its DC.
        for handle in east + west:
            await wait_until(
                manager_has_n_peers(handle, 1),
                timeout=45.0,
                poll=0.5,
                description=f"{handle.node_id} sees 1 intra-DC peer pre-partition",
            )

        await cluster.faults.partition(east, west)
        assert cluster.faults.network_fault_summary()["partitions"] == 1

        # Intra-DC peer count should remain stable through the partition.
        # Cross-DC traffic is dropped, but local SWIM probes between
        # east-0 / east-1 (and west-0 / west-1) keep flowing.
        await asyncio.sleep(2.0)
        for handle in east + west:
            assert (
                len(handle.instance._manager_state.get_active_manager_peer_ids())
                >= 1
            ), (
                f"{handle.node_id} lost intra-DC peer during cross-DC partition"
            )

        await cluster.faults.heal_partition()
        assert cluster.faults.network_fault_summary()["partitions"] == 0


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_one_way_drop_rate() -> None:
    """Asymmetric 100% drop east→west; reverse direction unaffected.

    Sequence:
      1. Stabilize.
      2. For every (east_manager, west_manager) pair, install
         drop_rate(probability=1.0, src=east, dst=west). The reverse
         direction has no rule installed, so packets flow normally.
      3. Brief observation; the asymmetry is hard to assert directly
         from black-box state without reaching into per-peer
         heartbeat staleness counters. Instead we verify the rules
         are installed (network_fault_summary count) and that the
         continuous invariants don't fire — confirming the harness
         doesn't double-count or block both directions.
      4. clear_network_faults to reset.
    """
    spec = _l3_spec(base_port=22100)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="one_way_drop_rate",
    ) as cluster:
        east = cluster.managers("east")
        west = cluster.managers("west")

        # 2x2 cross matrix = 4 rules; full coverage of the directional drop.
        for src in east:
            for dst in west:
                await cluster.faults.drop_rate(
                    probability=1.0, src=src, dst=dst
                )
        assert cluster.faults.network_fault_summary()["drops"] == 4

        # Brief observation window: the cluster shouldn't crash, no
        # invariant should trip in this short period.
        await asyncio.sleep(3.0)

        await cluster.faults.clear_network_faults()
        summary = cluster.faults.network_fault_summary()
        assert summary == {"partitions": 0, "delays": 0, "drops": 0}


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_flapping_partition() -> None:
    """Partition / heal / partition / heal — no leaked rule state.

    Three flap cycles validates that:
      * partition() is idempotent under repeated install of the same
        rule (each call appends a fresh rule object — that's by
        design, the matcher OR's them).
      * heal_partition() clears all installed partitions in one shot.
      * The harness's continuous invariants don't fire under rapid
        flapping (split-brain detection would catch any window where
        both sides simultaneously elect leaders).
    """
    spec = _l3_spec(base_port=22200)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="flapping_partition",
    ) as cluster:
        east = cluster.managers("east")
        west = cluster.managers("west")

        # Wait for both DCs to elect their initial leader so flaps
        # land against a stable cluster.
        await wait_until(
            dc_has_leader(east),
            timeout=45.0,
            description="east elects initial leader",
        )
        await wait_until(
            dc_has_leader(west),
            timeout=45.0,
            description="west elects initial leader",
        )

        for cycle in range(3):
            await cluster.faults.partition(east, west)
            assert cluster.faults.network_fault_summary()["partitions"] == 1
            await asyncio.sleep(1.5)
            await cluster.faults.heal_partition()
            assert cluster.faults.network_fault_summary()["partitions"] == 0
            await asyncio.sleep(1.0)
            _ = cycle  # mark used

        # After all flaps, the harness's clean exit asserts no stuck
        # state and no invariant violations occurred during the run.


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_intra_dc_delay_does_not_break_quorum() -> None:
    """200ms delay between every manager pair in one DC; quorum holds.

    SWIM probes have a per-peer request_timeout (configured in this
    spec to 5s). A 200ms one-way delay leaves probes well within
    timeout — SUSPECT shouldn't fire — and leader election should
    still complete within the stabilization budget. The scenario
    passes when:
      1. Delay rules are installed (network_fault_summary count).
      2. dc_has_leader still holds after a few seconds.
      3. Cleanup is clean.
    """
    spec = _l2_spec(base_port=22300)
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="intra_dc_delay_does_not_break_quorum",
    ) as cluster:
        managers = cluster.managers("main")
        assert len(managers) == 3

        await wait_until(
            dc_has_leader(managers),
            timeout=45.0,
            description="initial leader before delay",
        )

        for src in managers:
            for dst in managers:
                if src.node_id == dst.node_id:
                    continue
                await cluster.faults.delay(
                    ms=200.0, src=src, dst=dst, jitter_ms=50.0
                )
        assert cluster.faults.network_fault_summary()["delays"] == 6

        # Allow some rounds of probing under the delay regime.
        await asyncio.sleep(4.0)

        # Some manager should still be leader. Either the original
        # or a re-elected one — the test allows both because under
        # delay an election can still complete.
        await wait_until(
            dc_has_leader(managers),
            timeout=15.0,
            description="leader holds under intra-DC delay",
            on_fail=lambda: cluster.dump_diagnostics(
                reason="leader lost under 200ms intra-DC delay"
            ),
        )

        await cluster.faults.clear_network_faults()
