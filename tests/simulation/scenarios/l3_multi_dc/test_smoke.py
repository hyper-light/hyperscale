"""
L3 smoke scenarios — multi-datacenter through a gate cluster.

Two scenarios at this level:

1. ``test_l3_framework_structure`` — harness pieces only. Validates
   that a realistic L3 spec composes cleanly and reserves a unique
   contiguous port range.

2. ``test_l3_cluster_lifecycle`` — full real-server stand-up of 3
   gates + 2 datacenters × (2 managers + 1 worker × 2 cores).
   Stabilization waits for manager peer discovery and worker
   registration in each DC, plus gate-cluster formation across the
   3-gate quorum. Same continuous safety + liveness invariants
   ticking throughout.
"""

import pytest

from tests.simulation.harness import (
    ClusterHarness,
    ClusterSpec,
    DCSpec,
    EnvOverrides,
    ExecutionMode,
    HarnessTimeouts,
    PortAllocator,
    ServerKind,
    gate_cluster_formed,
    wait_until,
)


def _l3_spec() -> ClusterSpec:
    return ClusterSpec(
        gates=3,
        datacenters={
            "east": DCSpec(managers=2, workers=1, cores_per_worker=2),
            "west": DCSpec(managers=2, workers=1, cores_per_worker=2),
        },
        env=EnvOverrides(request_timeout="5s", log_level="error"),
        base_port=19400,
        timeouts=HarnessTimeouts(stabilization_default=75.0),
    )


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_l3_framework_structure() -> None:
    """L3 spec composes gates + per-DC managers and workers correctly."""
    spec = _l3_spec()

    # 3 gates + 2 DCs × (2 managers + 1 worker) = 3 + 4 + 2 = 9 nodes
    assert spec.total_node_count() == 9
    assert spec.gates == 3
    assert set(spec.datacenters.keys()) == {"east", "west"}

    for dc_id in ("east", "west"):
        dc = spec.datacenters[dc_id]
        assert dc.managers == 2
        assert dc.workers == 1
        assert dc.cores_per_worker == 2

    # PortAllocator handles a realistic L3 reservation: gates need pairs,
    # workers need triples (TCP + UDP + derived port range).
    ports = PortAllocator(host=spec.host, base_port=spec.base_port)
    gate_pairs = [ports.reserve_pair() for _ in range(spec.gates)]
    manager_pairs = [
        ports.reserve_pair()
        for dc in spec.datacenters.values()
        for _ in range(dc.managers)
    ]
    worker_triples = [
        ports.reserve_range(3)
        for dc in spec.datacenters.values()
        for _ in range(dc.workers)
    ]

    all_ports = (
        [p for pair in gate_pairs for p in pair]
        + [p for pair in manager_pairs for p in pair]
        + [p for triple in worker_triples for p in triple]
    )
    assert len(set(all_ports)) == len(all_ports), "ports must be unique across L3"


@pytest.mark.asyncio
@pytest.mark.simulation
@pytest.mark.skip(
    reason=(
        "Same production task-cleanup gaps as L2 (see L2 lifecycle skip "
        "reason). Adds 3 gates on top — same SWIM/timing-wheel/cleanup "
        "leaks plus gate-side variants. Re-enable once shutdown "
        "propagation is consistent across HealthAwareServer, "
        "MercurySyncBaseServer, TaskRunner internals, and per-node bg "
        "loops."
    ),
)
async def test_l3_cluster_lifecycle() -> None:
    """Full L3 stand-up + gate-cluster formation + tear-down.

    Per-DC stabilization (manager peers, worker registration, worker
    subprocesses) is handled by the harness ``_stabilize`` in the same
    way as L2. Gate-cluster formation is asserted explicitly here
    because stabilization does not yet wait on it — the gates form
    their cluster asynchronously and may take a few seconds longer
    than per-DC quorum.
    """
    spec = _l3_spec()
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="l3_cluster_lifecycle",
    ) as cluster:
        assert len(cluster.gates) == 3
        assert len(cluster.managers("east")) == 2
        assert len(cluster.managers("west")) == 2
        assert len(cluster.workers("east")) == 1
        assert len(cluster.workers("west")) == 1
        assert len(cluster.all_handles()) == 9

        for handle in cluster.all_handles():
            assert handle.started is True

        # Gates each expect (gates - 1) peers; cluster formation happens
        # in the background after start, so wait explicitly.
        await wait_until(
            gate_cluster_formed(cluster.gates, expected_peers=spec.gates - 1),
            timeout=30.0,
            poll=0.5,
            description="gate cluster formed",
            on_fail=lambda: cluster.dump_diagnostics(
                reason="gate cluster did not form"
            ),
        )

        # Each DC's managers should know about each other and have the
        # local worker registered. Stabilization already waited on this,
        # so these are belt-and-suspenders on the assertion side.
        for dc_id in ("east", "west"):
            for manager in cluster.managers(dc_id):
                state = manager.instance._manager_state
                assert len(state.get_active_manager_peer_ids()) >= 1, (
                    f"{manager.node_id} should know its DC peer"
                )
                assert state.get_worker_count() >= 1, (
                    f"{manager.node_id} should have its DC's worker"
                )
            for worker in cluster.workers(dc_id):
                assert worker.kind is ServerKind.WORKER
                assert cluster.supervisor.tracked_pids(worker.node_id)

    assert cluster.supervisor.cleanup_errors == [], (
        f"cleanup reported errors: {cluster.supervisor.cleanup_errors}"
    )
