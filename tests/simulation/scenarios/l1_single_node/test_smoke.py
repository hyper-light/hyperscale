"""
L1 smoke scenarios.

Two scenarios at this level:

1. ``test_l1_framework_structure`` — exercises everything the harness
   does *without* starting real servers: spec construction, port
   allocation, supervisor preflight (zombie reap, baseline pid
   snapshot), final descendant sweep (no-op), task-leak detection,
   port-release verification.

2. ``test_l1_cluster_lifecycle`` — full real-server stand-up + tear-down.
   Stabilization is condition-driven (``wait_until`` over manager
   peer/worker counts and worker subprocess presence) rather than a
   flat sleep, so the test returns as soon as the cluster is actually
   ready. Continuous safety + liveness invariants tick the entire
   time.
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
    Supervisor,
)


def _l1_spec() -> ClusterSpec:
    return ClusterSpec(
        gates=0,
        datacenters={
            "local": DCSpec(managers=1, workers=1, cores_per_worker=2),
        },
        env=EnvOverrides(request_timeout="5s", log_level="error"),
        base_port=19000,
        timeouts=HarnessTimeouts(stabilization_default=45.0),
    )


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_l1_framework_structure() -> None:
    """The harness owns its lifetime even with zero servers running."""
    spec = _l1_spec()

    assert spec.total_node_count() == 2
    assert spec.gates == 0
    assert "local" in spec.datacenters

    ports = PortAllocator(host=spec.host, base_port=spec.base_port)
    pair = ports.reserve_pair()
    assert pair[0] != pair[1]
    assert all(port >= spec.base_port for port in pair)

    next_ports = PortAllocator(host=spec.host, base_port=spec.base_port)
    next_pair = next_ports.reserve_pair()
    assert set(pair).isdisjoint(next_pair), (
        "a new allocator in the same process must not immediately reuse "
        "recently retired harness ports"
    )

    supervisor = Supervisor(timeouts=HarnessTimeouts(), ports=ports)
    async with supervisor as sup:
        assert sup.run_id, "supervisor must mint a run_id at __aenter__"
        assert sup.server_handles == []
    assert supervisor.cleanup_errors == [], (
        f"unexpected cleanup errors: {supervisor.cleanup_errors}"
    )

    held = await ports.verify_all_released(settle_seconds=0.1)
    assert held == [], f"ports unexpectedly held: {held}"
    held = await next_ports.verify_all_released(settle_seconds=0.1)
    assert held == [], f"ports unexpectedly held: {held}"


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_l1_cluster_lifecycle() -> None:
    """Full real-server stand-up and condition-driven stabilization.

    Returns as soon as the manager has registered the worker AND the
    worker has spawned its subprocess pool — no fixed sleep. Continuous
    safety + liveness invariants run on a 100 ms tick throughout.
    """
    spec = _l1_spec()
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="l1_cluster_lifecycle",
    ) as cluster:
        assert cluster.gates == []
        assert len(cluster.managers("local")) == 1
        assert len(cluster.workers("local")) == 1

        manager = cluster.managers("local")[0]
        worker = cluster.workers("local")[0]

        assert manager.kind is ServerKind.MANAGER
        assert worker.kind is ServerKind.WORKER
        assert manager.started is True
        assert worker.started is True

        # `_stabilize` would not have returned without these holding;
        # double-check explicitly so a future regression in the
        # stabilization predicates is loud.
        assert cluster.supervisor.tracked_pids(worker.node_id), (
            "stabilization should have observed at least one worker subprocess"
        )
        assert manager.instance._manager_state.get_worker_count() >= 1

    assert cluster.supervisor.cleanup_errors == [], (
        f"cleanup reported errors: {cluster.supervisor.cleanup_errors}"
    )
