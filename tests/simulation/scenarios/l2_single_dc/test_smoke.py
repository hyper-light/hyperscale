"""
L2 smoke scenarios — single-datacenter quorum + worker pool.

Two scenarios at this level:

1. ``test_l2_framework_structure`` — exercises the harness pieces
   (spec, port reservation) without starting servers. Useful as a
   ``pytest -k structure`` quick gate even when full clusters are
   too heavy to run.

2. ``test_l2_cluster_lifecycle`` — full real-server stand-up of a
   3-manager quorum + 2 workers in one DC, condition-driven
   stabilization until all manager peers know about each other AND
   every worker has registered AND every worker subprocess pool is
   alive. Same continuous safety + liveness invariant ticking as
   L1.
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
)


def _l2_spec() -> ClusterSpec:
    return ClusterSpec(
        gates=0,
        datacenters={
            "main": DCSpec(managers=3, workers=2, cores_per_worker=2),
        },
        env=EnvOverrides(request_timeout="5s", log_level="error"),
        base_port=19200,
        timeouts=HarnessTimeouts(stabilization_default=60.0),
    )


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_l2_framework_structure() -> None:
    """A 3-manager, 2-worker spec spends ports in a contiguous range.

    3 manager TCP/UDP pairs (6) + 2 worker (TCP/UDP/derived) triples (6)
    validates the PortAllocator under realistic L2 load without
    actually starting any servers.
    """
    spec = _l2_spec()
    assert spec.total_node_count() == 5

    ports = PortAllocator(host=spec.host, base_port=spec.base_port)
    pairs = [ports.reserve_pair() for _ in range(3)]
    triples = [ports.reserve_range(3) for _ in range(2)]

    flat = [p for pair in pairs for p in pair] + [
        p for triple in triples for p in triple
    ]
    assert len(set(flat)) == len(flat), "all reserved ports must be unique"
    assert min(flat) >= spec.base_port


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_l2_cluster_lifecycle() -> None:
    """Full L2 cluster stand-up + condition-driven stabilization + tear-down.

    Stabilization predicates (built into the harness's ``_stabilize``):

    * each manager has discovered ``managers - 1`` active peers,
    * each manager has registered all ``workers`` workers, and
    * each worker's subprocess pool has spawned at least one tracked PID.

    Returns as soon as all hold; the harness's diagnostic dumper writes
    a snapshot if the budget is exhausted, so a regression in any of
    those predicates surfaces with named per-node detail rather than an
    opaque ``TimeoutError``.
    """
    spec = _l2_spec()
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        scenario_name="l2_cluster_lifecycle",
    ) as cluster:
        assert cluster.gates == []
        managers = cluster.managers("main")
        workers = cluster.workers("main")
        assert len(managers) == 3
        assert len(workers) == 2

        for manager in managers:
            assert manager.kind is ServerKind.MANAGER
            assert manager.started is True
            # Stabilization would not have returned without these.
            state = manager.instance._manager_state
            assert len(state.get_active_manager_peer_ids()) >= 2, (
                f"{manager.node_id} should know about its 2 peers"
            )
            assert state.get_worker_count() >= 2, (
                f"{manager.node_id} should have all 2 workers registered"
            )

        for worker in workers:
            assert worker.kind is ServerKind.WORKER
            assert worker.started is True
            assert cluster.supervisor.tracked_pids(worker.node_id), (
                f"{worker.node_id} subprocess pool should have spawned"
            )

    assert cluster.supervisor.cleanup_errors == [], (
        f"cleanup reported errors: {cluster.supervisor.cleanup_errors}"
    )
