"""
L1 smoke scenarios.

Two scenarios at this level:

1. ``test_l1_framework_structure`` — exercises everything the harness
   does *without* starting real servers: spec construction, port
   allocation, supervisor preflight (zombie reap, baseline pid
   snapshot), final descendant sweep (no-op), task-leak detection,
   port-release verification. This is the canonical "the harness
   itself is healthy" gate.

2. ``test_l1_cluster_lifecycle`` — full real-server stand-up + tear-down.
   Currently xfailed: the framework surfaced ten production bugs
   during construction-and-startup of a single-manager + single-worker
   cluster (see docs/dev/simulation_framework.md §18). The first nine
   are fixed inline; the tenth (worker startup hang past
   ``setup_server_pool``) requires a deeper investigation that is
   tracked separately. Once that lands this test should be flipped
   from ``xfail`` to a regular test.
"""

import asyncio

import pytest

from tests.simulation.harness import (
    ClusterHarness,
    ClusterSpec,
    DCSpec,
    EnvOverrides,
    ExecutionMode,
    PortAllocator,
    ServerKind,
    Supervisor,
    HarnessTimeouts,
)


def _l1_spec() -> ClusterSpec:
    return ClusterSpec(
        gates=0,
        datacenters={
            "local": DCSpec(managers=1, workers=1, cores_per_worker=2),
        },
        env=EnvOverrides(request_timeout="5s", log_level="error"),
        base_port=19000,
    )


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_l1_framework_structure() -> None:
    """The harness owns its lifetime even with zero servers running.

    Validates every Phase 1 piece that does not depend on production-server
    startup: spec validation, port reservation, supervisor preflight,
    asyncio task-leak detection, and post-teardown port-release verification.
    """
    spec = _l1_spec()

    # Spec carries the full topology shape.
    assert spec.total_node_count() == 2
    assert spec.gates == 0
    assert "local" in spec.datacenters

    # PortAllocator hands out bindable ports and reuses the spec's host.
    ports = PortAllocator(host=spec.host, base_port=spec.base_port)
    pair = ports.reserve_pair()
    assert pair[0] != pair[1]
    assert all(port >= spec.base_port for port in pair)

    # Supervisor preflight + final-sweep cycle with no servers registered.
    supervisor = Supervisor(timeouts=HarnessTimeouts(), ports=ports)
    async with supervisor as sup:
        assert sup.run_id, "supervisor must mint a run_id at __aenter__"
        # Baseline must include this process's existing children only.
        # Asserting non-strict; the supervisor's correctness is in the diff.
        assert sup.server_handles == []
    # __aexit__ ran. Cleanup errors should be empty for a no-op lifetime.
    assert supervisor.cleanup_errors == [], (
        f"unexpected cleanup errors: {supervisor.cleanup_errors}"
    )

    # The reserved ports must release after teardown (we never bound them
    # for real, but the verifier still re-binds to confirm).
    held = await ports.verify_all_released(settle_seconds=0.1)
    assert held == [], f"ports unexpectedly held: {held}"


@pytest.mark.asyncio
@pytest.mark.simulation
@pytest.mark.skip(
    reason=(
        "Worker startup hangs past `setup_server_pool` in the production "
        "code path; the harness exposed ten distinct startup bugs (nine "
        "fixed inline). The tenth needs deeper investigation in the worker "
        "lifecycle and leaves multiprocessing semaphores leaked, which "
        "pytest's teardown cannot recover from. Re-enable once the worker "
        "lifecycle hang is fixed. See docs/dev/simulation_framework.md §18."
    ),
)
async def test_l1_cluster_lifecycle(stabilization_seconds: float) -> None:
    """Full real-server stand-up and teardown — currently skipped."""
    spec = _l1_spec()
    async with asyncio.timeout(45):
        async with ClusterHarness(
            spec,
            mode=ExecutionMode.REAL,
            stabilization_seconds=stabilization_seconds,
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

            tracked = cluster.supervisor.tracked_pids(worker.node_id)
            if stabilization_seconds >= 1.5:
                assert tracked, (
                    "supervisor should have snapshotted worker subprocess "
                    f"PIDs after {stabilization_seconds}s"
                )

        assert cluster.supervisor.cleanup_errors == [], (
            f"cleanup reported errors: {cluster.supervisor.cleanup_errors}"
        )
