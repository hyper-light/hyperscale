"""
L1 smoke scenario.

The simplest assertion the harness can make: build a one-manager,
one-worker cluster on real ports, verify it stood up, tear it down,
and confirm the supervisor reaped every artifact (subprocesses,
asyncio tasks, ports).

This single test exercises every guarantee the Phase 1 foundation makes.
If it passes, the harness is real.
"""

import pytest

from tests.simulation.harness import (
    ClusterHarness,
    ClusterSpec,
    DCSpec,
    EnvOverrides,
    ExecutionMode,
    ServerKind,
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
async def test_l1_cluster_lifecycle(stabilization_seconds: float) -> None:
    """Stand up L1, verify topology, tear down, assert clean cleanup."""
    spec = _l1_spec()
    async with ClusterHarness(
        spec,
        mode=ExecutionMode.REAL,
        stabilization_seconds=stabilization_seconds,
    ) as cluster:
        # Topology accessors return what the spec requested.
        assert cluster.gates == []
        assert len(cluster.managers("local")) == 1
        assert len(cluster.workers("local")) == 1

        manager = cluster.managers("local")[0]
        worker = cluster.workers("local")[0]

        assert manager.kind is ServerKind.MANAGER
        assert worker.kind is ServerKind.WORKER
        assert manager.started is True
        assert worker.started is True

        # The supervisor should have observed at least one worker subprocess.
        # The 1 s pid-tracking tick may not have fired yet at very short
        # stabilization windows, so accept zero only if the stabilization
        # budget is below the tick interval.
        tracked = cluster.supervisor.tracked_pids(worker.node_id)
        if stabilization_seconds >= 1.5:
            assert tracked, (
                "expected supervisor to have snapshotted worker subprocess "
                f"PIDs after {stabilization_seconds}s; got empty set"
            )

    # __aexit__ ran cleanly. The supervisor's cleanup_errors should be empty;
    # if not, __aexit__ would have raised. Belt-and-suspenders below.
    assert cluster.supervisor.cleanup_errors == [], (
        f"cleanup reported errors: {cluster.supervisor.cleanup_errors}"
    )
