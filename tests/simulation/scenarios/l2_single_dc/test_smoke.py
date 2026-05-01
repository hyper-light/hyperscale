"""
L2 smoke scenario.

3-manager quorum + 2 workers in a single datacenter. Exercises:
- Manager peer discovery (each manager learns about its 2 peers).
- Worker registration (workers register with the seed-manager set).
- Multi-subprocess process tracking (2 workers × 2 cores = 4 subprocesses).
- Port-range cleanup at scale.
"""

import pytest

from tests.simulation.harness import (
    ClusterHarness,
    ClusterSpec,
    DCSpec,
    EnvOverrides,
    ExecutionMode,
)


def _l2_spec() -> ClusterSpec:
    return ClusterSpec(
        gates=0,
        datacenters={
            "main": DCSpec(managers=3, workers=2, cores_per_worker=2),
        },
        env=EnvOverrides(request_timeout="5s", log_level="error"),
        base_port=19200,
    )


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_l2_cluster_lifecycle() -> None:
    spec = _l2_spec()
    async with ClusterHarness(
        spec, mode=ExecutionMode.REAL, stabilization_seconds=12.0
    ) as cluster:
        managers = cluster.managers("main")
        workers = cluster.workers("main")
        assert len(managers) == 3
        assert len(workers) == 2

        # Each worker should have spun up `cores_per_worker` subprocesses by
        # now (12s stabilization > 1s pid-tick interval).
        for worker in workers:
            tracked = cluster.supervisor.tracked_pids(worker.node_id)
            assert len(tracked) >= 1, (
                f"worker {worker.node_id} has no tracked subprocesses; "
                f"got {tracked}"
            )

    assert cluster.supervisor.cleanup_errors == [], (
        f"cleanup reported errors: {cluster.supervisor.cleanup_errors}"
    )
