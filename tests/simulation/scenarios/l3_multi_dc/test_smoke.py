"""
L3 smoke scenario.

3 gates + 2 datacenters × (2 managers + 1 worker × 2 cores). The minimum
configuration that still exercises gate clustering, cross-DC manager
addressing, and per-DC worker pools.

Larger than the existing `test_gate_cross_dc_dispatch.py` setup is
deliberately not the target here — that's the full failover scenario.
This is the smoke test that proves the harness assembles the full L3
topology and reaps it cleanly.
"""

import pytest

from tests.simulation.harness import (
    ClusterHarness,
    ClusterSpec,
    DCSpec,
    EnvOverrides,
    ExecutionMode,
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
    )


@pytest.mark.asyncio
@pytest.mark.simulation
async def test_l3_cluster_lifecycle() -> None:
    spec = _l3_spec()
    async with ClusterHarness(
        spec, mode=ExecutionMode.REAL, stabilization_seconds=15.0
    ) as cluster:
        assert len(cluster.gates) == 3
        assert len(cluster.managers("east")) == 2
        assert len(cluster.managers("west")) == 2
        assert len(cluster.workers("east")) == 1
        assert len(cluster.workers("west")) == 1

        # Total handles registered: 3 gates + 4 managers + 2 workers = 9.
        assert len(cluster.all_handles()) == 9

    assert cluster.supervisor.cleanup_errors == [], (
        f"cleanup reported errors: {cluster.supervisor.cleanup_errors}"
    )
