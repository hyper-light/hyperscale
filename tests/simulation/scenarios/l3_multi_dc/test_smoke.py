"""
L3 smoke scenarios.

Currently only the framework-structure test runs; the full lifecycle test
is blocked on the same worker-startup hang that affects L1/L2. See L1 smoke
docstring and docs/dev/simulation_framework.md §18.
"""

import pytest

from tests.simulation.harness import (
    ClusterSpec,
    DCSpec,
    EnvOverrides,
    PortAllocator,
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
async def test_l3_framework_structure() -> None:
    """L3 spec composes gates + per-DC managers and workers correctly."""
    spec = _l3_spec()

    # 3 gates + 2 DCs × (2 managers + 1 worker) = 3 + 4 + 2 = 9 nodes
    assert spec.total_node_count() == 9
    assert spec.gates == 3
    assert set(spec.datacenters.keys()) == {"east", "west"}

    # Each DC carries the same shape under this spec.
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
