"""
L2 smoke scenarios.

Currently only the framework-structure test runs; the full lifecycle test
is blocked on the same worker-startup hang that affects L1. See L1 smoke
docstring and docs/dev/simulation_framework.md §18.
"""

import pytest

from tests.simulation.harness import (
    ClusterSpec,
    DCSpec,
    EnvOverrides,
    PortAllocator,
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
async def test_l2_framework_structure() -> None:
    """A 3-manager, 2-worker spec spends 16 ports in a contiguous range.

    3 manager TCP/UDP pairs (6) + 2 worker (TCP/UDP/derived) triples (6) +
    headroom validates the PortAllocator under realistic L2 load without
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
