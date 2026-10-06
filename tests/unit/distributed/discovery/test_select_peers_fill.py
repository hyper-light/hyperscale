"""
AD-28: ``DiscoveryService.select_peers`` returns distinct peers, as many as
asked for and the service knows.

The backup fill of ``select_peers`` once built ``SelectionResult`` with
fields that never existed, unreachable until an unrelated fix made it run:
pinned for counts below, at and above the number of known peers.
"""

import pytest

from hyperscale.distributed.discovery import DiscoveryService
from hyperscale.distributed.discovery.models.discovery_config import DiscoveryConfig


def _service_with_seeds(seed_count: int) -> DiscoveryService:
    return DiscoveryService(
        DiscoveryConfig(
            cluster_id="test-cluster",
            environment_id="test",
            static_seeds=[f"10.0.0.{index}:9000" for index in range(1, seed_count + 1)],
        )
    )


@pytest.mark.parametrize("requested", [1, 3, 5, 8])
def test_select_peers_fills_with_distinct_peers_up_to_the_pool(requested: int) -> None:
    service = _service_with_seeds(5)

    results = service.select_peers("job-1", count=requested)

    peer_ids = [result.peer_id for result in results]
    assert len(peer_ids) == min(requested, 5)
    assert len(set(peer_ids)) == len(peer_ids)
