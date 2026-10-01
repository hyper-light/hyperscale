"""
AD-28 sticky bindings honor PeerHealth's ordering (HEALTHY highest).

``health_degradation_threshold`` is documented as "evict bindings when
health reaches this level or worse", but all three comparisons ran
backwards: a successful probe (HEALTHY) EVICTED a peer's bindings while
UNHEALTHY / EVICTED peers kept theirs and were reported as healthy
sticky targets. DiscoveryService routes every record_success /
record_failure through update_peer_health, so each success broke
stickiness. Pinned for every health level, in both directions.
"""

import pytest

from hyperscale.distributed.discovery.models.peer_info import PeerHealth
from hyperscale.distributed.discovery.pool.sticky_connection import (
    StickyConfig,
    StickyConnectionManager,
)

AT_OR_WORSE_THAN_THRESHOLD = (PeerHealth.EVICTED, PeerHealth.UNHEALTHY, PeerHealth.DEGRADED)
BETTER_THAN_THRESHOLD = (PeerHealth.UNKNOWN, PeerHealth.HEALTHY)


def _bound_manager() -> StickyConnectionManager:
    manager = StickyConnectionManager(config=StickyConfig())
    manager.bind("job-1", "peer-1")
    return manager


@pytest.mark.parametrize("health", AT_OR_WORSE_THAN_THRESHOLD)
def test_unhealthy_peer_loses_its_bindings(health: PeerHealth) -> None:
    manager = _bound_manager()

    assert manager.update_peer_health("peer-1", health) == 1
    assert manager.get_binding("job-1") is None
    assert not manager.is_bound_healthy("job-1")


@pytest.mark.parametrize("health", BETTER_THAN_THRESHOLD)
def test_healthy_peer_keeps_its_bindings(health: PeerHealth) -> None:
    manager = _bound_manager()

    assert manager.update_peer_health("peer-1", health) == 0
    assert manager.get_binding("job-1") == "peer-1"
    assert manager.is_bound_healthy("job-1")


@pytest.mark.parametrize("health", AT_OR_WORSE_THAN_THRESHOLD)
def test_unhealthy_binding_is_not_reported_healthy_when_eviction_is_off(health: PeerHealth) -> None:
    manager = StickyConnectionManager(config=StickyConfig(evict_on_unhealthy=False))
    manager.bind("job-1", "peer-1")
    manager.update_peer_health("peer-1", health)

    assert not manager.is_bound_healthy("job-1")
    assert manager.get_stats()["unhealthy_bindings"] == 1
    assert manager.get_stats()["healthy_bindings"] == 0


# The sticky-hit branch of DiscoveryService.select_peer and the backup
# fill of select_peers were unreachable while the comparisons above ran
# backwards, so both built SelectionResult with fields that never existed
# (latency_estimate_ms, no candidates_considered): the first sticky hit
# after the ordering fix raised TypeError.


def _service_with_seeds(seed_count: int):
    from hyperscale.distributed.discovery import DiscoveryService
    from hyperscale.distributed.discovery.models.discovery_config import (
        DiscoveryConfig,
    )

    return DiscoveryService(
        DiscoveryConfig(
            cluster_id="test-cluster",
            environment_id="test",
            static_seeds=[f"10.0.0.{index}:9000" for index in range(1, seed_count + 1)],
        )
    )


def test_sticky_hit_returns_the_bound_peer() -> None:
    service = _service_with_seeds(3)

    first = service.select_peer("job-1")
    second = service.select_peer("job-1")

    assert first is not None and second is not None
    assert second.peer_id == first.peer_id
    assert second.was_load_balanced is False
    assert second.candidates_considered == 1


@pytest.mark.parametrize("requested", [1, 3, 5, 8])
def test_select_peers_fills_with_distinct_peers_up_to_the_pool(requested: int) -> None:
    service = _service_with_seeds(5)

    results = service.select_peers("job-1", count=requested)

    peer_ids = [result.peer_id for result in results]
    assert len(peer_ids) == min(requested, 5)
    assert len(set(peer_ids)) == len(peer_ids)
