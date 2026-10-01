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
