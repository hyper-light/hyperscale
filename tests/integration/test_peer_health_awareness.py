"""
Integration tests for PeerHealthAwareness (Phase 6.2).

Tests peer health tracking and SWIM behavior adaptation including:
- PeerHealthInfo creation and staleness detection
- PeerHealthAwareness health update processing
- Timeout adaptation based on peer load
- Proxy filtering for indirect probes
- Gossip reduction factors
- Callback integration for state transitions
- Concurrency handling
- Edge cases and failure paths
"""

import asyncio
import time
from unittest.mock import MagicMock

import pytest

from hyperscale.distributed_rewrite.health.tracker import HealthPiggyback
from hyperscale.distributed_rewrite.swim.health.peer_health_awareness import (
    PeerHealthAwareness,
    PeerHealthAwarenessConfig,
    PeerHealthInfo,
    PeerLoadLevel,
)


# =============================================================================
# PeerHealthInfo Tests
# =============================================================================


class TestPeerHealthInfo:
    """Test PeerHealthInfo creation and properties."""

    def test_from_piggyback_healthy(self) -> None:
        """Test creating PeerHealthInfo from healthy piggyback."""
        piggyback = HealthPiggyback(
            node_id="worker-1",
            node_type="worker",
            overload_state="healthy",
            accepting_work=True,
            capacity=8,
            throughput=10.0,
            expected_throughput=15.0,
            timestamp=time.monotonic(),
        )

        info = PeerHealthInfo.from_piggyback(piggyback)

        assert info.node_id == "worker-1"
        assert info.load_level == PeerLoadLevel.HEALTHY
        assert info.accepting_work is True
        assert info.capacity == 8

    def test_from_piggyback_overloaded(self) -> None:
        """Test creating PeerHealthInfo from overloaded piggyback."""
        piggyback = HealthPiggyback(
            node_id="worker-1",
            node_type="worker",
            overload_state="overloaded",
            accepting_work=False,
            capacity=0,
            timestamp=time.monotonic(),
        )

        info = PeerHealthInfo.from_piggyback(piggyback)

        assert info.load_level == PeerLoadLevel.OVERLOADED
        assert info.is_overloaded is True
        assert info.is_stressed is True
        assert info.is_healthy is False

    def test_from_piggyback_all_states(self) -> None:
        """Test load level mapping for all overload states."""
        state_to_level = {
            "healthy": PeerLoadLevel.HEALTHY,
            "busy": PeerLoadLevel.BUSY,
            "stressed": PeerLoadLevel.STRESSED,
            "overloaded": PeerLoadLevel.OVERLOADED,
        }

        for state, expected_level in state_to_level.items():
            piggyback = HealthPiggyback(
                node_id=f"node-{state}",
                node_type="worker",
                overload_state=state,
                timestamp=time.monotonic(),
            )
            info = PeerHealthInfo.from_piggyback(piggyback)
            assert info.load_level == expected_level

    def test_from_piggyback_unknown_state(self) -> None:
        """Test unknown overload state maps to UNKNOWN."""
        piggyback = HealthPiggyback(
            node_id="node-1",
            node_type="worker",
            overload_state="unknown_state",
            timestamp=time.monotonic(),
        )

        info = PeerHealthInfo.from_piggyback(piggyback)
        assert info.load_level == PeerLoadLevel.UNKNOWN

    def test_is_stale_fresh(self) -> None:
        """Test that fresh info is not stale."""
        piggyback = HealthPiggyback(
            node_id="node-1",
            node_type="worker",
            timestamp=time.monotonic(),
        )
        info = PeerHealthInfo.from_piggyback(piggyback)

        assert info.is_stale(max_age_seconds=30.0) is False

    def test_is_stale_old(self) -> None:
        """Test that old info is stale."""
        piggyback = HealthPiggyback(
            node_id="node-1",
            node_type="worker",
            timestamp=time.monotonic(),
        )
        info = PeerHealthInfo.from_piggyback(piggyback)
        # Manually backdate
        info.last_update = time.monotonic() - 60.0

        assert info.is_stale(max_age_seconds=30.0) is True


class TestPeerLoadLevelOrdering:
    """Test PeerLoadLevel ordering."""

    def test_level_ordering(self) -> None:
        """Test that load levels are properly ordered."""
        assert PeerLoadLevel.UNKNOWN < PeerLoadLevel.HEALTHY
        assert PeerLoadLevel.HEALTHY < PeerLoadLevel.BUSY
        assert PeerLoadLevel.BUSY < PeerLoadLevel.STRESSED
        assert PeerLoadLevel.STRESSED < PeerLoadLevel.OVERLOADED


# =============================================================================
# PeerHealthAwareness Basic Tests
# =============================================================================


class TestPeerHealthAwarenessBasic:
    """Test basic PeerHealthAwareness operations."""

    def test_on_health_update(self) -> None:
        """Test processing a health update."""
        awareness = PeerHealthAwareness()

        health = HealthPiggyback(
            node_id="worker-1",
            node_type="worker",
            overload_state="stressed",
            capacity=4,
            timestamp=time.monotonic(),
        )
        awareness.on_health_update(health)

        peer_info = awareness.get_peer_info("worker-1")
        assert peer_info is not None
        assert peer_info.load_level == PeerLoadLevel.STRESSED

    def test_on_health_update_replaces_old(self) -> None:
        """Test that newer updates replace older ones."""
        awareness = PeerHealthAwareness()

        # First update: healthy
        health1 = HealthPiggyback(
            node_id="worker-1",
            node_type="worker",
            overload_state="healthy",
            timestamp=time.monotonic(),
        )
        awareness.on_health_update(health1)

        # Second update: overloaded
        health2 = HealthPiggyback(
            node_id="worker-1",
            node_type="worker",
            overload_state="overloaded",
            timestamp=time.monotonic(),
        )
        awareness.on_health_update(health2)

        peer_info = awareness.get_peer_info("worker-1")
        assert peer_info is not None
        assert peer_info.load_level == PeerLoadLevel.OVERLOADED

    def test_get_load_level_unknown(self) -> None:
        """Test load level for unknown peer."""
        awareness = PeerHealthAwareness()
        assert awareness.get_load_level("unknown-node") == PeerLoadLevel.UNKNOWN

    def test_remove_peer(self) -> None:
        """Test removing a peer."""
        awareness = PeerHealthAwareness()

        health = HealthPiggyback(
            node_id="worker-1",
            node_type="worker",
            timestamp=time.monotonic(),
        )
        awareness.on_health_update(health)
        assert awareness.get_peer_info("worker-1") is not None

        removed = awareness.remove_peer("worker-1")
        assert removed is True
        assert awareness.get_peer_info("worker-1") is None

    def test_remove_unknown_peer(self) -> None:
        """Test removing unknown peer returns False."""
        awareness = PeerHealthAwareness()
        removed = awareness.remove_peer("unknown")
        assert removed is False


# =============================================================================
# Timeout Adaptation Tests
# =============================================================================


class TestTimeoutAdaptation:
    """Test probe timeout adaptation based on peer load."""

    def test_timeout_healthy_peer(self) -> None:
        """Test timeout for healthy peer is unchanged."""
        awareness = PeerHealthAwareness()

        health = HealthPiggyback(
            node_id="worker-1",
            node_type="worker",
            overload_state="healthy",
            timestamp=time.monotonic(),
        )
        awareness.on_health_update(health)

        base_timeout = 1.0
        adjusted = awareness.get_probe_timeout("worker-1", base_timeout)
        assert adjusted == base_timeout

    def test_timeout_busy_peer(self) -> None:
        """Test timeout for busy peer is slightly increased."""
        config = PeerHealthAwarenessConfig(timeout_multiplier_busy=1.25)
        awareness = PeerHealthAwareness(config=config)

        health = HealthPiggyback(
            node_id="worker-1",
            node_type="worker",
            overload_state="busy",
            timestamp=time.monotonic(),
        )
        awareness.on_health_update(health)

        base_timeout = 1.0
        adjusted = awareness.get_probe_timeout("worker-1", base_timeout)
        assert adjusted == 1.25

    def test_timeout_stressed_peer(self) -> None:
        """Test timeout for stressed peer is increased more."""
        config = PeerHealthAwarenessConfig(timeout_multiplier_stressed=1.75)
        awareness = PeerHealthAwareness(config=config)

        health = HealthPiggyback(
            node_id="worker-1",
            node_type="worker",
            overload_state="stressed",
            timestamp=time.monotonic(),
        )
        awareness.on_health_update(health)

        base_timeout = 1.0
        adjusted = awareness.get_probe_timeout("worker-1", base_timeout)
        assert adjusted == 1.75

    def test_timeout_overloaded_peer(self) -> None:
        """Test timeout for overloaded peer is significantly increased."""
        config = PeerHealthAwarenessConfig(timeout_multiplier_overloaded=2.5)
        awareness = PeerHealthAwareness(config=config)

        health = HealthPiggyback(
            node_id="worker-1",
            node_type="worker",
            overload_state="overloaded",
            timestamp=time.monotonic(),
        )
        awareness.on_health_update(health)

        base_timeout = 1.0
        adjusted = awareness.get_probe_timeout("worker-1", base_timeout)
        assert adjusted == 2.5

    def test_timeout_unknown_peer(self) -> None:
        """Test timeout for unknown peer is unchanged."""
        awareness = PeerHealthAwareness()

        base_timeout = 1.0
        adjusted = awareness.get_probe_timeout("unknown-node", base_timeout)
        assert adjusted == base_timeout

    def test_timeout_adaptation_disabled(self) -> None:
        """Test timeout adaptation can be disabled."""
        config = PeerHealthAwarenessConfig(
            enable_timeout_adaptation=False,
            timeout_multiplier_overloaded=2.5,
        )
        awareness = PeerHealthAwareness(config=config)

        health = HealthPiggyback(
            node_id="worker-1",
            node_type="worker",
            overload_state="overloaded",
            timestamp=time.monotonic(),
        )
        awareness.on_health_update(health)

        base_timeout = 1.0
        adjusted = awareness.get_probe_timeout("worker-1", base_timeout)
        assert adjusted == base_timeout  # Not multiplied


# =============================================================================
# Proxy Selection Tests
# =============================================================================


class TestProxySelection:
    """Test proxy selection filtering for indirect probes."""

    def test_should_use_healthy_as_proxy(self) -> None:
        """Test healthy peer can be used as proxy."""
        awareness = PeerHealthAwareness()

        health = HealthPiggyback(
            node_id="worker-1",
            node_type="worker",
            overload_state="healthy",
            timestamp=time.monotonic(),
        )
        awareness.on_health_update(health)

        assert awareness.should_use_as_proxy("worker-1") is True

    def test_should_use_busy_as_proxy(self) -> None:
        """Test busy peer can still be used as proxy."""
        awareness = PeerHealthAwareness()

        health = HealthPiggyback(
            node_id="worker-1",
            node_type="worker",
            overload_state="busy",
            timestamp=time.monotonic(),
        )
        awareness.on_health_update(health)

        assert awareness.should_use_as_proxy("worker-1") is True

    def test_should_not_use_stressed_as_proxy(self) -> None:
        """Test stressed peer should not be used as proxy."""
        awareness = PeerHealthAwareness()

        health = HealthPiggyback(
            node_id="worker-1",
            node_type="worker",
            overload_state="stressed",
            timestamp=time.monotonic(),
        )
        awareness.on_health_update(health)

        assert awareness.should_use_as_proxy("worker-1") is False

    def test_should_not_use_overloaded_as_proxy(self) -> None:
        """Test overloaded peer should not be used as proxy."""
        awareness = PeerHealthAwareness()

        health = HealthPiggyback(
            node_id="worker-1",
            node_type="worker",
            overload_state="overloaded",
            timestamp=time.monotonic(),
        )
        awareness.on_health_update(health)

        assert awareness.should_use_as_proxy("worker-1") is False

    def test_should_use_unknown_as_proxy(self) -> None:
        """Test unknown peer can be used as proxy (optimistic)."""
        awareness = PeerHealthAwareness()
        assert awareness.should_use_as_proxy("unknown-node") is True

    def test_proxy_avoidance_disabled(self) -> None:
        """Test proxy avoidance can be disabled."""
        config = PeerHealthAwarenessConfig(enable_proxy_avoidance=False)
        awareness = PeerHealthAwareness(config=config)

        health = HealthPiggyback(
            node_id="worker-1",
            node_type="worker",
            overload_state="overloaded",
            timestamp=time.monotonic(),
        )
        awareness.on_health_update(health)

        # Should still return True when disabled
        assert awareness.should_use_as_proxy("worker-1") is True

    def test_filter_proxy_candidates(self) -> None:
        """Test filtering a list of proxy candidates."""
        awareness = PeerHealthAwareness()

        # Add mixed health states
        for node_id, state in [
            ("healthy-1", "healthy"),
            ("healthy-2", "healthy"),
            ("stressed-1", "stressed"),
            ("overloaded-1", "overloaded"),
        ]:
            health = HealthPiggyback(
                node_id=node_id,
                node_type="worker",
                overload_state=state,
                timestamp=time.monotonic(),
            )
            awareness.on_health_update(health)

        candidates = ["healthy-1", "healthy-2", "stressed-1", "overloaded-1", "unknown"]
        filtered = awareness.filter_proxy_candidates(candidates)

        assert "healthy-1" in filtered
        assert "healthy-2" in filtered
        assert "unknown" in filtered  # Unknown is allowed
        assert "stressed-1" not in filtered
        assert "overloaded-1" not in filtered


# =============================================================================
# Gossip Reduction Tests
# =============================================================================


class TestGossipReduction:
    """Test gossip reduction factors."""

    def test_gossip_factor_healthy(self) -> None:
        """Test full gossip for healthy peer."""
        awareness = PeerHealthAwareness()

        health = HealthPiggyback(
            node_id="worker-1",
            node_type="worker",
            overload_state="healthy",
            timestamp=time.monotonic(),
        )
        awareness.on_health_update(health)

        factor = awareness.get_gossip_reduction_factor("worker-1")
        assert factor == 1.0

    def test_gossip_factor_busy(self) -> None:
        """Test slightly reduced gossip for busy peer."""
        awareness = PeerHealthAwareness()

        health = HealthPiggyback(
            node_id="worker-1",
            node_type="worker",
            overload_state="busy",
            timestamp=time.monotonic(),
        )
        awareness.on_health_update(health)

        factor = awareness.get_gossip_reduction_factor("worker-1")
        assert factor == 0.75

    def test_gossip_factor_stressed(self) -> None:
        """Test reduced gossip for stressed peer."""
        awareness = PeerHealthAwareness()

        health = HealthPiggyback(
            node_id="worker-1",
            node_type="worker",
            overload_state="stressed",
            timestamp=time.monotonic(),
        )
        awareness.on_health_update(health)

        factor = awareness.get_gossip_reduction_factor("worker-1")
        assert factor == 0.50

    def test_gossip_factor_overloaded(self) -> None:
        """Test minimal gossip for overloaded peer."""
        awareness = PeerHealthAwareness()

        health = HealthPiggyback(
            node_id="worker-1",
            node_type="worker",
            overload_state="overloaded",
            timestamp=time.monotonic(),
        )
        awareness.on_health_update(health)

        factor = awareness.get_gossip_reduction_factor("worker-1")
        assert factor == 0.25

    def test_gossip_reduction_disabled(self) -> None:
        """Test gossip reduction can be disabled."""
        config = PeerHealthAwarenessConfig(enable_gossip_reduction=False)
        awareness = PeerHealthAwareness(config=config)

        health = HealthPiggyback(
            node_id="worker-1",
            node_type="worker",
            overload_state="overloaded",
            timestamp=time.monotonic(),
        )
        awareness.on_health_update(health)

        factor = awareness.get_gossip_reduction_factor("worker-1")
        assert factor == 1.0


# =============================================================================
# Callback Tests
# =============================================================================


class TestCallbacks:
    """Test callback integration for state transitions."""

    def test_callback_on_overloaded(self) -> None:
        """Test callback invoked when peer becomes stressed."""
        awareness = PeerHealthAwareness()
        on_overloaded = MagicMock()
        awareness.set_overload_callback(on_overloaded=on_overloaded)

        # Transition to stressed
        health = HealthPiggyback(
            node_id="worker-1",
            node_type="worker",
            overload_state="stressed",
            timestamp=time.monotonic(),
        )
        awareness.on_health_update(health)

        on_overloaded.assert_called_once_with("worker-1")

    def test_callback_on_recovered(self) -> None:
        """Test callback invoked when peer recovers."""
        awareness = PeerHealthAwareness()
        on_recovered = MagicMock()
        awareness.set_overload_callback(on_recovered=on_recovered)

        # First become stressed
        stressed = HealthPiggyback(
            node_id="worker-1",
            node_type="worker",
            overload_state="stressed",
            timestamp=time.monotonic(),
        )
        awareness.on_health_update(stressed)

        # Then recover
        healthy = HealthPiggyback(
            node_id="worker-1",
            node_type="worker",
            overload_state="healthy",
            timestamp=time.monotonic(),
        )
        awareness.on_health_update(healthy)

        on_recovered.assert_called_once_with("worker-1")

    def test_callback_not_called_for_same_state(self) -> None:
        """Test callback not invoked for repeated same state."""
        awareness = PeerHealthAwareness()
        on_overloaded = MagicMock()
        awareness.set_overload_callback(on_overloaded=on_overloaded)

        # First stressed
        health1 = HealthPiggyback(
            node_id="worker-1",
            node_type="worker",
            overload_state="stressed",
            timestamp=time.monotonic(),
        )
        awareness.on_health_update(health1)

        # Second stressed (same state)
        health2 = HealthPiggyback(
            node_id="worker-1",
            node_type="worker",
            overload_state="stressed",
            timestamp=time.monotonic(),
        )
        awareness.on_health_update(health2)

        # Only called once for first transition
        assert on_overloaded.call_count == 1

    def test_callback_exception_does_not_break_processing(self) -> None:
        """Test callback exceptions don't affect processing."""
        awareness = PeerHealthAwareness()
        on_overloaded = MagicMock(side_effect=Exception("Callback error"))
        awareness.set_overload_callback(on_overloaded=on_overloaded)

        health = HealthPiggyback(
            node_id="worker-1",
            node_type="worker",
            overload_state="stressed",
            timestamp=time.monotonic(),
        )

        # Should not raise
        awareness.on_health_update(health)

        # Peer should still be tracked
        assert awareness.get_peer_info("worker-1") is not None


# =============================================================================
# Query Method Tests
# =============================================================================


class TestQueryMethods:
    """Test peer query methods."""

    def test_get_healthy_peers(self) -> None:
        """Test getting list of healthy peers."""
        awareness = PeerHealthAwareness()

        for node_id, state in [
            ("h1", "healthy"),
            ("h2", "healthy"),
            ("s1", "stressed"),
            ("o1", "overloaded"),
        ]:
            health = HealthPiggyback(
                node_id=node_id,
                node_type="worker",
                overload_state=state,
                timestamp=time.monotonic(),
            )
            awareness.on_health_update(health)

        healthy = awareness.get_healthy_peers()
        assert set(healthy) == {"h1", "h2"}

    def test_get_stressed_peers(self) -> None:
        """Test getting list of stressed/overloaded peers."""
        awareness = PeerHealthAwareness()

        for node_id, state in [
            ("h1", "healthy"),
            ("s1", "stressed"),
            ("o1", "overloaded"),
        ]:
            health = HealthPiggyback(
                node_id=node_id,
                node_type="worker",
                overload_state=state,
                timestamp=time.monotonic(),
            )
            awareness.on_health_update(health)

        stressed = awareness.get_stressed_peers()
        assert set(stressed) == {"s1", "o1"}

    def test_get_overloaded_peers(self) -> None:
        """Test getting list of overloaded peers only."""
        awareness = PeerHealthAwareness()

        for node_id, state in [
            ("h1", "healthy"),
            ("s1", "stressed"),
            ("o1", "overloaded"),
            ("o2", "overloaded"),
        ]:
            health = HealthPiggyback(
                node_id=node_id,
                node_type="worker",
                overload_state=state,
                timestamp=time.monotonic(),
            )
            awareness.on_health_update(health)

        overloaded = awareness.get_overloaded_peers()
        assert set(overloaded) == {"o1", "o2"}

    def test_get_peers_not_accepting_work(self) -> None:
        """Test getting peers not accepting work."""
        awareness = PeerHealthAwareness()

        for node_id, accepting in [
            ("a1", True),
            ("a2", True),
            ("n1", False),
        ]:
            health = HealthPiggyback(
                node_id=node_id,
                node_type="worker",
                accepting_work=accepting,
                timestamp=time.monotonic(),
            )
            awareness.on_health_update(health)

        not_accepting = awareness.get_peers_not_accepting_work()
        assert not_accepting == ["n1"]

    def test_rank_by_health(self) -> None:
        """Test ranking nodes by health."""
        awareness = PeerHealthAwareness()

        for node_id, state in [
            ("o1", "overloaded"),
            ("h1", "healthy"),
            ("s1", "stressed"),
            ("b1", "busy"),
        ]:
            health = HealthPiggyback(
                node_id=node_id,
                node_type="worker",
                overload_state=state,
                timestamp=time.monotonic(),
            )
            awareness.on_health_update(health)

        ranked = awareness.rank_by_health(["o1", "h1", "s1", "b1", "unknown"])

        # Healthiest first: unknown (0), healthy (1), busy (2), stressed (3), overloaded (4)
        assert ranked[0] == "unknown"  # Unknown = 0
        assert ranked[1] == "h1"       # Healthy = 1
        assert ranked[2] == "b1"       # Busy = 2
        assert ranked[3] == "s1"       # Stressed = 3
        assert ranked[4] == "o1"       # Overloaded = 4


# =============================================================================
# Capacity and Cleanup Tests
# =============================================================================


class TestCapacityAndCleanup:
    """Test capacity limits and cleanup."""

    def test_max_tracked_peers(self) -> None:
        """Test max tracked peers limit."""
        config = PeerHealthAwarenessConfig(max_tracked_peers=5)
        awareness = PeerHealthAwareness(config=config)

        # Add more than max
        for i in range(10):
            health = HealthPiggyback(
                node_id=f"node-{i}",
                node_type="worker",
                timestamp=time.monotonic(),
            )
            awareness.on_health_update(health)

        assert len(awareness._peers) <= 5

    def test_cleanup_stale(self) -> None:
        """Test stale entry cleanup."""
        config = PeerHealthAwarenessConfig(stale_threshold_seconds=1.0)
        awareness = PeerHealthAwareness(config=config)

        # Add entry and make it stale
        health = HealthPiggyback(
            node_id="stale-node",
            node_type="worker",
            timestamp=time.monotonic(),
        )
        awareness.on_health_update(health)
        awareness._peers["stale-node"].last_update = time.monotonic() - 60.0

        # Add fresh entry
        health2 = HealthPiggyback(
            node_id="fresh-node",
            node_type="worker",
            timestamp=time.monotonic(),
        )
        awareness.on_health_update(health2)

        removed = awareness.cleanup_stale()

        assert removed == 1
        assert awareness.get_peer_info("stale-node") is None
        assert awareness.get_peer_info("fresh-node") is not None

    def test_clear(self) -> None:
        """Test clearing all peers."""
        awareness = PeerHealthAwareness()

        for i in range(10):
            health = HealthPiggyback(
                node_id=f"node-{i}",
                node_type="worker",
                timestamp=time.monotonic(),
            )
            awareness.on_health_update(health)

        assert len(awareness._peers) == 10

        awareness.clear()

        assert len(awareness._peers) == 0


# =============================================================================
# Concurrency Tests
# =============================================================================


class TestConcurrency:
    """Test concurrent operations."""

    @pytest.mark.asyncio
    async def test_concurrent_updates(self) -> None:
        """Test concurrent health updates."""
        awareness = PeerHealthAwareness()

        async def update_node(node_idx: int) -> None:
            for update_num in range(10):
                state = ["healthy", "busy", "stressed"][update_num % 3]
                health = HealthPiggyback(
                    node_id=f"node-{node_idx}",
                    node_type="worker",
                    overload_state=state,
                    timestamp=time.monotonic(),
                )
                awareness.on_health_update(health)
                await asyncio.sleep(0.001)

        await asyncio.gather(*[update_node(i) for i in range(10)])

        # All nodes should be tracked
        for i in range(10):
            assert awareness.get_peer_info(f"node-{i}") is not None

    @pytest.mark.asyncio
    async def test_concurrent_queries_and_updates(self) -> None:
        """Test concurrent queries during updates."""
        awareness = PeerHealthAwareness()

        # Populate some initial data
        for i in range(20):
            health = HealthPiggyback(
                node_id=f"node-{i}",
                node_type="worker",
                overload_state="healthy",
                timestamp=time.monotonic(),
            )
            awareness.on_health_update(health)

        async def do_updates() -> None:
            for _ in range(50):
                node_id = f"node-{_ % 20}"
                health = HealthPiggyback(
                    node_id=node_id,
                    node_type="worker",
                    overload_state="stressed",
                    timestamp=time.monotonic(),
                )
                awareness.on_health_update(health)
                await asyncio.sleep(0.001)

        async def do_queries() -> None:
            for _ in range(100):
                awareness.get_healthy_peers()
                awareness.get_stressed_peers()
                awareness.filter_proxy_candidates([f"node-{i}" for i in range(20)])
                await asyncio.sleep(0.001)

        await asyncio.gather(do_updates(), do_queries())


# =============================================================================
# Statistics Tests
# =============================================================================


class TestStatistics:
    """Test statistics tracking."""

    def test_stats(self) -> None:
        """Test statistics are tracked correctly."""
        awareness = PeerHealthAwareness()

        # Add some updates
        for i in range(5):
            health = HealthPiggyback(
                node_id=f"node-{i}",
                node_type="worker",
                overload_state="healthy" if i < 3 else "overloaded",
                timestamp=time.monotonic(),
            )
            awareness.on_health_update(health)

        stats = awareness.get_stats()

        assert stats["tracked_peers"] == 5
        assert stats["total_updates"] == 5
        assert stats["current_overloaded"] == 2
        assert stats["overloaded_updates"] == 2


# =============================================================================
# Edge Cases
# =============================================================================


class TestEdgeCases:
    """Test edge cases."""

    def test_empty_node_id(self) -> None:
        """Test handling empty node ID."""
        awareness = PeerHealthAwareness()

        health = HealthPiggyback(
            node_id="",
            node_type="worker",
            timestamp=time.monotonic(),
        )
        awareness.on_health_update(health)

        # Empty string is valid
        assert awareness.get_peer_info("") is not None

    def test_stale_info_auto_removed(self) -> None:
        """Test that stale info is auto-removed on query."""
        config = PeerHealthAwarenessConfig(stale_threshold_seconds=0.1)
        awareness = PeerHealthAwareness(config=config)

        health = HealthPiggyback(
            node_id="node-1",
            node_type="worker",
            timestamp=time.monotonic(),
        )
        awareness.on_health_update(health)

        # Make stale
        awareness._peers["node-1"].last_update = time.monotonic() - 60.0

        # Query should return None and remove stale entry
        result = awareness.get_peer_info("node-1")
        assert result is None
        assert "node-1" not in awareness._peers
