"""
Integration tests for DiscoveryService (AD-28).

These tests verify that the DiscoveryService correctly:
1. Initializes with configuration
2. Adds and removes peers
3. Selects peers using Power of Two Choices with EWMA
4. Records success/failure feedback
5. Handles locality-aware selection
"""

import pytest
import time

from hyperscale.distributed.discovery import (
    DiscoveryConfig,
    DiscoveryService,
    PeerInfo,
    PeerHealth,
    LocalityInfo,
    LocalityTier,
    SelectionResult,
)


class TestDiscoveryServiceBasics:
    """Test basic DiscoveryService functionality."""

    def test_service_initialization_with_static_seeds(self):
        """DiscoveryService should initialize with static seeds."""
        config = DiscoveryConfig(
            cluster_id="test-cluster",
            environment_id="test",
            static_seeds=["10.0.0.1:9000", "10.0.0.2:9000"],
        )

        service = DiscoveryService(config)

        assert service.peer_count == 2
        assert service.has_peers is True

    def test_service_initialization_without_locality(self):
        """DiscoveryService should work without locality configuration."""
        config = DiscoveryConfig(
            cluster_id="test-cluster",
            environment_id="test",
            static_seeds=["10.0.0.1:9000"],
        )

        service = DiscoveryService(config)

        assert service.local_locality is None
        assert service.peer_count == 1

    def test_service_initialization_with_locality(self):
        """DiscoveryService should initialize locality filter when configured."""
        config = DiscoveryConfig(
            cluster_id="test-cluster",
            environment_id="test",
            static_seeds=["10.0.0.1:9000"],
            datacenter_id="us-east-1a",
            region_id="us-east-1",
        )

        service = DiscoveryService(config)

        assert service.local_locality is not None
        assert service.local_locality.datacenter_id == "us-east-1a"
        assert service.local_locality.region_id == "us-east-1"


class TestPeerManagement:
    """Test peer add/remove/query operations."""

    def test_add_peer_manually(self):
        """add_peer should add a new peer to the service."""
        config = DiscoveryConfig(
            cluster_id="test-cluster",
            environment_id="test",
            static_seeds=["10.0.0.1:9000"],
        )
        service = DiscoveryService(config)

        peer = service.add_peer(
            peer_id="manager-1",
            host="10.0.1.1",
            port=9000,
            role="manager",
        )

        assert service.peer_count == 2
        assert service.contains("manager-1")
        assert peer.peer_id == "manager-1"
        assert peer.host == "10.0.1.1"
        assert peer.port == 9000

    def test_add_peer_with_locality(self):
        """add_peer should set locality on the peer."""
        config = DiscoveryConfig(
            cluster_id="test-cluster",
            environment_id="test",
            static_seeds=["10.0.0.1:9000"],
            datacenter_id="us-east-1a",
            region_id="us-east-1",
        )
        service = DiscoveryService(config)

        peer = service.add_peer(
            peer_id="manager-1",
            host="10.0.1.1",
            port=9000,
            datacenter_id="us-east-1a",
            region_id="us-east-1",
        )

        assert peer.datacenter_id == "us-east-1a"
        assert peer.region_id == "us-east-1"

    def test_remove_peer(self):
        """remove_peer should remove a peer from the service."""
        config = DiscoveryConfig(
            cluster_id="test-cluster",
            environment_id="test",
            static_seeds=["10.0.0.1:9000"],
        )
        service = DiscoveryService(config)

        service.add_peer(peer_id="manager-1", host="10.0.1.1", port=9000)
        assert service.peer_count == 2

        removed = service.remove_peer("manager-1")
        assert removed is True
        assert service.peer_count == 1
        assert not service.contains("manager-1")

    def test_remove_nonexistent_peer(self):
        """remove_peer should return False for nonexistent peer."""
        config = DiscoveryConfig(
            cluster_id="test-cluster",
            environment_id="test",
            static_seeds=["10.0.0.1:9000"],
        )
        service = DiscoveryService(config)

        removed = service.remove_peer("nonexistent")
        assert removed is False

    def test_get_peer(self):
        """get_peer should return the peer if found."""
        config = DiscoveryConfig(
            cluster_id="test-cluster",
            environment_id="test",
            static_seeds=["10.0.0.1:9000"],
        )
        service = DiscoveryService(config)

        service.add_peer(peer_id="manager-1", host="10.0.1.1", port=9000)

        peer = service.get_peer("manager-1")
        assert peer is not None
        assert peer.peer_id == "manager-1"

        nonexistent = service.get_peer("nonexistent")
        assert nonexistent is None

    def test_get_peer_address(self):
        """get_peer_address should return (host, port) tuple."""
        config = DiscoveryConfig(
            cluster_id="test-cluster",
            environment_id="test",
            static_seeds=["10.0.0.1:9000"],
        )
        service = DiscoveryService(config)

        service.add_peer(peer_id="manager-1", host="10.0.1.1", port=9000)

        addr = service.get_peer_address("manager-1")
        assert addr == ("10.0.1.1", 9000)

    def test_get_all_peers(self):
        """get_all_peers should return all known peers."""
        config = DiscoveryConfig(
            cluster_id="test-cluster",
            environment_id="test",
            static_seeds=["10.0.0.1:9000"],
        )
        service = DiscoveryService(config)

        service.add_peer(peer_id="manager-1", host="10.0.1.1", port=9000)
        service.add_peer(peer_id="manager-2", host="10.0.1.2", port=9000)

        peers = service.get_all_peers()
        assert len(peers) == 3  # 1 seed + 2 added


class TestPeerSelection:
    """Test peer selection using Power of Two Choices."""

    def test_select_peer_returns_result(self):
        """select_peer should return SelectionResult for known peers."""
        config = DiscoveryConfig(
            cluster_id="test-cluster",
            environment_id="test",
            static_seeds=["10.0.0.1:9000", "10.0.0.2:9000"],
        )
        service = DiscoveryService(config)

        result = service.select_peer("workflow-123")

        assert result is not None
        assert isinstance(result, SelectionResult)
        assert result.peer_id is not None
        assert result.effective_latency_ms >= 0

    def test_select_peer_returns_none_when_no_peers(self):
        """select_peer should return None when no peers available."""
        config = DiscoveryConfig(
            cluster_id="test-cluster",
            environment_id="test",
            static_seeds=["10.0.0.1:9000"],
        )
        service = DiscoveryService(config)

        # Remove the only peer
        service.clear()

        result = service.select_peer("workflow-123")
        assert result is None

    def test_select_peer_is_deterministic(self):
        """select_peer should return consistent results for same key."""
        config = DiscoveryConfig(
            cluster_id="test-cluster",
            environment_id="test",
            static_seeds=["10.0.0.1:9000", "10.0.0.2:9000", "10.0.0.3:9000"],
        )
        service = DiscoveryService(config)

        # Same key should get same peer (deterministic rendezvous hash)
        results = [service.select_peer("workflow-123") for _ in range(5)]

        peer_ids = [r.peer_id for r in results if r is not None]
        assert len(set(peer_ids)) == 1  # All same peer

    def test_select_peer_with_filter(self):
        """select_peer_with_filter should only consider filtered peers."""
        config = DiscoveryConfig(
            cluster_id="test-cluster",
            environment_id="test",
            static_seeds=["10.0.0.1:9000"],
        )
        service = DiscoveryService(config)

        service.add_peer(peer_id="healthy-1", host="10.0.1.1", port=9000)
        service.add_peer(peer_id="healthy-2", host="10.0.1.2", port=9000)

        # Filter to only "healthy-*" peers
        result = service.select_peer_with_filter(
            "workflow-123",
            filter_fn=lambda p: p.startswith("healthy-"),
        )

        assert result is not None
        assert result.peer_id.startswith("healthy-")


class TestFeedbackRecording:
    """Test success/failure feedback recording."""

    def test_record_success_updates_latency(self):
        """record_success should update peer latency tracking."""
        config = DiscoveryConfig(
            cluster_id="test-cluster",
            environment_id="test",
            static_seeds=["10.0.0.1:9000"],
        )
        service = DiscoveryService(config)

        service.add_peer(peer_id="manager-1", host="10.0.1.1", port=9000)

        # Record some successes
        for _ in range(5):
            service.record_success("manager-1", latency_ms=15.0)

        peer = service.get_peer("manager-1")
        assert peer is not None
        assert peer.ewma_latency_ms > 0
        assert peer.health == PeerHealth.HEALTHY

    def test_record_failure_updates_health(self):
        """record_failure should update peer health tracking."""
        config = DiscoveryConfig(
            cluster_id="test-cluster",
            environment_id="test",
            static_seeds=["10.0.0.1:9000"],
        )
        service = DiscoveryService(config)

        service.add_peer(peer_id="manager-1", host="10.0.1.1", port=9000)

        # Record multiple failures
        for _ in range(3):
            service.record_failure("manager-1")

        peer = service.get_peer("manager-1")
        assert peer is not None
        assert peer.consecutive_failures == 3
        assert peer.health == PeerHealth.UNHEALTHY

    def test_failure_affects_selection_weight(self):
        """Failures should reduce peer's selection weight."""
        config = DiscoveryConfig(
            cluster_id="test-cluster",
            environment_id="test",
            static_seeds=["10.0.0.1:9000"],
        )
        service = DiscoveryService(config)

        service.add_peer(peer_id="manager-1", host="10.0.1.1", port=9000)

        initial_latency = service.get_effective_latency("manager-1")

        # Record failures
        for _ in range(3):
            service.record_failure("manager-1")

        # Effective latency should increase (penalty applied)
        after_latency = service.get_effective_latency("manager-1")
        assert after_latency > initial_latency


class TestHealthFiltering:
    """Test health-based peer filtering."""

    def test_get_healthy_peers(self):
        """get_healthy_peers should return only healthy/unknown peers."""
        config = DiscoveryConfig(
            cluster_id="test-cluster",
            environment_id="test",
            static_seeds=["10.0.0.1:9000"],
        )
        service = DiscoveryService(config)

        service.add_peer(peer_id="healthy-1", host="10.0.1.1", port=9000)
        service.add_peer(peer_id="unhealthy-1", host="10.0.1.2", port=9000)

        # Make one unhealthy
        for _ in range(3):
            service.record_failure("unhealthy-1")

        healthy = service.get_healthy_peers()
        healthy_ids = {p.peer_id for p in healthy}

        assert "healthy-1" in healthy_ids
        assert "unhealthy-1" not in healthy_ids

    def test_get_peers_by_health(self):
        """get_peers_by_health should filter by specific health status."""
        config = DiscoveryConfig(
            cluster_id="test-cluster",
            environment_id="test",
            static_seeds=["10.0.0.1:9000"],
        )
        service = DiscoveryService(config)

        service.add_peer(peer_id="manager-1", host="10.0.1.1", port=9000)

        # Make it unhealthy
        for _ in range(3):
            service.record_failure("manager-1")

        unhealthy = service.get_peers_by_health(PeerHealth.UNHEALTHY)
        assert len(unhealthy) == 1
        assert unhealthy[0].peer_id == "manager-1"


class TestLocalityAwareSelection:
    """Test locality-aware peer selection."""

    def test_locality_filter_initializes_with_config(self):
        """Locality filter should initialize when datacenter_id is set."""
        config = DiscoveryConfig(
            cluster_id="test-cluster",
            environment_id="test",
            static_seeds=["10.0.0.1:9000"],
            datacenter_id="us-east-1a",
            region_id="us-east-1",
        )
        service = DiscoveryService(config)

        assert service.local_locality is not None
        assert service.local_locality.datacenter_id == "us-east-1a"

    def test_update_peer_locality(self):
        """update_peer_locality should update a peer's location."""
        config = DiscoveryConfig(
            cluster_id="test-cluster",
            environment_id="test",
            static_seeds=["10.0.0.1:9000"],
            datacenter_id="us-east-1a",
            region_id="us-east-1",
        )
        service = DiscoveryService(config)

        service.add_peer(peer_id="manager-1", host="10.0.1.1", port=9000)

        updated = service.update_peer_locality(
            "manager-1",
            datacenter_id="us-west-2a",
            region_id="us-west-2",
        )

        assert updated is True
        peer = service.get_peer("manager-1")
        assert peer is not None
        assert peer.datacenter_id == "us-west-2a"
        assert peer.region_id == "us-west-2"


class TestMetricsAndMaintenance:
    """Test metrics and maintenance operations."""

    def test_get_metrics_snapshot(self):
        """get_metrics_snapshot should return useful metrics."""
        config = DiscoveryConfig(
            cluster_id="test-cluster",
            environment_id="test",
            static_seeds=["10.0.0.1:9000", "10.0.0.2:9000"],
        )
        service = DiscoveryService(config)

        # Add and interact with peers
        service.add_peer(peer_id="manager-1", host="10.0.1.1", port=9000)
        service.record_success("manager-1", latency_ms=10.0)

        metrics = service.get_metrics_snapshot()

        assert "peer_count" in metrics
        assert metrics["peer_count"] == 3
        assert "healthy_peer_count" in metrics
        assert "dns_cache_stats" in metrics

    def test_decay_failures(self):
        """decay_failures should allow failed peers to recover."""
        config = DiscoveryConfig(
            cluster_id="test-cluster",
            environment_id="test",
            static_seeds=["10.0.0.1:9000"],
        )
        service = DiscoveryService(config)

        service.add_peer(peer_id="manager-1", host="10.0.1.1", port=9000)

        # Make it have failures
        service.record_failure("manager-1")

        # Decay should reduce failure impact
        decayed = service.decay_failures()
        assert decayed >= 0

    def test_clear(self):
        """clear should remove all peers and reset state."""
        config = DiscoveryConfig(
            cluster_id="test-cluster",
            environment_id="test",
            static_seeds=["10.0.0.1:9000", "10.0.0.2:9000"],
        )
        service = DiscoveryService(config)

        assert service.peer_count == 2

        service.clear()

        assert service.peer_count == 0
        assert service.has_peers is False


class TestCallbacks:
    """Test callback functionality."""

    def test_on_peer_added_callback(self):
        """on_peer_added callback should be called when peer is added."""
        config = DiscoveryConfig(
            cluster_id="test-cluster",
            environment_id="test",
            static_seeds=["10.0.0.1:9000"],
        )
        service = DiscoveryService(config)

        added_peers: list[PeerInfo] = []
        service.set_callbacks(on_peer_added=lambda p: added_peers.append(p))

        service.add_peer(peer_id="manager-1", host="10.0.1.1", port=9000)

        assert len(added_peers) == 1
        assert added_peers[0].peer_id == "manager-1"

    def test_on_peer_removed_callback(self):
        """on_peer_removed callback should be called when peer is removed."""
        config = DiscoveryConfig(
            cluster_id="test-cluster",
            environment_id="test",
            static_seeds=["10.0.0.1:9000"],
        )
        service = DiscoveryService(config)

        service.add_peer(peer_id="manager-1", host="10.0.1.1", port=9000)

        removed_ids: list[str] = []
        service.set_callbacks(on_peer_removed=lambda p_id: removed_ids.append(p_id))

        service.remove_peer("manager-1")

        assert len(removed_ids) == 1
        assert removed_ids[0] == "manager-1"
