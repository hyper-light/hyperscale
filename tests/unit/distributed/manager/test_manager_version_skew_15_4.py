"""
Unit tests for the manager's version skew handler (AD-25).

Each test class validates:
- Happy path (normal operations)
- Negative path (invalid inputs, error conditions)
- Failure modes (exception handling)
- Concurrency and race conditions
- Edge cases (boundary conditions, special values)
"""

import asyncio
import pytest
from unittest.mock import MagicMock, AsyncMock

from hyperscale.distributed.nodes.manager.version_skew import ManagerVersionSkewHandler
from hyperscale.distributed.nodes.manager.config import ManagerConfig
from hyperscale.distributed.nodes.manager.state import ManagerState
from hyperscale.distributed.protocol.version import (
    ProtocolVersion,
    NodeCapabilities,
    NegotiatedCapabilities,
    CURRENT_PROTOCOL_VERSION,
    get_features_for_version,
)


# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def mock_logger():
    """Create a mock logger."""
    logger = MagicMock()
    logger.log = AsyncMock()
    return logger


@pytest.fixture
def mock_task_runner():
    """Create a mock task runner."""
    runner = MagicMock()
    runner.run = MagicMock()
    return runner


@pytest.fixture
def manager_config():
    """Create a basic ManagerConfig."""
    return ManagerConfig(
        host="127.0.0.1",
        tcp_port=8000,
        udp_port=8001,
        rate_limit_default_max_requests=100,
        rate_limit_default_window_seconds=10.0,
        rate_limit_cleanup_interval_seconds=300.0,
    )


@pytest.fixture
def manager_state():
    """Create a ManagerState instance."""
    return ManagerState()


@pytest.fixture
def version_skew_handler(manager_state, manager_config, mock_logger, mock_task_runner):
    """Create a ManagerVersionSkewHandler."""
    return ManagerVersionSkewHandler(
        state=manager_state,
        config=manager_config,
        logger=mock_logger,
        node_id="manager-test-123",
        task_runner=mock_task_runner,
    )


# =============================================================================
# ManagerVersionSkewHandler Tests - Happy Path
# =============================================================================


class TestManagerVersionSkewHandlerHappyPath:
    """Happy path tests for ManagerVersionSkewHandler."""

    def test_initialization(self, version_skew_handler):
        """Handler initializes with correct protocol version."""
        assert version_skew_handler.protocol_version == CURRENT_PROTOCOL_VERSION
        assert version_skew_handler.capabilities == get_features_for_version(
            CURRENT_PROTOCOL_VERSION
        )

    def test_get_local_capabilities(self, version_skew_handler):
        """get_local_capabilities returns correct capabilities."""
        caps = version_skew_handler.get_local_capabilities()

        assert isinstance(caps, NodeCapabilities)
        assert caps.protocol_version == CURRENT_PROTOCOL_VERSION
        assert "heartbeat" in caps.capabilities

    def test_negotiate_with_worker_same_version(self, version_skew_handler):
        """Negotiate with worker at same version."""
        worker_id = "worker-123"
        remote_caps = NodeCapabilities.current()

        result = version_skew_handler.negotiate_with_worker(worker_id, remote_caps)

        assert isinstance(result, NegotiatedCapabilities)
        assert result.compatible is True
        assert result.local_version == CURRENT_PROTOCOL_VERSION
        assert result.remote_version == CURRENT_PROTOCOL_VERSION
        assert len(result.common_features) > 0

    def test_negotiate_with_worker_older_minor_version(self, version_skew_handler):
        """Negotiate with worker at older minor version."""
        worker_id = "worker-old"
        older_version = ProtocolVersion(
            CURRENT_PROTOCOL_VERSION.major,
            CURRENT_PROTOCOL_VERSION.minor - 1,
        )
        remote_caps = NodeCapabilities(
            protocol_version=older_version,
            capabilities=get_features_for_version(older_version),
        )

        result = version_skew_handler.negotiate_with_worker(worker_id, remote_caps)

        assert result.compatible is True
        # Common features should be limited to older version's features
        assert len(result.common_features) <= len(remote_caps.capabilities)

    def test_negotiate_with_gate(self, version_skew_handler, manager_state):
        """Negotiate with gate stores capabilities in state."""
        gate_id = "gate-123"
        remote_caps = NodeCapabilities.current()

        result = version_skew_handler.negotiate_with_gate(gate_id, remote_caps)

        assert result.compatible is True
        assert gate_id in manager_state._gate_negotiated_caps

    def test_negotiate_with_peer_manager(self, version_skew_handler):
        """Negotiate with peer manager."""
        peer_id = "manager-peer-123"
        remote_caps = NodeCapabilities.current()

        result = version_skew_handler.negotiate_with_peer_manager(peer_id, remote_caps)

        assert result.compatible is True
        assert version_skew_handler.get_peer_capabilities(peer_id) is not None

    def test_worker_supports_feature(self, version_skew_handler):
        """Check if worker supports feature after negotiation."""
        worker_id = "worker-feature"
        remote_caps = NodeCapabilities.current()

        version_skew_handler.negotiate_with_worker(worker_id, remote_caps)

        assert (
            version_skew_handler.worker_supports_feature(worker_id, "heartbeat") is True
        )
        assert (
            version_skew_handler.worker_supports_feature(worker_id, "unknown_feature")
            is False
        )

    def test_gate_supports_feature(self, version_skew_handler):
        """Check if gate supports feature after negotiation."""
        gate_id = "gate-feature"
        remote_caps = NodeCapabilities.current()

        version_skew_handler.negotiate_with_gate(gate_id, remote_caps)

        assert version_skew_handler.gate_supports_feature(gate_id, "heartbeat") is True

    def test_peer_supports_feature(self, version_skew_handler):
        """Check if peer supports feature after negotiation."""
        peer_id = "peer-feature"
        remote_caps = NodeCapabilities.current()

        version_skew_handler.negotiate_with_peer_manager(peer_id, remote_caps)

        assert version_skew_handler.peer_supports_feature(peer_id, "heartbeat") is True

    def test_is_version_compatible(self, version_skew_handler):
        """Check version compatibility."""
        compatible = ProtocolVersion(CURRENT_PROTOCOL_VERSION.major, 0)
        incompatible = ProtocolVersion(CURRENT_PROTOCOL_VERSION.major + 1, 0)

        assert version_skew_handler.is_version_compatible(compatible) is True
        assert version_skew_handler.is_version_compatible(incompatible) is False


# =============================================================================
# ManagerVersionSkewHandler Tests - Negative Path
# =============================================================================


class TestManagerVersionSkewHandlerNegativePath:
    """Negative path tests for ManagerVersionSkewHandler."""

    def test_negotiate_with_worker_incompatible_version(self, version_skew_handler):
        """Negotiation fails with incompatible major version."""
        worker_id = "worker-incompat"
        incompatible_version = ProtocolVersion(CURRENT_PROTOCOL_VERSION.major + 1, 0)
        remote_caps = NodeCapabilities(
            protocol_version=incompatible_version,
            capabilities=set(),
        )

        with pytest.raises(ValueError) as exc_info:
            version_skew_handler.negotiate_with_worker(worker_id, remote_caps)

        assert "Incompatible protocol versions" in str(exc_info.value)

    def test_negotiate_with_gate_incompatible_version(self, version_skew_handler):
        """Gate negotiation fails with incompatible version."""
        gate_id = "gate-incompat"
        incompatible_version = ProtocolVersion(CURRENT_PROTOCOL_VERSION.major + 1, 0)
        remote_caps = NodeCapabilities(
            protocol_version=incompatible_version,
            capabilities=set(),
        )

        with pytest.raises(ValueError):
            version_skew_handler.negotiate_with_gate(gate_id, remote_caps)

    def test_negotiate_with_peer_incompatible_version(self, version_skew_handler):
        """Peer negotiation fails with incompatible version."""
        peer_id = "peer-incompat"
        incompatible_version = ProtocolVersion(CURRENT_PROTOCOL_VERSION.major + 1, 0)
        remote_caps = NodeCapabilities(
            protocol_version=incompatible_version,
            capabilities=set(),
        )

        with pytest.raises(ValueError):
            version_skew_handler.negotiate_with_peer_manager(peer_id, remote_caps)

    def test_worker_supports_feature_not_negotiated(self, version_skew_handler):
        """Feature check returns False for non-negotiated worker."""
        assert (
            version_skew_handler.worker_supports_feature(
                "nonexistent-worker", "heartbeat"
            )
            is False
        )

    def test_gate_supports_feature_not_negotiated(self, version_skew_handler):
        """Feature check returns False for non-negotiated gate."""
        assert (
            version_skew_handler.gate_supports_feature("nonexistent-gate", "heartbeat")
            is False
        )

    def test_peer_supports_feature_not_negotiated(self, version_skew_handler):
        """Feature check returns False for non-negotiated peer."""
        assert (
            version_skew_handler.peer_supports_feature("nonexistent-peer", "heartbeat")
            is False
        )


# =============================================================================
# ManagerVersionSkewHandler Tests - Node Removal
# =============================================================================


class TestManagerVersionSkewHandlerRemoval:
    """Tests for node capability removal."""

    def test_remove_worker(self, version_skew_handler):
        """remove_worker clears worker capabilities."""
        worker_id = "worker-to-remove"
        remote_caps = NodeCapabilities.current()

        version_skew_handler.negotiate_with_worker(worker_id, remote_caps)
        assert version_skew_handler.get_worker_capabilities(worker_id) is not None

        version_skew_handler.remove_worker(worker_id)
        assert version_skew_handler.get_worker_capabilities(worker_id) is None

    def test_remove_gate(self, version_skew_handler, manager_state):
        """remove_gate clears gate capabilities from handler and state."""
        gate_id = "gate-to-remove"
        remote_caps = NodeCapabilities.current()

        version_skew_handler.negotiate_with_gate(gate_id, remote_caps)
        assert gate_id in manager_state._gate_negotiated_caps

        version_skew_handler.remove_gate(gate_id)
        assert version_skew_handler.get_gate_capabilities(gate_id) is None
        assert gate_id not in manager_state._gate_negotiated_caps

    def test_remove_peer(self, version_skew_handler):
        """remove_peer clears peer capabilities."""
        peer_id = "peer-to-remove"
        remote_caps = NodeCapabilities.current()

        version_skew_handler.negotiate_with_peer_manager(peer_id, remote_caps)
        assert version_skew_handler.get_peer_capabilities(peer_id) is not None

        version_skew_handler.remove_peer(peer_id)
        assert version_skew_handler.get_peer_capabilities(peer_id) is None

    def test_remove_nonexistent_worker(self, version_skew_handler):
        """remove_worker handles nonexistent worker gracefully."""
        version_skew_handler.remove_worker("nonexistent")

    def test_remove_nonexistent_gate(self, version_skew_handler):
        """remove_gate handles nonexistent gate gracefully."""
        version_skew_handler.remove_gate("nonexistent")

    def test_remove_nonexistent_peer(self, version_skew_handler):
        """remove_peer handles nonexistent peer gracefully."""
        version_skew_handler.remove_peer("nonexistent")


# =============================================================================
# ManagerVersionSkewHandler Tests - Feature Queries
# =============================================================================


class TestManagerVersionSkewHandlerFeatureQueries:
    """Tests for feature query methods."""

    def test_get_common_features_with_all_workers(self, version_skew_handler):
        """Get features common to all workers."""
        # Initially no workers
        common = version_skew_handler.get_common_features_with_all_workers()
        assert common == set()

        # Add two workers with same version
        remote_caps = NodeCapabilities.current()
        version_skew_handler.negotiate_with_worker("worker-1", remote_caps)
        version_skew_handler.negotiate_with_worker("worker-2", remote_caps)

        common = version_skew_handler.get_common_features_with_all_workers()
        assert len(common) > 0
        assert "heartbeat" in common

    def test_get_common_features_with_all_workers_mixed_versions(
        self, version_skew_handler
    ):
        """Common features with workers at different versions."""
        # Worker 1: current version
        version_skew_handler.negotiate_with_worker(
            "worker-current",
            NodeCapabilities.current(),
        )

        # Worker 2: older version (1.0)
        older_version = ProtocolVersion(1, 0)
        older_caps = NodeCapabilities(
            protocol_version=older_version,
            capabilities=get_features_for_version(older_version),
        )
        version_skew_handler.negotiate_with_worker("worker-old", older_caps)

        common = version_skew_handler.get_common_features_with_all_workers()

        # Should only include features from 1.0
        assert "heartbeat" in common
        assert "job_submission" in common
        # 1.1+ features should not be common
        if CURRENT_PROTOCOL_VERSION.minor > 0:
            # batched_stats was introduced in 1.1
            assert "batched_stats" not in common

    def test_get_common_features_with_all_gates(self, version_skew_handler):
        """Get features common to all gates."""
        # No gates initially
        common = version_skew_handler.get_common_features_with_all_gates()
        assert common == set()

        # Add gates
        version_skew_handler.negotiate_with_gate("gate-1", NodeCapabilities.current())
        version_skew_handler.negotiate_with_gate("gate-2", NodeCapabilities.current())

        common = version_skew_handler.get_common_features_with_all_gates()
        assert "heartbeat" in common


# =============================================================================
# ManagerVersionSkewHandler Tests - Metrics
# =============================================================================


class TestManagerVersionSkewHandlerMetrics:
    """Tests for version skew metrics."""

    def test_get_version_metrics_empty(self, version_skew_handler):
        """Metrics with no connected nodes."""
        metrics = version_skew_handler.get_version_metrics()

        assert "local_version" in metrics
        assert "local_feature_count" in metrics
        assert metrics["worker_count"] == 0
        assert metrics["gate_count"] == 0
        assert metrics["peer_count"] == 0

    def test_get_version_metrics_with_nodes(self, version_skew_handler):
        """Metrics with connected nodes."""
        # Add various nodes
        current_caps = NodeCapabilities.current()
        version_skew_handler.negotiate_with_worker("worker-1", current_caps)
        version_skew_handler.negotiate_with_worker("worker-2", current_caps)
        version_skew_handler.negotiate_with_gate("gate-1", current_caps)
        version_skew_handler.negotiate_with_peer_manager("peer-1", current_caps)

        metrics = version_skew_handler.get_version_metrics()

        assert metrics["worker_count"] == 2
        assert metrics["gate_count"] == 1
        assert metrics["peer_count"] == 1
        assert str(CURRENT_PROTOCOL_VERSION) in metrics["worker_versions"]

    def test_get_version_metrics_mixed_versions(self, version_skew_handler):
        """Metrics with nodes at different versions."""
        current_caps = NodeCapabilities.current()
        version_skew_handler.negotiate_with_worker("worker-current", current_caps)

        older_version = ProtocolVersion(1, 0)
        older_caps = NodeCapabilities(
            protocol_version=older_version,
            capabilities=get_features_for_version(older_version),
        )
        version_skew_handler.negotiate_with_worker("worker-old", older_caps)

        metrics = version_skew_handler.get_version_metrics()

        assert metrics["worker_count"] == 2
        # Should have two different versions
        assert len(metrics["worker_versions"]) == 2


# =============================================================================
# ManagerVersionSkewHandler Tests - Concurrency
# =============================================================================


class TestManagerVersionSkewHandlerConcurrency:
    """Concurrency tests for ManagerVersionSkewHandler."""

    @pytest.mark.asyncio
    async def test_concurrent_negotiations(self, version_skew_handler):
        """Multiple concurrent negotiations work correctly."""
        results = []

        async def negotiate_worker(worker_id: str):
            caps = NodeCapabilities.current()
            result = version_skew_handler.negotiate_with_worker(worker_id, caps)
            results.append((worker_id, result.compatible))

        # Run concurrent negotiations
        await asyncio.gather(*[negotiate_worker(f"worker-{idx}") for idx in range(20)])

        assert len(results) == 20
        assert all(compatible for _, compatible in results)

    @pytest.mark.asyncio
    async def test_concurrent_feature_checks(self, version_skew_handler):
        """Concurrent feature checks work correctly."""
        # Pre-negotiate workers
        for idx in range(10):
            version_skew_handler.negotiate_with_worker(
                f"worker-{idx}",
                NodeCapabilities.current(),
            )

        results = []

        async def check_feature(worker_id: str):
            result = version_skew_handler.worker_supports_feature(
                worker_id, "heartbeat"
            )
            results.append((worker_id, result))

        await asyncio.gather(*[check_feature(f"worker-{idx}") for idx in range(10)])

        assert len(results) == 10
        assert all(supports for _, supports in results)


# =============================================================================
# ManagerVersionSkewHandler Tests - Edge Cases
# =============================================================================


class TestManagerVersionSkewHandlerEdgeCases:
    """Edge case tests for ManagerVersionSkewHandler."""

    def test_empty_capabilities(self, version_skew_handler):
        """Handle negotiation with empty capabilities."""
        worker_id = "worker-empty-caps"
        empty_caps = NodeCapabilities(
            protocol_version=CURRENT_PROTOCOL_VERSION,
            capabilities=set(),
        )

        result = version_skew_handler.negotiate_with_worker(worker_id, empty_caps)

        assert result.compatible is True
        assert len(result.common_features) == 0

    def test_re_negotiate_updates_capabilities(self, version_skew_handler):
        """Re-negotiating updates stored capabilities."""
        worker_id = "worker-renegotiate"

        # First negotiation with 1.0
        v1_caps = NodeCapabilities(
            protocol_version=ProtocolVersion(1, 0),
            capabilities=get_features_for_version(ProtocolVersion(1, 0)),
        )
        result1 = version_skew_handler.negotiate_with_worker(worker_id, v1_caps)

        # Re-negotiate with current version
        current_caps = NodeCapabilities.current()
        result2 = version_skew_handler.negotiate_with_worker(worker_id, current_caps)

        # Second result should have more features
        assert len(result2.common_features) >= len(result1.common_features)

    def test_protocol_version_property(self, version_skew_handler):
        """protocol_version property returns correct version."""
        assert version_skew_handler.protocol_version == CURRENT_PROTOCOL_VERSION

    def test_capabilities_property(self, version_skew_handler):
        """capabilities property returns correct set."""
        caps = version_skew_handler.capabilities
        assert isinstance(caps, set)
        assert "heartbeat" in caps

    def test_get_capabilities_none_for_unknown(self, version_skew_handler):
        """get_*_capabilities returns None for unknown nodes."""
        assert version_skew_handler.get_worker_capabilities("unknown") is None
        assert version_skew_handler.get_gate_capabilities("unknown") is None
        assert version_skew_handler.get_peer_capabilities("unknown") is None
