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
from hyperscale.distributed.nodes.manager.models.manager_config import ManagerConfig
from hyperscale.distributed.nodes.manager.state import ManagerState
from hyperscale.distributed.env import Env
from hyperscale.distributed.slo import SLOConfig
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
        rate_limit_cleanup_interval_seconds=300.0,
    )


@pytest.fixture
def manager_state():
    """Create a ManagerState instance."""
    return ManagerState(slo_config=SLOConfig.from_env(Env()))


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

    async def test_negotiate_with_gate(self, version_skew_handler, manager_state):
        """Negotiate with gate stores capabilities in state."""
        gate_id = "gate-123"
        remote_caps = NodeCapabilities.current()

        result = await version_skew_handler.negotiate_with_gate(gate_id, remote_caps)

        assert result.compatible is True
        assert gate_id in manager_state._gate_negotiated_caps

    async def test_gate_supports_feature(self, version_skew_handler):
        """Check if gate supports feature after negotiation."""
        gate_id = "gate-feature"
        remote_caps = NodeCapabilities.current()

        await version_skew_handler.negotiate_with_gate(gate_id, remote_caps)

        assert version_skew_handler.gate_supports_feature(gate_id, "heartbeat") is True

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

    async def test_negotiate_with_gate_incompatible_version(self, version_skew_handler):
        """Gate negotiation fails with incompatible version."""
        gate_id = "gate-incompat"
        incompatible_version = ProtocolVersion(CURRENT_PROTOCOL_VERSION.major + 1, 0)
        remote_caps = NodeCapabilities(
            protocol_version=incompatible_version,
            capabilities=set(),
        )

        with pytest.raises(ValueError):
            await version_skew_handler.negotiate_with_gate(gate_id, remote_caps)

    def test_gate_supports_feature_not_negotiated(self, version_skew_handler):
        """Feature check returns False for non-negotiated gate."""
        assert (
            version_skew_handler.gate_supports_feature("nonexistent-gate", "heartbeat")
            is False
        )

# =============================================================================
# ManagerVersionSkewHandler Tests - Node Removal
# =============================================================================


class TestManagerVersionSkewHandlerRemoval:
    """Tests for node capability removal."""

    async def test_remove_gate(self, version_skew_handler, manager_state):
        """remove_gate clears gate capabilities from handler and state."""
        gate_id = "gate-to-remove"
        remote_caps = NodeCapabilities.current()

        await version_skew_handler.negotiate_with_gate(gate_id, remote_caps)
        assert gate_id in manager_state._gate_negotiated_caps

        version_skew_handler.remove_gate(gate_id)
        assert version_skew_handler.get_gate_capabilities(gate_id) is None
        assert gate_id not in manager_state._gate_negotiated_caps

    def test_remove_nonexistent_gate(self, version_skew_handler):
        """remove_gate handles nonexistent gate gracefully."""
        version_skew_handler.remove_gate("nonexistent")

# =============================================================================
# ManagerVersionSkewHandler Tests - Feature Queries
# =============================================================================


class TestManagerVersionSkewHandlerFeatureQueries:
    """Tests for feature query methods."""

    async def test_get_common_features_with_all_gates(self, version_skew_handler):
        """Get features common to all gates."""
        # No gates initially
        common = version_skew_handler.get_common_features_with_all_gates()
        assert common == set()

        # Add gates
        await version_skew_handler.negotiate_with_gate("gate-1", NodeCapabilities.current())
        await version_skew_handler.negotiate_with_gate("gate-2", NodeCapabilities.current())

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
        assert metrics["gate_count"] == 0

# =============================================================================
# ManagerVersionSkewHandler Tests - Concurrency
# =============================================================================


class TestManagerVersionSkewHandlerConcurrency:
    """Concurrency tests for ManagerVersionSkewHandler."""

# =============================================================================
# ManagerVersionSkewHandler Tests - Edge Cases
# =============================================================================


class TestManagerVersionSkewHandlerEdgeCases:
    """Edge case tests for ManagerVersionSkewHandler."""

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
        assert version_skew_handler.get_gate_capabilities("unknown") is None
