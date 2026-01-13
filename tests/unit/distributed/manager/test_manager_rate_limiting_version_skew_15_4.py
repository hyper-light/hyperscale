"""
Unit tests for Manager Rate Limiting and Version Skew modules from AD-24 and AD-25.

Tests cover:
- ManagerRateLimitingCoordinator (AD-24)
- ManagerVersionSkewHandler (AD-25)

Each test class validates:
- Happy path (normal operations)
- Negative path (invalid inputs, error conditions)
- Failure modes (exception handling)
- Concurrency and race conditions
- Edge cases (boundary conditions, special values)
"""

import asyncio
import pytest
import time
from unittest.mock import MagicMock, AsyncMock, patch

from hyperscale.distributed.nodes.manager.rate_limiting import (
    ManagerRateLimitingCoordinator,
)
from hyperscale.distributed.nodes.manager.version_skew import ManagerVersionSkewHandler
from hyperscale.distributed.nodes.manager.config import ManagerConfig
from hyperscale.distributed.nodes.manager.state import ManagerState
from hyperscale.distributed.reliability.overload import (
    HybridOverloadDetector,
    OverloadState,
)
from hyperscale.distributed.reliability.priority import RequestPriority
from hyperscale.distributed.reliability.rate_limiting import RateLimitResult
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
def overload_detector():
    """Create a HybridOverloadDetector."""
    return HybridOverloadDetector()


@pytest.fixture
def rate_limiting_coordinator(
    manager_state, manager_config, mock_logger, mock_task_runner, overload_detector
):
    """Create a ManagerRateLimitingCoordinator."""
    return ManagerRateLimitingCoordinator(
        state=manager_state,
        config=manager_config,
        logger=mock_logger,
        node_id="manager-test-123",
        task_runner=mock_task_runner,
        overload_detector=overload_detector,
    )


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
# ManagerRateLimitingCoordinator Tests - Happy Path
# =============================================================================


class TestManagerRateLimitingCoordinatorHappyPath:
    """Happy path tests for ManagerRateLimitingCoordinator."""

    def test_initialization(self, rate_limiting_coordinator, overload_detector):
        """Coordinator initializes correctly."""
        assert rate_limiting_coordinator._server_limiter is not None
        assert rate_limiting_coordinator._cooperative_limiter is not None
        assert rate_limiting_coordinator._cleanup_task is None
        assert rate_limiting_coordinator.overload_detector is overload_detector

    @pytest.mark.asyncio
    async def test_check_rate_limit_allows_request(self, rate_limiting_coordinator):
        """check_rate_limit allows requests within limits."""
        result = await rate_limiting_coordinator.check_rate_limit(
            client_id="client-1",
            operation="job_submit",
            priority=RequestPriority.NORMAL,
        )

        assert isinstance(result, RateLimitResult)
        assert result.allowed is True
        assert result.retry_after_seconds == 0.0

    @pytest.mark.asyncio
    async def test_check_rate_limit_critical_always_allowed(
        self, rate_limiting_coordinator
    ):
        """CRITICAL priority requests are always allowed."""
        for idx in range(200):
            await rate_limiting_coordinator.check_rate_limit(
                client_id="client-1",
                operation="job_submit",
                priority=RequestPriority.NORMAL,
            )

        result = await rate_limiting_coordinator.check_rate_limit(
            client_id="client-1",
            operation="job_submit",
            priority=RequestPriority.CRITICAL,
        )
        assert result.allowed is True

    @pytest.mark.asyncio
    async def test_check_simple_allows_request(self, rate_limiting_coordinator):
        """check_simple provides simple rate limiting."""
        result = await rate_limiting_coordinator.check_simple(("192.168.1.1", 5000))
        assert result is True

    @pytest.mark.asyncio
    async def test_check_rate_limit_async(self, rate_limiting_coordinator):
        """Async rate limit check works."""
        result = await rate_limiting_coordinator.check_rate_limit_async(
            client_id="client-1",
            operation="heartbeat",
            priority=RequestPriority.NORMAL,
            max_wait=0.0,
        )

        assert isinstance(result, RateLimitResult)
        assert result.allowed is True

    @pytest.mark.asyncio
    async def test_get_metrics(self, rate_limiting_coordinator):
        """get_metrics returns server and cooperative metrics."""
        await rate_limiting_coordinator.check_rate_limit(
            client_id="client-1",
            operation="job_submit",
        )

        metrics = rate_limiting_coordinator.get_metrics()

        assert "server" in metrics
        assert "cooperative" in metrics
        assert metrics["server"]["total_requests"] >= 1

    @pytest.mark.asyncio
    async def test_get_client_stats(self, rate_limiting_coordinator):
        """get_client_stats returns operation stats for client."""
        await rate_limiting_coordinator.check_rate_limit(
            client_id="client-stats",
            operation="job_submit",
        )
        await rate_limiting_coordinator.check_rate_limit(
            client_id="client-stats",
            operation="heartbeat",
        )

        stats = rate_limiting_coordinator.get_client_stats("client-stats")

        assert "job_submit" in stats
        assert "heartbeat" in stats

    @pytest.mark.asyncio
    async def test_reset_client(self, rate_limiting_coordinator):
        """reset_client clears client rate limit state."""
        client_id = "client-to-reset"

        for idx in range(10):
            await rate_limiting_coordinator.check_rate_limit(
                client_id=client_id,
                operation="job_submit",
            )

        rate_limiting_coordinator.reset_client(client_id)

        result = await rate_limiting_coordinator.check_rate_limit(
            client_id=client_id,
            operation="job_submit",
        )
        assert result.allowed is True


# =============================================================================
# ManagerRateLimitingCoordinator Tests - Negative Path
# =============================================================================


class TestManagerRateLimitingCoordinatorNegativePath:
    """Negative path tests for ManagerRateLimitingCoordinator."""

    def test_check_rate_limit_rejects_when_exhausted(self, rate_limiting_coordinator):
        """Rate limit rejects requests when limit exhausted."""
        client_id = "flood-client"

        # Exhaust the rate limit for job_submit (50 per 10s window)
        for idx in range(60):
            rate_limiting_coordinator.check_rate_limit(
                client_id=client_id,
                operation="job_submit",
                priority=RequestPriority.NORMAL,
            )

        # Next request should be rejected
        result = rate_limiting_coordinator.check_rate_limit(
            client_id=client_id,
            operation="job_submit",
            priority=RequestPriority.NORMAL,
        )

        assert result.allowed is False
        assert result.retry_after_seconds > 0

    def test_is_outbound_blocked_initially_false(self, rate_limiting_coordinator):
        """Outbound operations are not blocked initially."""
        assert rate_limiting_coordinator.is_outbound_blocked("job_submit") is False

    def test_handle_rate_limit_response_blocks_outbound(
        self, rate_limiting_coordinator, mock_task_runner
    ):
        """handle_rate_limit_response blocks outbound operations."""
        operation = "sync_state"
        retry_after = 5.0

        rate_limiting_coordinator.handle_rate_limit_response(operation, retry_after)

        assert rate_limiting_coordinator.is_outbound_blocked(operation) is True
        assert rate_limiting_coordinator.get_outbound_retry_after(operation) > 0

        # Verify warning was logged
        mock_task_runner.run.assert_called()


# =============================================================================
# ManagerRateLimitingCoordinator Tests - Cooperative Rate Limiting
# =============================================================================


class TestManagerRateLimitingCoordinatorCooperative:
    """Tests for cooperative rate limiting behavior."""

    @pytest.mark.asyncio
    async def test_wait_if_outbound_limited_no_wait(self, rate_limiting_coordinator):
        """wait_if_outbound_limited returns immediately when not blocked."""
        waited = await rate_limiting_coordinator.wait_if_outbound_limited("job_submit")
        assert waited == 0.0

    @pytest.mark.asyncio
    async def test_wait_if_outbound_limited_waits_when_blocked(
        self, rate_limiting_coordinator
    ):
        """wait_if_outbound_limited waits when operation is blocked."""
        operation = "stats_update"
        short_wait = 0.1

        rate_limiting_coordinator.handle_rate_limit_response(operation, short_wait)

        start = time.monotonic()
        waited = await rate_limiting_coordinator.wait_if_outbound_limited(operation)
        elapsed = time.monotonic() - start

        assert waited >= short_wait * 0.9  # Allow small timing variance
        assert elapsed >= short_wait * 0.9


# =============================================================================
# ManagerRateLimitingCoordinator Tests - Cleanup Loop
# =============================================================================


class TestManagerRateLimitingCoordinatorCleanup:
    """Tests for cleanup loop functionality."""

    @pytest.mark.asyncio
    async def test_start_cleanup_loop(self, rate_limiting_coordinator):
        """start_cleanup_loop creates and starts cleanup task."""
        assert rate_limiting_coordinator._cleanup_task is None

        await rate_limiting_coordinator.start_cleanup_loop()

        assert rate_limiting_coordinator._cleanup_task is not None
        assert not rate_limiting_coordinator._cleanup_task.done()

        # Cleanup
        await rate_limiting_coordinator.stop_cleanup_loop()

    @pytest.mark.asyncio
    async def test_start_cleanup_loop_idempotent(self, rate_limiting_coordinator):
        """Starting cleanup loop twice doesn't create duplicate tasks."""
        await rate_limiting_coordinator.start_cleanup_loop()
        first_task = rate_limiting_coordinator._cleanup_task

        await rate_limiting_coordinator.start_cleanup_loop()
        second_task = rate_limiting_coordinator._cleanup_task

        assert first_task is second_task

        await rate_limiting_coordinator.stop_cleanup_loop()

    @pytest.mark.asyncio
    async def test_stop_cleanup_loop(self, rate_limiting_coordinator):
        """stop_cleanup_loop cancels and clears cleanup task."""
        await rate_limiting_coordinator.start_cleanup_loop()
        assert rate_limiting_coordinator._cleanup_task is not None

        await rate_limiting_coordinator.stop_cleanup_loop()

        assert rate_limiting_coordinator._cleanup_task is None

    @pytest.mark.asyncio
    async def test_stop_cleanup_loop_no_task(self, rate_limiting_coordinator):
        """stop_cleanup_loop is safe when no task exists."""
        await rate_limiting_coordinator.stop_cleanup_loop()
        assert rate_limiting_coordinator._cleanup_task is None

    def test_cleanup_inactive_clients(self, rate_limiting_coordinator):
        """cleanup_inactive_clients removes stale client state."""
        # This is a pass-through to the underlying limiter
        cleaned = rate_limiting_coordinator.cleanup_inactive_clients()
        assert cleaned >= 0


# =============================================================================
# ManagerRateLimitingCoordinator Tests - Concurrency
# =============================================================================


class TestManagerRateLimitingCoordinatorConcurrency:
    """Concurrency tests for ManagerRateLimitingCoordinator."""

    @pytest.mark.asyncio
    async def test_concurrent_rate_limit_checks(self, rate_limiting_coordinator):
        """Multiple concurrent rate limit checks work correctly."""
        results = []

        async def check_limit(client_id: str):
            result = rate_limiting_coordinator.check_rate_limit(
                client_id=client_id,
                operation="heartbeat",
            )
            results.append((client_id, result.allowed))

        # Run concurrent checks for different clients
        await asyncio.gather(*[check_limit(f"client-{idx}") for idx in range(20)])

        assert len(results) == 20
        # All should be allowed (different clients, first request each)
        assert all(allowed for _, allowed in results)

    @pytest.mark.asyncio
    async def test_concurrent_async_checks(self, rate_limiting_coordinator):
        """Async rate limit checks handle concurrency."""
        client_id = "concurrent-client"

        async def async_check():
            return await rate_limiting_coordinator.check_rate_limit_async(
                client_id=client_id,
                operation="stats_update",
                priority=RequestPriority.NORMAL,
                max_wait=0.1,
            )

        results = await asyncio.gather(*[async_check() for _ in range(10)])

        # Most should succeed (stats_update has high limit)
        allowed_count = sum(1 for r in results if r.allowed)
        assert allowed_count >= 5


# =============================================================================
# ManagerRateLimitingCoordinator Tests - Edge Cases
# =============================================================================


class TestManagerRateLimitingCoordinatorEdgeCases:
    """Edge case tests for ManagerRateLimitingCoordinator."""

    def test_empty_client_id(self, rate_limiting_coordinator):
        """Empty client ID is handled."""
        result = rate_limiting_coordinator.check_rate_limit(
            client_id="",
            operation="job_submit",
        )
        assert isinstance(result, RateLimitResult)

    def test_unknown_operation(self, rate_limiting_coordinator):
        """Unknown operations use default limits."""
        result = rate_limiting_coordinator.check_rate_limit(
            client_id="client-1",
            operation="unknown_operation_xyz",
        )
        assert result.allowed is True

    def test_get_client_stats_unknown_client(self, rate_limiting_coordinator):
        """get_client_stats returns empty dict for unknown client."""
        stats = rate_limiting_coordinator.get_client_stats("nonexistent-client")
        assert stats == {}

    def test_reset_unknown_client(self, rate_limiting_coordinator):
        """reset_client handles unknown client gracefully."""
        # Should not raise
        rate_limiting_coordinator.reset_client("nonexistent-client")

    def test_get_outbound_retry_after_not_blocked(self, rate_limiting_coordinator):
        """get_outbound_retry_after returns 0 when not blocked."""
        retry_after = rate_limiting_coordinator.get_outbound_retry_after("not_blocked")
        assert retry_after == 0.0


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


# =============================================================================
# Integration Tests
# =============================================================================


class TestRateLimitingAndVersionSkewIntegration:
    """Integration tests combining rate limiting and version skew."""

    def test_both_coordinators_share_state(
        self, manager_state, manager_config, mock_logger, mock_task_runner
    ):
        """Both coordinators can use the same state."""
        overload_detector = HybridOverloadDetector()

        rate_limiter = ManagerRateLimitingCoordinator(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
            overload_detector=overload_detector,
        )

        version_handler = ManagerVersionSkewHandler(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
        )

        # Rate limiter should work
        result = rate_limiter.check_rate_limit("client-1", "job_submit")
        assert result.allowed is True

        # Version handler should also work
        caps = NodeCapabilities.current()
        negotiated = version_handler.negotiate_with_gate("gate-1", caps)
        assert negotiated.compatible is True

        # Both affect state
        assert "gate-1" in manager_state._gate_negotiated_caps
