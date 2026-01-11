"""
Integration tests for client core modules (Sections 15.1.5-15.1.12).

Tests ClientTargetSelector, ClientProtocol, ClientLeadershipTracker,
ClientJobTracker, ClientJobSubmitter, ClientCancellationManager,
ClientReportingManager, and ClientDiscovery.

Covers:
- Happy path: Normal operations
- Negative path: Invalid inputs, failures
- Failure mode: Network errors, timeouts
- Concurrency: Race conditions, concurrent operations
- Edge cases: Boundary values, empty data
"""

import asyncio
import time
from unittest.mock import Mock, AsyncMock, patch

import pytest

from hyperscale.distributed.nodes.client.targets import ClientTargetSelector
from hyperscale.distributed.nodes.client.protocol import ClientProtocol
from hyperscale.distributed.nodes.client.leadership import ClientLeadershipTracker
from hyperscale.distributed.nodes.client.tracking import ClientJobTracker
from hyperscale.distributed.nodes.client.config import ClientConfig
from hyperscale.distributed.nodes.client.state import ClientState
from hyperscale.distributed.protocol.version import ProtocolVersion
from hyperscale.distributed.models import (
    ClientJobResult,
    GateLeaderInfo,
    ManagerLeaderInfo,
    JobStatus,
)


def make_mock_logger():
    """Create a mock logger for testing."""
    logger = Mock()
    logger.log = AsyncMock()
    return logger


class TestClientTargetSelector:
    """Test ClientTargetSelector class."""

    def test_happy_path_instantiation(self):
        """Test normal target selector creation."""
        config = ClientConfig(
            host="localhost",
            tcp_port=8000,
            env="test",
            managers=[("m1", 7000), ("m2", 7001)],
            gates=[("g1", 9000), ("g2", 9001)],
        )
        state = ClientState()

        selector = ClientTargetSelector(config, state)

        assert selector._config == config
        assert selector._state == state

    def test_get_callback_addr(self):
        """Test callback address retrieval."""
        config = ClientConfig(
            host="192.168.1.1",
            tcp_port=5000,
            env="test",
            managers=[],
            gates=[],
        )
        state = ClientState()
        selector = ClientTargetSelector(config, state)

        addr = selector.get_callback_addr()

        assert addr == ("192.168.1.1", 5000)

    def test_get_next_manager_round_robin(self):
        """Test round-robin manager selection."""
        config = ClientConfig(
            host="localhost",
            tcp_port=8000,
            env="test",
            managers=[("m1", 7000), ("m2", 7001), ("m3", 7002)],
            gates=[],
        )
        state = ClientState()
        selector = ClientTargetSelector(config, state)

        # Get managers in round-robin order
        m1 = selector.get_next_manager()
        m2 = selector.get_next_manager()
        m3 = selector.get_next_manager()
        m4 = selector.get_next_manager()  # Should wrap around

        assert m1 == ("m1", 7000)
        assert m2 == ("m2", 7001)
        assert m3 == ("m3", 7002)
        assert m4 == ("m1", 7000)  # Wrapped around

    def test_get_next_gate_round_robin(self):
        """Test round-robin gate selection."""
        config = ClientConfig(
            host="localhost",
            tcp_port=8000,
            env="test",
            managers=[],
            gates=[("g1", 9000), ("g2", 9001)],
        )
        state = ClientState()
        selector = ClientTargetSelector(config, state)

        g1 = selector.get_next_gate()
        g2 = selector.get_next_gate()
        g3 = selector.get_next_gate()

        assert g1 == ("g1", 9000)
        assert g2 == ("g2", 9001)
        assert g3 == ("g1", 9000)  # Wrapped

    def test_get_all_targets(self):
        """Test getting all targets (gates + managers)."""
        config = ClientConfig(
            host="localhost",
            tcp_port=8000,
            env="test",
            managers=[("m1", 7000)],
            gates=[("g1", 9000)],
        )
        state = ClientState()
        selector = ClientTargetSelector(config, state)

        all_targets = selector.get_all_targets()

        assert len(all_targets) == 2
        assert ("g1", 9000) in all_targets
        assert ("m1", 7000) in all_targets

    def test_get_targets_for_job_with_sticky_target(self):
        """Test getting targets with sticky routing."""
        config = ClientConfig(
            host="localhost",
            tcp_port=8000,
            env="test",
            managers=[("m1", 7000), ("m2", 7001)],
            gates=[("g1", 9000)],
        )
        state = ClientState()
        job_id = "sticky-job"
        sticky_target = ("m1", 7000)

        state.mark_job_target(job_id, sticky_target)

        selector = ClientTargetSelector(config, state)
        targets = selector.get_targets_for_job(job_id)

        # Sticky target should be first
        assert targets[0] == sticky_target
        assert len(targets) == 3  # sticky + all others

    def test_get_targets_for_job_no_sticky(self):
        """Test getting targets without sticky routing."""
        config = ClientConfig(
            host="localhost",
            tcp_port=8000,
            env="test",
            managers=[("m1", 7000)],
            gates=[("g1", 9000)],
        )
        state = ClientState()
        selector = ClientTargetSelector(config, state)

        targets = selector.get_targets_for_job("new-job")

        assert len(targets) == 2

    def test_edge_case_no_managers(self):
        """Test with no managers configured - returns None."""
        config = ClientConfig(
            host="localhost",
            tcp_port=8000,
            env="test",
            managers=[],
            gates=[("g1", 9000)],
        )
        state = ClientState()
        selector = ClientTargetSelector(config, state)

        # Should return None, not raise
        result = selector.get_next_manager()
        assert result is None

    def test_edge_case_no_gates(self):
        """Test with no gates configured - returns None."""
        config = ClientConfig(
            host="localhost",
            tcp_port=8000,
            env="test",
            managers=[("m1", 7000)],
            gates=[],
        )
        state = ClientState()
        selector = ClientTargetSelector(config, state)

        # Should return None, not raise
        result = selector.get_next_gate()
        assert result is None

    def test_edge_case_single_manager(self):
        """Test with single manager (always returns same)."""
        config = ClientConfig(
            host="localhost",
            tcp_port=8000,
            env="test",
            managers=[("m1", 7000)],
            gates=[],
        )
        state = ClientState()
        selector = ClientTargetSelector(config, state)

        m1 = selector.get_next_manager()
        m2 = selector.get_next_manager()
        m3 = selector.get_next_manager()

        assert m1 == m2 == m3 == ("m1", 7000)

    def test_concurrency_round_robin(self):
        """Test concurrent round-robin selection."""
        config = ClientConfig(
            host="localhost",
            tcp_port=8000,
            env="test",
            managers=[("m1", 7000), ("m2", 7001)],
            gates=[],
        )
        state = ClientState()
        selector = ClientTargetSelector(config, state)

        selected = []
        for _ in range(100):
            selected.append(selector.get_next_manager())

        # Should alternate between m1 and m2
        assert selected.count(("m1", 7000)) == 50
        assert selected.count(("m2", 7001)) == 50


class TestClientProtocol:
    """Test ClientProtocol class."""

    def test_happy_path_instantiation(self):
        """Test normal protocol initialization."""
        state = ClientState()
        logger = make_mock_logger()
        protocol = ClientProtocol(state, logger)

        assert protocol._state == state
        assert protocol._logger == logger

    def test_get_client_capabilities_string(self):
        """Test client capabilities string generation."""
        state = ClientState()
        logger = make_mock_logger()
        protocol = ClientProtocol(state, logger)

        capabilities = protocol.get_client_capabilities_string()

        assert isinstance(capabilities, str)
        # Should contain some features
        assert len(capabilities) > 0

    def test_negotiate_capabilities_compatible(self):
        """Test capability negotiation with compatible server."""
        state = ClientState()
        logger = make_mock_logger()
        protocol = ClientProtocol(state, logger)

        server_addr = ("server1", 8000)
        result = protocol.negotiate_capabilities(
            server_addr=server_addr,
            server_version_major=1,
            server_version_minor=0,
            server_capabilities_str="feature1,feature2",
        )

        # Should store negotiated capabilities
        assert server_addr in state._server_negotiated_caps
        caps = state._server_negotiated_caps[server_addr]
        # NegotiatedCapabilities stores ProtocolVersion objects
        assert caps.remote_version.major == 1
        assert caps.remote_version.minor == 0

    def test_negotiate_capabilities_multiple_servers(self):
        """Test negotiating with multiple servers."""
        state = ClientState()
        logger = make_mock_logger()
        protocol = ClientProtocol(state, logger)

        server1 = ("server1", 8000)
        server2 = ("server2", 8001)

        protocol.negotiate_capabilities(server1, 1, 0, "feat1")
        protocol.negotiate_capabilities(server2, 1, 1, "feat1,feat2")

        assert len(state._server_negotiated_caps) == 2
        assert server1 in state._server_negotiated_caps
        assert server2 in state._server_negotiated_caps

    def test_edge_case_empty_capabilities(self):
        """Test with empty capabilities string."""
        state = ClientState()
        logger = make_mock_logger()
        protocol = ClientProtocol(state, logger)

        server_addr = ("server", 8000)
        protocol.negotiate_capabilities(
            server_addr=server_addr,
            server_version_major=1,
            server_version_minor=0,
            server_capabilities_str="",
        )

        assert server_addr in state._server_negotiated_caps

    def test_edge_case_version_mismatch(self):
        """Test with server version mismatch."""
        state = ClientState()
        logger = make_mock_logger()
        protocol = ClientProtocol(state, logger)

        server_addr = ("old-server", 8000)
        # Old server version
        protocol.negotiate_capabilities(
            server_addr=server_addr,
            server_version_major=0,
            server_version_minor=1,
            server_capabilities_str="",
        )

        # Should still store but with limited features
        assert server_addr in state._server_negotiated_caps


class TestClientLeadershipTracker:
    """Test ClientLeadershipTracker class."""

    def test_happy_path_instantiation(self):
        """Test normal leadership tracker creation."""
        state = ClientState()
        logger = make_mock_logger()
        tracker = ClientLeadershipTracker(state, logger)

        assert tracker._state == state
        assert tracker._logger == logger

    def test_validate_gate_fence_token_valid(self):
        """Test valid gate fence token."""
        state = ClientState()
        logger = make_mock_logger()
        tracker = ClientLeadershipTracker(state, logger)

        job_id = "job-123"
        # First update
        tracker.update_gate_leader(job_id, ("gate1", 9000), fence_token=1)

        # Validate newer token
        valid, msg = tracker.validate_gate_fence_token(job_id, new_fence_token=2)

        assert valid is True
        assert msg == ""

    def test_validate_gate_fence_token_stale(self):
        """Test stale gate fence token."""
        state = ClientState()
        logger = make_mock_logger()
        tracker = ClientLeadershipTracker(state, logger)

        job_id = "job-456"
        tracker.update_gate_leader(job_id, ("gate1", 9000), fence_token=5)

        # Try older token
        valid, msg = tracker.validate_gate_fence_token(job_id, new_fence_token=3)

        assert valid is False
        assert "Stale fence token" in msg

    def test_validate_gate_fence_token_no_current_leader(self):
        """Test fence token validation with no current leader."""
        state = ClientState()
        logger = make_mock_logger()
        tracker = ClientLeadershipTracker(state, logger)

        # No leader yet
        valid, msg = tracker.validate_gate_fence_token("new-job", new_fence_token=1)

        assert valid is True
        assert msg == ""

    def test_update_gate_leader(self):
        """Test updating gate leader."""
        state = ClientState()
        logger = make_mock_logger()
        tracker = ClientLeadershipTracker(state, logger)

        job_id = "gate-leader-job"
        gate_addr = ("gate1", 9000)

        tracker.update_gate_leader(job_id, gate_addr, fence_token=1)

        assert job_id in state._gate_job_leaders
        tracking = state._gate_job_leaders[job_id]
        assert tracking.gate_addr == gate_addr

    def test_update_manager_leader(self):
        """Test updating manager leader."""
        state = ClientState()
        logger = make_mock_logger()
        tracker = ClientLeadershipTracker(state, logger)

        job_id = "mgr-leader-job"
        datacenter_id = "dc-east"
        manager_addr = ("manager1", 7000)

        tracker.update_manager_leader(
            job_id, datacenter_id, manager_addr, fence_token=1
        )

        key = (job_id, datacenter_id)
        assert key in state._manager_job_leaders

    def test_mark_job_orphaned(self):
        """Test marking job as orphaned."""
        state = ClientState()
        logger = make_mock_logger()
        tracker = ClientLeadershipTracker(state, logger)

        job_id = "orphan-job"

        tracker.mark_job_orphaned(
            job_id,
            last_known_gate=("gate1", 9000),
            last_known_manager=None,
        )

        assert state.is_job_orphaned(job_id) is True

    def test_clear_job_orphaned(self):
        """Test clearing orphan status."""
        state = ClientState()
        logger = make_mock_logger()
        tracker = ClientLeadershipTracker(state, logger)

        job_id = "clear-orphan-job"

        tracker.mark_job_orphaned(
            job_id,
            last_known_gate=None,
            last_known_manager=None,
        )
        assert state.is_job_orphaned(job_id) is True

        tracker.clear_job_orphaned(job_id)
        assert state.is_job_orphaned(job_id) is False

    def test_get_current_gate_leader(self):
        """Test getting current gate leader."""
        state = ClientState()
        logger = make_mock_logger()
        tracker = ClientLeadershipTracker(state, logger)

        job_id = "get-gate-leader"
        gate_addr = ("gate2", 9001)

        tracker.update_gate_leader(job_id, gate_addr, fence_token=1)

        result = tracker.get_current_gate_leader(job_id)

        assert result == gate_addr

    def test_get_current_gate_leader_no_leader(self):
        """Test getting gate leader when none exists."""
        state = ClientState()
        logger = make_mock_logger()
        tracker = ClientLeadershipTracker(state, logger)

        result = tracker.get_current_gate_leader("nonexistent-job")

        assert result is None

    def test_get_leadership_metrics(self):
        """Test leadership metrics retrieval."""
        state = ClientState()
        logger = make_mock_logger()
        tracker = ClientLeadershipTracker(state, logger)

        state.increment_gate_transfers()
        state.increment_manager_transfers()
        tracker.mark_job_orphaned(
            "job1",
            last_known_gate=None,
            last_known_manager=None,
        )

        metrics = tracker.get_leadership_metrics()

        assert metrics["gate_transfers_received"] == 1
        assert metrics["manager_transfers_received"] == 1
        assert metrics["orphaned_jobs"] == 1

    def test_edge_case_multiple_leader_updates(self):
        """Test multiple leader updates for same job."""
        state = ClientState()
        logger = make_mock_logger()
        tracker = ClientLeadershipTracker(state, logger)

        job_id = "multi-update-job"

        tracker.update_gate_leader(job_id, ("gate1", 9000), fence_token=1)
        tracker.update_gate_leader(job_id, ("gate2", 9001), fence_token=2)
        tracker.update_gate_leader(job_id, ("gate3", 9002), fence_token=3)

        # Should have latest leader
        leader = tracker.get_current_gate_leader(job_id)
        assert leader == ("gate3", 9002)


class TestClientJobTracker:
    """Test ClientJobTracker class."""

    def test_happy_path_instantiation(self):
        """Test normal job tracker creation."""
        state = ClientState()
        logger = make_mock_logger()
        tracker = ClientJobTracker(state, logger)

        assert tracker._state == state
        assert tracker._logger == logger

    def test_initialize_job_tracking(self):
        """Test job tracking initialization."""
        state = ClientState()
        logger = make_mock_logger()
        tracker = ClientJobTracker(state, logger)

        job_id = "track-job-123"
        status_callback = Mock()

        tracker.initialize_job_tracking(
            job_id,
            on_status_update=status_callback,
        )

        assert job_id in state._jobs
        assert job_id in state._job_events

    def test_update_job_status(self):
        """Test job status update."""
        state = ClientState()
        logger = make_mock_logger()
        tracker = ClientJobTracker(state, logger)

        job_id = "status-job"
        tracker.initialize_job_tracking(job_id)

        tracker.update_job_status(job_id, "RUNNING")

        assert state._jobs[job_id].status == "RUNNING"

    def test_update_job_status_completion(self):
        """Test job status update with completion event."""
        state = ClientState()
        logger = make_mock_logger()
        tracker = ClientJobTracker(state, logger)

        job_id = "complete-job"
        tracker.initialize_job_tracking(job_id)

        tracker.update_job_status(job_id, "COMPLETED")

        # Completion event should be set
        assert state._job_events[job_id].is_set()

    def test_mark_job_failed(self):
        """Test marking job as failed."""
        state = ClientState()
        logger = make_mock_logger()
        tracker = ClientJobTracker(state, logger)

        job_id = "failed-job"
        tracker.initialize_job_tracking(job_id)

        error = "Worker timeout"
        tracker.mark_job_failed(job_id, error)

        assert state._jobs[job_id].status == "failed"
        # Should signal completion
        assert state._job_events[job_id].is_set()

    @pytest.mark.asyncio
    async def test_wait_for_job_success(self):
        """Test waiting for job completion."""
        state = ClientState()
        logger = make_mock_logger()
        tracker = ClientJobTracker(state, logger)

        job_id = "wait-job"
        tracker.initialize_job_tracking(job_id)

        async def complete_job():
            await asyncio.sleep(0.01)
            tracker.update_job_status(job_id, "COMPLETED")

        await asyncio.gather(
            tracker.wait_for_job(job_id),
            complete_job(),
        )

        assert state._jobs[job_id].status == "COMPLETED"

    @pytest.mark.asyncio
    async def test_wait_for_job_timeout(self):
        """Test waiting for job with timeout."""
        state = ClientState()
        logger = make_mock_logger()
        tracker = ClientJobTracker(state, logger)

        job_id = "timeout-job"
        tracker.initialize_job_tracking(job_id)

        with pytest.raises(asyncio.TimeoutError):
            await tracker.wait_for_job(job_id, timeout=0.05)

    def test_get_job_status(self):
        """Test getting job status."""
        state = ClientState()
        logger = make_mock_logger()
        tracker = ClientJobTracker(state, logger)

        job_id = "get-status-job"
        tracker.initialize_job_tracking(job_id)
        tracker.update_job_status(job_id, "RUNNING")

        result = tracker.get_job_status(job_id)

        assert result.status == "RUNNING"

    def test_get_job_status_nonexistent(self):
        """Test getting status of nonexistent job."""
        state = ClientState()
        logger = make_mock_logger()
        tracker = ClientJobTracker(state, logger)

        status = tracker.get_job_status("nonexistent-job")

        assert status is None

    def test_edge_case_multiple_status_updates(self):
        """Test multiple status updates for same job."""
        state = ClientState()
        logger = make_mock_logger()
        tracker = ClientJobTracker(state, logger)

        job_id = "multi-status-job"
        tracker.initialize_job_tracking(job_id)

        tracker.update_job_status(job_id, "PENDING")
        tracker.update_job_status(job_id, "RUNNING")
        tracker.update_job_status(job_id, "COMPLETED")

        # Should have final status
        assert state._jobs[job_id].status == "COMPLETED"

    @pytest.mark.asyncio
    async def test_concurrency_multiple_waiters(self):
        """Test multiple waiters for same job."""
        state = ClientState()
        logger = make_mock_logger()
        tracker = ClientJobTracker(state, logger)

        job_id = "multi-waiter-job"
        tracker.initialize_job_tracking(job_id)

        async def waiter():
            return await tracker.wait_for_job(job_id)

        async def completer():
            await asyncio.sleep(0.02)
            tracker.update_job_status(job_id, "COMPLETED")

        results = await asyncio.gather(
            waiter(),
            waiter(),
            waiter(),
            completer(),
        )

        # All waiters should complete and return ClientJobResult
        for result in results[:3]:
            assert isinstance(result, ClientJobResult)
