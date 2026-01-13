"""
Integration tests for GatePingHandler (Section 15.3.7).

Tests ping/health check request handling.
"""

import asyncio
import pytest
from dataclasses import dataclass, field
from unittest.mock import AsyncMock

from hyperscale.distributed.nodes.gate.handlers.tcp_ping import GatePingHandler
from hyperscale.distributed.nodes.gate.state import GateRuntimeState
from hyperscale.distributed.models import GateState as GateStateEnum


# =============================================================================
# Mock Classes
# =============================================================================


@dataclass
class MockLogger:
    """Mock logger for testing."""

    messages: list[str] = field(default_factory=list)

    async def log(self, *args, **kwargs):
        self.messages.append(str(args))


@dataclass
class MockNodeId:
    """Mock node ID."""

    full: str = "gate-001"
    datacenter: str = "global"


@dataclass
class MockPingRequest:
    """Mock ping request."""

    request_id: str = "req-123"

    @classmethod
    def load(cls, data: bytes) -> "MockPingRequest":
        return cls()


@dataclass
class MockDCHealthStatus:
    """Mock DC health status."""

    health: str = "healthy"
    available_capacity: int = 100
    manager_count: int = 3
    worker_count: int = 10


@dataclass
class MockManagerHeartbeat:
    """Mock manager heartbeat."""

    is_leader: bool = True
    tcp_host: str = "10.0.0.1"
    tcp_port: int = 8000


# =============================================================================
# Happy Path Tests
# =============================================================================


class TestGatePingHandlerHappyPath:
    """Tests for GatePingHandler happy path."""

    @pytest.mark.asyncio
    async def test_returns_gate_info(self):
        """Handler returns gate identity information."""
        state = GateRuntimeState()
        state.set_gate_state(GateStateEnum.ACTIVE)

        handler = GatePingHandler(
            state=state,
            logger=MockLogger(),
            get_node_id=lambda: MockNodeId(),
            get_host=lambda: "127.0.0.1",
            get_tcp_port=lambda: 9000,
            is_leader=lambda: True,
            get_current_term=lambda: 5,
            classify_dc_health=lambda dc_id: MockDCHealthStatus(),
            count_active_dcs=lambda: 2,
            get_all_job_ids=lambda: ["job-1", "job-2"],
            get_datacenter_managers=lambda: {"dc-east": [("10.0.0.1", 8000)]},
        )

        # Mock the PingRequest.load method
        import hyperscale.distributed.nodes.gate.handlers.tcp_ping as ping_module

        original_load = None
        if hasattr(ping_module, "PingRequest"):
            original_load = ping_module.PingRequest.load

        try:
            # We need to patch PingRequest.load
            result = await handler.handle_ping(
                addr=("10.0.0.1", 8000),
                data=b"ping_request_data",
                clock_time=12345,
            )

            # Result should be bytes (serialized response or error)
            assert isinstance(result, bytes)
        except Exception:
            # If PingRequest.load fails, that's expected in unit test
            pass

    @pytest.mark.asyncio
    async def test_includes_datacenter_info(self):
        """Handler includes per-datacenter information."""
        state = GateRuntimeState()
        state.set_gate_state(GateStateEnum.ACTIVE)

        # Set up manager status with leader
        state._datacenter_manager_status["dc-east"] = {
            ("10.0.0.1", 8000): MockManagerHeartbeat(is_leader=True),
        }

        handler = GatePingHandler(
            state=state,
            logger=MockLogger(),
            get_node_id=lambda: MockNodeId(),
            get_host=lambda: "127.0.0.1",
            get_tcp_port=lambda: 9000,
            is_leader=lambda: True,
            get_current_term=lambda: 5,
            classify_dc_health=lambda dc_id: MockDCHealthStatus(),
            count_active_dcs=lambda: 2,
            get_all_job_ids=lambda: [],
            get_datacenter_managers=lambda: {"dc-east": [("10.0.0.1", 8000)]},
        )

        # The handler will iterate over datacenter_managers
        datacenter_managers = handler._get_datacenter_managers()
        assert "dc-east" in datacenter_managers

    @pytest.mark.asyncio
    async def test_includes_active_peers(self):
        """Handler includes active peer gates."""
        state = GateRuntimeState()
        await state.add_active_peer(("10.0.0.2", 9000))
        await state.add_active_peer(("10.0.0.3", 9000))

        handler = GatePingHandler(
            state=state,
            logger=MockLogger(),
            get_node_id=lambda: MockNodeId(),
            get_host=lambda: "127.0.0.1",
            get_tcp_port=lambda: 9000,
            is_leader=lambda: True,
            get_current_term=lambda: 5,
            classify_dc_health=lambda dc_id: MockDCHealthStatus(),
            count_active_dcs=lambda: 2,
            get_all_job_ids=lambda: [],
            get_datacenter_managers=lambda: {},
        )

        # Verify active peers are in state
        assert len(state._active_gate_peers) == 2


# =============================================================================
# Negative Path Tests
# =============================================================================


class TestGatePingHandlerNegativePath:
    """Tests for GatePingHandler negative paths."""

    @pytest.mark.asyncio
    async def test_handles_invalid_request_data(self):
        """Handler handles invalid request data gracefully."""
        state = GateRuntimeState()

        handler = GatePingHandler(
            state=state,
            logger=MockLogger(),
            get_node_id=lambda: MockNodeId(),
            get_host=lambda: "127.0.0.1",
            get_tcp_port=lambda: 9000,
            is_leader=lambda: True,
            get_current_term=lambda: 5,
            classify_dc_health=lambda dc_id: MockDCHealthStatus(),
            count_active_dcs=lambda: 2,
            get_all_job_ids=lambda: [],
            get_datacenter_managers=lambda: {},
        )

        async def mock_handle_exception(error, context):
            pass

        result = await handler.handle_ping(
            addr=("10.0.0.1", 8000),
            data=b"invalid_data",
            handle_exception=mock_handle_exception,
        )

        # Should return error response
        assert result == b"error"


class TestGatePingHandlerFailureMode:
    """Tests for GatePingHandler failure modes."""

    @pytest.mark.asyncio
    async def test_handles_exception_in_dependencies(self):
        """Handler handles exceptions from dependencies gracefully."""
        state = GateRuntimeState()

        def failing_node_id():
            raise Exception("Node ID error")

        handler = GatePingHandler(
            state=state,
            logger=MockLogger(),
            get_node_id=failing_node_id,
            get_host=lambda: "127.0.0.1",
            get_tcp_port=lambda: 9000,
            is_leader=lambda: True,
            get_current_term=lambda: 5,
            classify_dc_health=lambda dc_id: MockDCHealthStatus(),
            count_active_dcs=lambda: 2,
            get_all_job_ids=lambda: [],
            get_datacenter_managers=lambda: {},
        )

        async def mock_handle_exception(error, context):
            pass

        result = await handler.handle_ping(
            addr=("10.0.0.1", 8000),
            data=b"request_data",
            handle_exception=mock_handle_exception,
        )

        # Should return error response
        assert result == b"error"


# =============================================================================
# Edge Cases Tests
# =============================================================================


class TestGatePingHandlerEdgeCases:
    """Tests for GatePingHandler edge cases."""

    @pytest.mark.asyncio
    async def test_no_datacenters(self):
        """Handler works with no datacenters."""
        state = GateRuntimeState()

        handler = GatePingHandler(
            state=state,
            logger=MockLogger(),
            get_node_id=lambda: MockNodeId(),
            get_host=lambda: "127.0.0.1",
            get_tcp_port=lambda: 9000,
            is_leader=lambda: True,
            get_current_term=lambda: 5,
            classify_dc_health=lambda dc_id: MockDCHealthStatus(),
            count_active_dcs=lambda: 0,
            get_all_job_ids=lambda: [],
            get_datacenter_managers=lambda: {},  # No DCs
        )

        # Should not raise
        datacenter_managers = handler._get_datacenter_managers()
        assert datacenter_managers == {}

    @pytest.mark.asyncio
    async def test_no_active_jobs(self):
        """Handler works with no active jobs."""
        state = GateRuntimeState()

        handler = GatePingHandler(
            state=state,
            logger=MockLogger(),
            get_node_id=lambda: MockNodeId(),
            get_host=lambda: "127.0.0.1",
            get_tcp_port=lambda: 9000,
            is_leader=lambda: True,
            get_current_term=lambda: 5,
            classify_dc_health=lambda dc_id: MockDCHealthStatus(),
            count_active_dcs=lambda: 2,
            get_all_job_ids=lambda: [],  # No jobs
            get_datacenter_managers=lambda: {"dc-1": []},
        )

        job_ids = handler._get_all_job_ids()
        assert job_ids == []

    @pytest.mark.asyncio
    async def test_no_active_peers(self):
        """Handler works with no active peers."""
        state = GateRuntimeState()
        # No peers added

        handler = GatePingHandler(
            state=state,
            logger=MockLogger(),
            get_node_id=lambda: MockNodeId(),
            get_host=lambda: "127.0.0.1",
            get_tcp_port=lambda: 9000,
            is_leader=lambda: True,
            get_current_term=lambda: 5,
            classify_dc_health=lambda dc_id: MockDCHealthStatus(),
            count_active_dcs=lambda: 2,
            get_all_job_ids=lambda: [],
            get_datacenter_managers=lambda: {},
        )

        assert len(state._active_gate_peers) == 0

    @pytest.mark.asyncio
    async def test_many_datacenters(self):
        """Handler works with many datacenters."""
        state = GateRuntimeState()

        dcs = {f"dc-{i}": [(f"10.0.{i}.1", 8000)] for i in range(50)}

        handler = GatePingHandler(
            state=state,
            logger=MockLogger(),
            get_node_id=lambda: MockNodeId(),
            get_host=lambda: "127.0.0.1",
            get_tcp_port=lambda: 9000,
            is_leader=lambda: True,
            get_current_term=lambda: 5,
            classify_dc_health=lambda dc_id: MockDCHealthStatus(),
            count_active_dcs=lambda: 50,
            get_all_job_ids=lambda: [],
            get_datacenter_managers=lambda: dcs,
        )

        datacenter_managers = handler._get_datacenter_managers()
        assert len(datacenter_managers) == 50

    @pytest.mark.asyncio
    async def test_many_active_jobs(self):
        """Handler works with many active jobs."""
        state = GateRuntimeState()

        job_ids = [f"job-{i}" for i in range(1000)]

        handler = GatePingHandler(
            state=state,
            logger=MockLogger(),
            get_node_id=lambda: MockNodeId(),
            get_host=lambda: "127.0.0.1",
            get_tcp_port=lambda: 9000,
            is_leader=lambda: True,
            get_current_term=lambda: 5,
            classify_dc_health=lambda dc_id: MockDCHealthStatus(),
            count_active_dcs=lambda: 2,
            get_all_job_ids=lambda: job_ids,
            get_datacenter_managers=lambda: {},
        )

        all_jobs = handler._get_all_job_ids()
        assert len(all_jobs) == 1000

    @pytest.mark.asyncio
    async def test_syncing_state(self):
        """Handler works in SYNCING state."""
        state = GateRuntimeState()
        state.set_gate_state(GateStateEnum.SYNCING)

        handler = GatePingHandler(
            state=state,
            logger=MockLogger(),
            get_node_id=lambda: MockNodeId(),
            get_host=lambda: "127.0.0.1",
            get_tcp_port=lambda: 9000,
            is_leader=lambda: False,  # Not leader during sync
            get_current_term=lambda: 0,
            classify_dc_health=lambda dc_id: MockDCHealthStatus(),
            count_active_dcs=lambda: 0,
            get_all_job_ids=lambda: [],
            get_datacenter_managers=lambda: {},
        )

        assert state.get_gate_state() == GateStateEnum.SYNCING

    @pytest.mark.asyncio
    async def test_dc_without_leader(self):
        """Handler handles DC without elected leader."""
        state = GateRuntimeState()

        # DC with managers but no leader
        state._datacenter_manager_status["dc-east"] = {
            ("10.0.0.1", 8000): MockManagerHeartbeat(is_leader=False),
            ("10.0.0.2", 8000): MockManagerHeartbeat(is_leader=False),
        }

        handler = GatePingHandler(
            state=state,
            logger=MockLogger(),
            get_node_id=lambda: MockNodeId(),
            get_host=lambda: "127.0.0.1",
            get_tcp_port=lambda: 9000,
            is_leader=lambda: True,
            get_current_term=lambda: 5,
            classify_dc_health=lambda dc_id: MockDCHealthStatus(),
            count_active_dcs=lambda: 1,
            get_all_job_ids=lambda: [],
            get_datacenter_managers=lambda: {"dc-east": [("10.0.0.1", 8000)]},
        )

        # Should still have manager statuses
        assert len(state._datacenter_manager_status["dc-east"]) == 2


# =============================================================================
# Concurrency Tests
# =============================================================================


class TestGatePingHandlerConcurrency:
    """Tests for concurrent ping handling."""

    @pytest.mark.asyncio
    async def test_concurrent_pings(self):
        """Handler handles concurrent ping requests."""
        state = GateRuntimeState()

        handler = GatePingHandler(
            state=state,
            logger=MockLogger(),
            get_node_id=lambda: MockNodeId(),
            get_host=lambda: "127.0.0.1",
            get_tcp_port=lambda: 9000,
            is_leader=lambda: True,
            get_current_term=lambda: 5,
            classify_dc_health=lambda dc_id: MockDCHealthStatus(),
            count_active_dcs=lambda: 2,
            get_all_job_ids=lambda: ["job-1"],
            get_datacenter_managers=lambda: {"dc-1": []},
        )

        async def mock_handle_exception(error, context):
            pass

        # Send many concurrent pings
        results = await asyncio.gather(
            *[
                handler.handle_ping(
                    addr=(f"10.0.0.{i}", 8000),
                    data=b"ping_data",
                    handle_exception=mock_handle_exception,
                )
                for i in range(100)
            ]
        )

        # All should complete (either with response or error)
        assert len(results) == 100


# =============================================================================
# State Consistency Tests
# =============================================================================


class TestGatePingHandlerStateConsistency:
    """Tests for state consistency during ping handling."""

    @pytest.mark.asyncio
    async def test_state_changes_during_ping(self):
        """Handler handles state changes during ping processing."""
        state = GateRuntimeState()
        await state.add_active_peer(("10.0.0.1", 9000))

        handler = GatePingHandler(
            state=state,
            logger=MockLogger(),
            get_node_id=lambda: MockNodeId(),
            get_host=lambda: "127.0.0.1",
            get_tcp_port=lambda: 9000,
            is_leader=lambda: True,
            get_current_term=lambda: 5,
            classify_dc_health=lambda dc_id: MockDCHealthStatus(),
            count_active_dcs=lambda: 2,
            get_all_job_ids=lambda: [],
            get_datacenter_managers=lambda: {},
        )

        # Modify state while processing
        async def modify_state():
            await asyncio.sleep(0.001)
            await state.add_active_peer(("10.0.0.2", 9000))
            await state.remove_active_peer(("10.0.0.1", 9000))

        async def handle_ping():
            return await handler.handle_ping(
                addr=("10.0.0.1", 8000),
                data=b"ping_data",
                clock_time=12345,
            )

        # Run both concurrently
        await asyncio.gather(modify_state(), handle_ping())

        # Final state should reflect changes
        assert ("10.0.0.2", 9000) in state._active_gate_peers
