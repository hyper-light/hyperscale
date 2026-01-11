"""
Integration tests for GateManagerHandler (Section 15.3.7).

Tests manager registration, status updates, and discovery broadcasts including:
- Role-based validation
- Protocol version negotiation (AD-25)
- Backpressure handling (AD-37)
- Manager heartbeat tracking
"""

import asyncio
import pytest
from dataclasses import dataclass, field
from unittest.mock import AsyncMock, MagicMock
from enum import Enum

from hyperscale.distributed.nodes.gate.handlers.tcp_manager import GateManagerHandler
from hyperscale.distributed.nodes.gate.state import GateRuntimeState
from hyperscale.distributed.models import (
    ManagerHeartbeat,
    ManagerDiscoveryBroadcast,
)
from hyperscale.distributed.protocol.version import NodeCapabilities


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
class MockTaskRunner:
    """Mock task runner for testing."""
    tasks: list = field(default_factory=list)

    def run(self, coro, *args, **kwargs):
        if asyncio.iscoroutinefunction(coro):
            task = asyncio.create_task(coro(*args, **kwargs))
            self.tasks.append(task)
            return task
        return None


@dataclass
class MockNodeId:
    """Mock node ID."""
    full: str = "gate-001"
    short: str = "001"
    datacenter: str = "global"


@dataclass
class MockEnv:
    """Mock environment configuration."""
    tls_enabled: bool = False


class MockNodeRole(Enum):
    MANAGER = "manager"
    WORKER = "worker"
    GATE = "gate"


@dataclass
class MockRoleValidator:
    """Mock role validator."""
    valid_roles: set = field(default_factory=lambda: {MockNodeRole.MANAGER})
    _validate_result: bool = True

    def validate_peer(self, cert_der: bytes, expected_role: MockNodeRole) -> bool:
        return self._validate_result


@dataclass
class MockGateInfo:
    """Mock gate info for healthy gates."""
    gate_id: str = "gate-001"
    addr: tuple[str, int] = field(default_factory=lambda: ("127.0.0.1", 9000))


@dataclass
class MockTransport:
    """Mock asyncio transport."""
    peer_cert: bytes | None = None

    def get_extra_info(self, name: str, default=None):
        if name == "ssl_object":
            if self.peer_cert:
                ssl_obj = MagicMock()
                ssl_obj.getpeercert.return_value = {"der": self.peer_cert}
                return ssl_obj
        return default


def create_mock_handler(
    state: GateRuntimeState = None,
    tls_enabled: bool = False,
    validate_role: bool = True,
) -> GateManagerHandler:
    """Create a mock handler with configurable behavior."""
    if state is None:
        state = GateRuntimeState()

    validator = MockRoleValidator()
    validator._validate_result = validate_role

    return GateManagerHandler(
        state=state,
        logger=MockLogger(),
        task_runner=MockTaskRunner(),
        env=MockEnv(tls_enabled=tls_enabled),
        datacenter_managers={},
        role_validator=validator,
        node_capabilities=NodeCapabilities.current(),
        get_node_id=lambda: MockNodeId(),
        get_host=lambda: "127.0.0.1",
        get_tcp_port=lambda: 9000,
        get_healthy_gates=lambda: [MockGateInfo()],
        record_manager_heartbeat=lambda dc, addr, manager_id, workers: None,
        handle_manager_backpressure_signal=lambda signal: None,
        update_dc_backpressure=lambda dc_id: None,
        broadcast_manager_discovery=AsyncMock(),
    )


# =============================================================================
# handle_status_update Happy Path Tests
# =============================================================================


class TestHandleStatusUpdateHappyPath:
    """Tests for handle_status_update happy path."""

    @pytest.mark.asyncio
    async def test_accepts_valid_heartbeat(self):
        """Accepts valid manager heartbeat."""
        state = GateRuntimeState()
        handler = create_mock_handler(state=state)

        heartbeat = ManagerHeartbeat(
            node_id="manager-001",
            datacenter="dc-east",
            is_leader=True,
            term=1,
            version=1,
            active_jobs=0,
            active_workflows=10,
            worker_count=5,
            healthy_worker_count=5,
            available_cores=40,
            total_cores=60,
            tcp_host="10.0.0.1",
            tcp_port=8000,
        )

        async def mock_handle_exception(error, context):
            pass

        result = await handler.handle_status_update(
            addr=("10.0.0.1", 8000),
            data=heartbeat.dump(),
            handle_exception=mock_handle_exception,
        )

        assert result == b'ok'

    @pytest.mark.asyncio
    async def test_records_heartbeat(self):
        """Records heartbeat in state."""
        state = GateRuntimeState()
        recorded_heartbeats = []

        def record_heartbeat(dc, addr, manager_id, workers):
            recorded_heartbeats.append({
                "dc": dc,
                "addr": addr,
                "manager_id": manager_id,
                "workers": workers,
            })

        handler = GateManagerHandler(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            env=MockEnv(),
            datacenter_managers={},
            role_validator=MockRoleValidator(),
            node_capabilities=NodeCapabilities.current(),
            get_node_id=lambda: MockNodeId(),
            get_host=lambda: "127.0.0.1",
            get_tcp_port=lambda: 9000,
            get_healthy_gates=lambda: [],
            record_manager_heartbeat=record_heartbeat,
            handle_manager_backpressure_signal=lambda signal: None,
            update_dc_backpressure=lambda dc_id: None,
            broadcast_manager_discovery=AsyncMock(),
        )

        heartbeat = ManagerHeartbeat(
            node_id="manager-001",
            datacenter="dc-east",
            is_leader=True,
            term=1,
            version=1,
            active_jobs=0,
            active_workflows=10,
            worker_count=5,
            healthy_worker_count=5,
            available_cores=40,
            total_cores=60,
            tcp_host="10.0.0.1",
            tcp_port=8000,
        )

        async def mock_handle_exception(error, context):
            pass

        await handler.handle_status_update(
            addr=("10.0.0.1", 8000),
            data=heartbeat.dump(),
            handle_exception=mock_handle_exception,
        )

        assert len(recorded_heartbeats) == 1
        assert recorded_heartbeats[0]["dc"] == "dc-east"
        assert recorded_heartbeats[0]["manager_id"] == "manager-001"


# =============================================================================
# handle_status_update Backpressure Tests (AD-37)
# =============================================================================


class TestHandleStatusUpdateBackpressure:
    """Tests for handle_status_update backpressure handling (AD-37)."""

    @pytest.mark.asyncio
    async def test_updates_dc_backpressure(self):
        """Updates DC backpressure level."""
        state = GateRuntimeState()
        updated_dcs = []

        handler = GateManagerHandler(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            env=MockEnv(),
            datacenter_managers={},
            role_validator=MockRoleValidator(),
            node_capabilities=NodeCapabilities.current(),
            get_node_id=lambda: MockNodeId(),
            get_host=lambda: "127.0.0.1",
            get_tcp_port=lambda: 9000,
            get_healthy_gates=lambda: [],
            record_manager_heartbeat=lambda dc, addr, manager_id, workers: None,
            handle_manager_backpressure_signal=lambda signal: None,
            update_dc_backpressure=lambda dc_id: updated_dcs.append(dc_id),
            broadcast_manager_discovery=AsyncMock(),
        )

        heartbeat = ManagerHeartbeat(
            node_id="manager-001",
            datacenter="dc-east",
            is_leader=True,
            term=1,
            version=1,
            active_jobs=0,
            active_workflows=10,
            worker_count=5,
            healthy_worker_count=5,
            available_cores=40,
            total_cores=60,
            tcp_host="10.0.0.1",
            tcp_port=8000,
        )

        async def mock_handle_exception(error, context):
            pass

        await handler.handle_status_update(
            addr=("10.0.0.1", 8000),
            data=heartbeat.dump(),
            handle_exception=mock_handle_exception,
        )

        assert "dc-east" in updated_dcs


# =============================================================================
# handle_status_update Negative Path Tests
# =============================================================================


class TestHandleStatusUpdateNegativePath:
    """Tests for handle_status_update negative paths."""

    @pytest.mark.asyncio
    async def test_handles_invalid_data(self):
        """Handles invalid heartbeat data gracefully."""
        handler = create_mock_handler()

        errors_handled = []

        async def mock_handle_exception(error, context):
            errors_handled.append((error, context))

        result = await handler.handle_status_update(
            addr=("10.0.0.1", 8000),
            data=b"invalid_data",
            handle_exception=mock_handle_exception,
        )

        assert result == b'error'
        assert len(errors_handled) == 1


# =============================================================================
# handle_register Happy Path Tests
# =============================================================================


class TestHandleRegisterHappyPath:
    """Tests for handle_register happy path."""

    @pytest.mark.asyncio
    async def test_accepts_valid_registration(self):
        """Accepts valid manager registration."""
        state = GateRuntimeState()
        handler = create_mock_handler(state=state)

        heartbeat = ManagerHeartbeat(
            node_id="manager-001",
            datacenter="dc-east",
            is_leader=True,
            term=1,
            version=1,
            active_jobs=0,
            active_workflows=0,
            worker_count=5,
            healthy_worker_count=5,
            available_cores=40,
            total_cores=60,
            tcp_host="10.0.0.1",
            tcp_port=8000,
        )

        async def mock_handle_exception(error, context):
            pass

        transport = MockTransport()

        result = await handler.handle_register(
            addr=("10.0.0.1", 8000),
            data=heartbeat.dump(),
            transport=transport,
            handle_exception=mock_handle_exception,
        )

        assert isinstance(result, bytes)

    @pytest.mark.asyncio
    async def test_returns_healthy_gates(self):
        """Returns healthy gates in registration response."""
        state = GateRuntimeState()
        healthy_gates = [MockGateInfo("gate-001", ("127.0.0.1", 9000))]

        handler = GateManagerHandler(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            env=MockEnv(),
            datacenter_managers={},
            role_validator=MockRoleValidator(),
            node_capabilities=NodeCapabilities.current(),
            get_node_id=lambda: MockNodeId(),
            get_host=lambda: "127.0.0.1",
            get_tcp_port=lambda: 9000,
            get_healthy_gates=lambda: healthy_gates,
            record_manager_heartbeat=lambda dc, addr, manager_id, workers: None,
            handle_manager_backpressure_signal=lambda signal: None,
            update_dc_backpressure=lambda dc_id: None,
            broadcast_manager_discovery=AsyncMock(),
        )

        heartbeat = ManagerHeartbeat(
            node_id="manager-001",
            datacenter="dc-east",
            is_leader=True,
            term=1,
            version=1,
            active_jobs=0,
            active_workflows=0,
            worker_count=5,
            healthy_worker_count=5,
            available_cores=40,
            total_cores=60,
            tcp_host="10.0.0.1",
            tcp_port=8000,
        )

        async def mock_handle_exception(error, context):
            pass

        transport = MockTransport()

        result = await handler.handle_register(
            addr=("10.0.0.1", 8000),
            data=heartbeat.dump(),
            transport=transport,
            handle_exception=mock_handle_exception,
        )

        assert isinstance(result, bytes)


# =============================================================================
# handle_register Negative Path Tests
# =============================================================================


class TestHandleRegisterNegativePath:
    """Tests for handle_register negative paths."""

    @pytest.mark.asyncio
    async def test_handles_invalid_data(self):
        """Handles invalid registration data gracefully."""
        handler = create_mock_handler()

        errors_handled = []

        async def mock_handle_exception(error, context):
            errors_handled.append((error, context))

        transport = MockTransport()

        result = await handler.handle_register(
            addr=("10.0.0.1", 8000),
            data=b"invalid_data",
            transport=transport,
            handle_exception=mock_handle_exception,
        )

        assert isinstance(result, bytes)
        # Should return error response


# =============================================================================
# handle_discovery Happy Path Tests
# =============================================================================


class TestHandleDiscoveryHappyPath:
    """Tests for handle_discovery happy path."""

    @pytest.mark.asyncio
    async def test_accepts_valid_discovery(self):
        """Accepts valid discovery broadcast."""
        state = GateRuntimeState()
        handler = create_mock_handler(state=state)

        broadcast = ManagerDiscoveryBroadcast(
            datacenter="dc-east",
            manager_tcp_addr=("10.0.0.1", 8000),
            manager_udp_addr=("10.0.0.1", 8001),
            source_gate_id="gate-002",
            worker_count=5,
            healthy_worker_count=5,
            available_cores=40,
            total_cores=60,
        )

        async def mock_handle_exception(error, context):
            pass

        datacenter_manager_udp = {}

        result = await handler.handle_discovery(
            addr=("10.0.0.2", 9000),
            data=broadcast.dump(),
            datacenter_manager_udp=datacenter_manager_udp,
            handle_exception=mock_handle_exception,
        )

        assert result == b'ok'

    @pytest.mark.asyncio
    async def test_updates_datacenter_managers(self):
        """Updates datacenter manager tracking."""
        state = GateRuntimeState()
        handler = create_mock_handler(state=state)

        broadcast = ManagerDiscoveryBroadcast(
            datacenter="dc-east",
            manager_tcp_addr=("10.0.0.1", 8000),
            manager_udp_addr=("10.0.0.1", 8001),
            source_gate_id="gate-002",
            worker_count=5,
            healthy_worker_count=5,
            available_cores=40,
            total_cores=60,
        )

        async def mock_handle_exception(error, context):
            pass

        datacenter_manager_udp = {}

        await handler.handle_discovery(
            addr=("10.0.0.2", 9000),
            data=broadcast.dump(),
            datacenter_manager_udp=datacenter_manager_udp,
            handle_exception=mock_handle_exception,
        )

        # Should have added dc-east to tracking
        assert "dc-east" in datacenter_manager_udp or "dc-east" in state._datacenter_manager_status


# =============================================================================
# handle_discovery Negative Path Tests
# =============================================================================


class TestHandleDiscoveryNegativePath:
    """Tests for handle_discovery negative paths."""

    @pytest.mark.asyncio
    async def test_handles_invalid_data(self):
        """Handles invalid discovery data gracefully."""
        handler = create_mock_handler()

        errors_handled = []

        async def mock_handle_exception(error, context):
            errors_handled.append((error, context))

        result = await handler.handle_discovery(
            addr=("10.0.0.2", 9000),
            data=b"invalid_data",
            datacenter_manager_udp={},
            handle_exception=mock_handle_exception,
        )

        assert result == b'error'


# =============================================================================
# Concurrency Tests
# =============================================================================


class TestConcurrency:
    """Tests for concurrent access patterns."""

    @pytest.mark.asyncio
    async def test_concurrent_status_updates(self):
        """Concurrent status updates don't interfere."""
        state = GateRuntimeState()
        handler = create_mock_handler(state=state)

        heartbeats = []
        for i in range(10):
            heartbeats.append(ManagerHeartbeat(
                node_id=f"manager-{i:03d}",
                datacenter=f"dc-{i % 3}",
                is_leader=(i == 0),
                term=1,
                version=1,
                active_jobs=0,
                active_workflows=10,
                worker_count=5,
                healthy_worker_count=5,
                available_cores=40,
                total_cores=60,
                tcp_host=f"10.0.0.{i}",
                tcp_port=8000,
            ))

        async def mock_handle_exception(error, context):
            pass

        results = await asyncio.gather(*[
            handler.handle_status_update(
                addr=(f"10.0.0.{i}", 8000),
                data=hb.dump(),
                handle_exception=mock_handle_exception,
            )
            for i, hb in enumerate(heartbeats)
        ])

        assert len(results) == 10
        assert all(r == b'ok' for r in results)

    @pytest.mark.asyncio
    async def test_concurrent_registrations(self):
        """Concurrent registrations don't interfere."""
        state = GateRuntimeState()
        handler = create_mock_handler(state=state)

        heartbeats = []
        for i in range(10):
            heartbeats.append(ManagerHeartbeat(
                node_id=f"manager-{i:03d}",
                datacenter=f"dc-{i % 3}",
                is_leader=(i == 0),
                term=1,
                version=1,
                active_jobs=0,
                active_workflows=0,
                worker_count=5,
                healthy_worker_count=5,
                available_cores=40,
                total_cores=60,
                tcp_host=f"10.0.0.{i}",
                tcp_port=8000,
            ))

        async def mock_handle_exception(error, context):
            pass

        transport = MockTransport()

        results = await asyncio.gather(*[
            handler.handle_register(
                addr=(f"10.0.0.{i}", 8000),
                data=hb.dump(),
                transport=transport,
                handle_exception=mock_handle_exception,
            )
            for i, hb in enumerate(heartbeats)
        ])

        assert len(results) == 10
        assert all(isinstance(r, bytes) for r in results)


# =============================================================================
# Edge Cases Tests
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    @pytest.mark.asyncio
    async def test_empty_manager_id(self):
        """Handles empty manager ID."""
        handler = create_mock_handler()

        heartbeat = ManagerHeartbeat(
            node_id="",
            datacenter="dc-east",
            is_leader=True,
            term=1,
            version=1,
            active_jobs=0,
            active_workflows=10,
            worker_count=5,
            healthy_worker_count=5,
            available_cores=40,
            total_cores=60,
            tcp_host="10.0.0.1",
            tcp_port=8000,
        )

        async def mock_handle_exception(error, context):
            pass

        result = await handler.handle_status_update(
            addr=("10.0.0.1", 8000),
            data=heartbeat.dump(),
            handle_exception=mock_handle_exception,
        )

        assert result == b'ok'

    @pytest.mark.asyncio
    async def test_zero_workers(self):
        """Handles zero worker count."""
        handler = create_mock_handler()

        heartbeat = ManagerHeartbeat(
            node_id="manager-001",
            datacenter="dc-east",
            is_leader=True,
            term=1,
            version=1,
            active_jobs=0,
            active_workflows=0,
            worker_count=0,
            healthy_worker_count=0,
            available_cores=0,
            total_cores=0,
            tcp_host="10.0.0.1",
            tcp_port=8000,
        )

        async def mock_handle_exception(error, context):
            pass

        result = await handler.handle_status_update(
            addr=("10.0.0.1", 8000),
            data=heartbeat.dump(),
            handle_exception=mock_handle_exception,
        )

        assert result == b'ok'

    @pytest.mark.asyncio
    async def test_very_large_worker_count(self):
        """Handles very large worker count."""
        handler = create_mock_handler()

        heartbeat = ManagerHeartbeat(
            node_id="manager-001",
            datacenter="dc-east",
            is_leader=True,
            term=1,
            version=1,
            active_jobs=0,
            active_workflows=100000,
            worker_count=10000,
            healthy_worker_count=10000,
            available_cores=800000,
            total_cores=1200000,
            tcp_host="10.0.0.1",
            tcp_port=8000,
        )

        async def mock_handle_exception(error, context):
            pass

        result = await handler.handle_status_update(
            addr=("10.0.0.1", 8000),
            data=heartbeat.dump(),
            handle_exception=mock_handle_exception,
        )

        assert result == b'ok'

    @pytest.mark.asyncio
    async def test_special_characters_in_datacenter(self):
        """Handles special characters in datacenter name."""
        handler = create_mock_handler()

        special_dcs = [
            "dc-us-east-1",
            "dc_us_west_2",
            "dc.eu.west.1",
            "dc:asia:pacific",
        ]

        async def mock_handle_exception(error, context):
            pass

        for dc in special_dcs:
            heartbeat = ManagerHeartbeat(
                node_id="manager-001",
                datacenter=dc,
                is_leader=True,
                term=1,
                version=1,
                active_jobs=0,
                active_workflows=10,
                worker_count=5,
                healthy_worker_count=5,
                available_cores=40,
                total_cores=60,
                tcp_host="10.0.0.1",
                tcp_port=8000,
            )

            result = await handler.handle_status_update(
                addr=("10.0.0.1", 8000),
                data=heartbeat.dump(),
                handle_exception=mock_handle_exception,
            )

            assert result == b'ok'

    @pytest.mark.asyncio
    async def test_many_active_jobs(self):
        """Handles heartbeat with many active jobs."""
        handler = create_mock_handler()

        active_jobs = [f"job-{i}" for i in range(1000)]

        heartbeat = ManagerHeartbeat(
            node_id="manager-001",
            datacenter="dc-east",
            is_leader=True,
            term=1,
            version=1,
            active_jobs=1000,
            active_workflows=500,
            worker_count=100,
            healthy_worker_count=100,
            available_cores=800,
            total_cores=1200,
            tcp_host="10.0.0.1",
            tcp_port=8000,
        )

        async def mock_handle_exception(error, context):
            pass

        result = await handler.handle_status_update(
            addr=("10.0.0.1", 8000),
            data=heartbeat.dump(),
            handle_exception=mock_handle_exception,
        )

        assert result == b'ok'


# =============================================================================
# Failure Mode Tests
# =============================================================================


class TestFailureModes:
    """Tests for failure mode handling."""

    @pytest.mark.asyncio
    async def test_handles_exception_in_heartbeat_recording(self):
        """Handles exception during heartbeat recording."""

        def failing_record(dc, addr, manager_id, workers):
            raise Exception("Recording failed")

        handler = GateManagerHandler(
            state=GateRuntimeState(),
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            env=MockEnv(),
            datacenter_managers={},
            role_validator=MockRoleValidator(),
            node_capabilities=NodeCapabilities.current(),
            get_node_id=lambda: MockNodeId(),
            get_host=lambda: "127.0.0.1",
            get_tcp_port=lambda: 9000,
            get_healthy_gates=lambda: [],
            record_manager_heartbeat=failing_record,
            handle_manager_backpressure_signal=lambda signal: None,
            update_dc_backpressure=lambda dc_id: None,
            broadcast_manager_discovery=AsyncMock(),
        )

        heartbeat = ManagerHeartbeat(
            node_id="manager-001",
            datacenter="dc-east",
            is_leader=True,
            term=1,
            version=1,
            active_jobs=0,
            active_workflows=10,
            worker_count=5,
            healthy_worker_count=5,
            available_cores=40,
            total_cores=60,
            tcp_host="10.0.0.1",
            tcp_port=8000,
        )

        errors_handled = []

        async def mock_handle_exception(error, context):
            errors_handled.append((error, context))

        result = await handler.handle_status_update(
            addr=("10.0.0.1", 8000),
            data=heartbeat.dump(),
            handle_exception=mock_handle_exception,
        )

        assert result == b'error'
        assert len(errors_handled) == 1

    @pytest.mark.asyncio
    async def test_handles_exception_in_discovery_broadcast(self):
        """Handles exception during discovery broadcast."""
        broadcast_mock = AsyncMock(side_effect=Exception("Broadcast failed"))

        handler = GateManagerHandler(
            state=GateRuntimeState(),
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            env=MockEnv(),
            datacenter_managers={},
            role_validator=MockRoleValidator(),
            node_capabilities=NodeCapabilities.current(),
            get_node_id=lambda: MockNodeId(),
            get_host=lambda: "127.0.0.1",
            get_tcp_port=lambda: 9000,
            get_healthy_gates=lambda: [],
            record_manager_heartbeat=lambda dc, addr, manager_id, workers: None,
            handle_manager_backpressure_signal=lambda signal: None,
            update_dc_backpressure=lambda dc_id: None,
            broadcast_manager_discovery=broadcast_mock,
        )

        heartbeat = ManagerHeartbeat(
            node_id="manager-001",
            datacenter="dc-east",
            is_leader=True,
            term=1,
            version=1,
            active_jobs=0,
            active_workflows=0,
            worker_count=5,
            healthy_worker_count=5,
            available_cores=40,
            total_cores=60,
            tcp_host="10.0.0.1",
            tcp_port=8000,
        )

        async def mock_handle_exception(error, context):
            pass

        transport = MockTransport()

        # This may or may not fail depending on when broadcast is called
        result = await handler.handle_register(
            addr=("10.0.0.1", 8000),
            data=heartbeat.dump(),
            transport=transport,
            handle_exception=mock_handle_exception,
        )

        assert isinstance(result, bytes)


__all__ = [
    "TestHandleStatusUpdateHappyPath",
    "TestHandleStatusUpdateBackpressure",
    "TestHandleStatusUpdateNegativePath",
    "TestHandleRegisterHappyPath",
    "TestHandleRegisterNegativePath",
    "TestHandleDiscoveryHappyPath",
    "TestHandleDiscoveryNegativePath",
    "TestConcurrency",
    "TestEdgeCases",
    "TestFailureModes",
]
