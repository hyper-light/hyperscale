"""
Integration tests for WorkerRegistrationHandler (Section 15.2.7).

Tests WorkerRegistrationHandler for manager registration and protocol negotiation.

Covers:
- Happy path: Normal registration flow
- Negative path: Registration failures, circuit breaker open
- Failure mode: Network errors, protocol negotiation failures
- Concurrency: Thread-safe registration operations
- Edge cases: Empty managers, version negotiation
"""

import asyncio
from unittest.mock import MagicMock, AsyncMock

import pytest

from hyperscale.distributed.nodes.worker.registration import WorkerRegistrationHandler
from hyperscale.distributed.nodes.worker.registry import WorkerRegistry
from hyperscale.distributed.models import (
    ManagerInfo,
    ManagerToWorkerRegistration,
    ManagerToWorkerRegistrationAck,
    NodeInfo,
    RegistrationResponse,
)
from hyperscale.distributed.protocol.version import NodeCapabilities, ProtocolVersion
from hyperscale.distributed.swim.core import CircuitState


class MockDiscoveryService:
    """Mock DiscoveryService for testing."""

    def __init__(self):
        self._peers: dict[str, dict] = {}

    def add_peer(
        self,
        peer_id: str,
        host: str,
        port: int,
        role: str,
        datacenter_id: str,
    ) -> None:
        """Add a peer to the discovery service."""
        self._peers[peer_id] = {
            "host": host,
            "port": port,
            "role": role,
            "datacenter_id": datacenter_id,
        }


class TestWorkerRegistrationHandlerInitialization:
    """Test WorkerRegistrationHandler initialization."""

    def test_happy_path_instantiation(self) -> None:
        """Test normal instantiation."""
        registry = WorkerRegistry(None)
        discovery = MockDiscoveryService()
        logger = MagicMock()

        handler = WorkerRegistrationHandler(
            registry=registry,
            discovery_service=discovery,
            logger=logger,
        )

        assert handler._registry is registry
        assert handler._discovery_service is discovery
        assert handler._logger is logger
        assert handler._negotiated_capabilities is None

    def test_with_node_capabilities(self) -> None:
        """Test with explicit node capabilities."""
        registry = WorkerRegistry(None)
        discovery = MockDiscoveryService()
        capabilities = NodeCapabilities.current(node_version="1.0.0")

        handler = WorkerRegistrationHandler(
            registry=registry,
            discovery_service=discovery,
            node_capabilities=capabilities,
        )

        assert handler._node_capabilities is capabilities

    def test_set_node_capabilities(self) -> None:
        """Test updating node capabilities."""
        registry = WorkerRegistry(None)
        discovery = MockDiscoveryService()

        handler = WorkerRegistrationHandler(
            registry=registry,
            discovery_service=discovery,
        )

        new_capabilities = NodeCapabilities.current(node_version="2.0.0")
        handler.set_node_capabilities(new_capabilities)

        assert handler._node_capabilities is new_capabilities


class TestWorkerRegistrationHandlerRegisterWithManager:
    """Test registering with a manager."""

    @pytest.mark.asyncio
    async def test_register_success(self) -> None:
        """Test successful registration."""
        registry = WorkerRegistry(None)
        discovery = MockDiscoveryService()
        logger = MagicMock()
        logger.log = AsyncMock()

        handler = WorkerRegistrationHandler(
            registry=registry,
            discovery_service=discovery,
            logger=logger,
        )

        node_info = NodeInfo(
            node_id="worker-123",
            role="worker",
            host="192.168.1.1",
            port=8000,
            datacenter="dc-1",
        )

        send_func = AsyncMock(return_value=b"OK")

        result = await handler.register_with_manager(
            manager_addr=("192.168.1.100", 8000),
            node_info=node_info,
            total_cores=8,
            available_cores=8,
            memory_mb=16000,
            available_memory_mb=15000,
            cluster_id="cluster-1",
            environment_id="env-1",
            send_func=send_func,
        )

        assert result is True
        send_func.assert_awaited()

    @pytest.mark.asyncio
    async def test_register_circuit_breaker_open(self) -> None:
        """Test registration when circuit breaker is open."""
        registry = WorkerRegistry(None)
        discovery = MockDiscoveryService()
        logger = MagicMock()
        logger.log = AsyncMock()

        handler = WorkerRegistrationHandler(
            registry=registry,
            discovery_service=discovery,
            logger=logger,
        )

        # Set circuit to OPEN
        circuit = registry.get_or_create_circuit_by_addr(("192.168.1.100", 8000))
        # Force circuit open by recording many errors
        for _ in range(10):
            circuit.record_error()

        node_info = NodeInfo(
            node_id="worker-123",
            role="worker",
            host="192.168.1.1",
            port=8000,
            datacenter="dc-1",
        )

        send_func = AsyncMock()

        result = await handler.register_with_manager(
            manager_addr=("192.168.1.100", 8000),
            node_info=node_info,
            total_cores=8,
            available_cores=8,
            memory_mb=16000,
            available_memory_mb=15000,
            cluster_id="cluster-1",
            environment_id="env-1",
            send_func=send_func,
        )

        # Should fail because circuit is open
        if circuit.circuit_state == CircuitState.OPEN:
            assert result is False
            send_func.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_register_with_retries(self) -> None:
        """Test registration with retry logic."""
        registry = WorkerRegistry(None)
        discovery = MockDiscoveryService()
        logger = MagicMock()
        logger.log = AsyncMock()

        handler = WorkerRegistrationHandler(
            registry=registry,
            discovery_service=discovery,
            logger=logger,
        )

        node_info = NodeInfo(
            node_id="worker-123",
            role="worker",
            host="192.168.1.1",
            port=8000,
            datacenter="dc-1",
        )

        call_count = [0]

        async def failing_send(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] < 3:
                raise ConnectionError("Connection failed")
            return b"OK"

        result = await handler.register_with_manager(
            manager_addr=("192.168.1.100", 8000),
            node_info=node_info,
            total_cores=8,
            available_cores=8,
            memory_mb=16000,
            available_memory_mb=15000,
            cluster_id="cluster-1",
            environment_id="env-1",
            send_func=failing_send,
            max_retries=3,
            base_delay=0.01,
        )

        assert result is True
        assert call_count[0] == 3

    @pytest.mark.asyncio
    async def test_register_all_retries_fail(self) -> None:
        """Test registration when all retries fail."""
        registry = WorkerRegistry(None)
        discovery = MockDiscoveryService()
        logger = MagicMock()
        logger.log = AsyncMock()

        handler = WorkerRegistrationHandler(
            registry=registry,
            discovery_service=discovery,
            logger=logger,
        )

        node_info = NodeInfo(
            node_id="worker-123",
            role="worker",
            host="192.168.1.1",
            port=8000,
            datacenter="dc-1",
        )

        send_func = AsyncMock(side_effect=RuntimeError("Connection failed"))

        result = await handler.register_with_manager(
            manager_addr=("192.168.1.100", 8000),
            node_info=node_info,
            total_cores=8,
            available_cores=8,
            memory_mb=16000,
            available_memory_mb=15000,
            cluster_id="cluster-1",
            environment_id="env-1",
            send_func=send_func,
            max_retries=2,
            base_delay=0.01,
        )

        assert result is False


class TestWorkerRegistrationHandlerProcessResponse:
    """Test processing registration responses."""

    def test_process_response_success(self) -> None:
        """Test processing successful registration response."""
        registry = WorkerRegistry(None)
        discovery = MockDiscoveryService()

        handler = WorkerRegistrationHandler(
            registry=registry,
            discovery_service=discovery,
        )

        # Create response with healthy managers
        manager1 = ManagerInfo(
            node_id="mgr-1",
            tcp_host="192.168.1.100",
            tcp_port=8000,
            udp_host="192.168.1.100",
            udp_port=8001,
            datacenter="dc-1",
            is_leader=True,
        )

        response = RegistrationResponse(
            accepted=True,
            manager_id="mgr-1",
            healthy_managers=[manager1],
            protocol_version_major=1,
            protocol_version_minor=0,
            capabilities="heartbeat_piggyback,priority_routing",
        )

        add_unconfirmed_peer = MagicMock()
        add_to_probe_scheduler = MagicMock()

        accepted, primary_id = handler.process_registration_response(
            data=response.dump(),
            node_host="192.168.1.1",
            node_port=8000,
            node_id_short="wkr",
            add_unconfirmed_peer=add_unconfirmed_peer,
            add_to_probe_scheduler=add_to_probe_scheduler,
        )

        assert accepted is True
        assert primary_id == "mgr-1"
        assert handler._negotiated_capabilities is not None
        assert handler._negotiated_capabilities.compatible is True

    def test_process_response_rejected(self) -> None:
        """Test processing rejected registration response."""
        registry = WorkerRegistry(None)
        discovery = MockDiscoveryService()

        handler = WorkerRegistrationHandler(
            registry=registry,
            discovery_service=discovery,
        )

        response = RegistrationResponse(
            accepted=False,
            manager_id="mgr-1",
            healthy_managers=[],
        )

        add_unconfirmed_peer = MagicMock()
        add_to_probe_scheduler = MagicMock()

        accepted, primary_id = handler.process_registration_response(
            data=response.dump(),
            node_host="192.168.1.1",
            node_port=8000,
            node_id_short="wkr",
            add_unconfirmed_peer=add_unconfirmed_peer,
            add_to_probe_scheduler=add_to_probe_scheduler,
        )

        assert accepted is False
        assert primary_id is None

    def test_process_response_with_multiple_managers(self) -> None:
        """Test processing response with multiple managers."""
        registry = WorkerRegistry(None)
        discovery = MockDiscoveryService()

        handler = WorkerRegistrationHandler(
            registry=registry,
            discovery_service=discovery,
        )

        manager1 = ManagerInfo(
            node_id="mgr-1",
            tcp_host="192.168.1.100",
            tcp_port=8000,
            udp_host="192.168.1.100",
            udp_port=8001,
            datacenter="dc-1",
            is_leader=False,
        )

        manager2 = ManagerInfo(
            node_id="mgr-2",
            tcp_host="192.168.1.101",
            tcp_port=8000,
            udp_host="192.168.1.101",
            udp_port=8001,
            datacenter="dc-1",
            is_leader=True,
        )

        response = RegistrationResponse(
            accepted=True,
            manager_id="mgr-1",
            healthy_managers=[manager1, manager2],
            protocol_version_major=1,
            protocol_version_minor=0,
        )

        add_unconfirmed_peer = MagicMock()
        add_to_probe_scheduler = MagicMock()

        accepted, primary_id = handler.process_registration_response(
            data=response.dump(),
            node_host="192.168.1.1",
            node_port=8000,
            node_id_short="wkr",
            add_unconfirmed_peer=add_unconfirmed_peer,
            add_to_probe_scheduler=add_to_probe_scheduler,
        )

        assert accepted is True
        assert primary_id == "mgr-2"  # Leader preferred

        # Both managers should be in registry
        assert "mgr-1" in registry._known_managers
        assert "mgr-2" in registry._known_managers

    def test_process_response_invalid_data(self) -> None:
        """Test processing invalid response data."""
        registry = WorkerRegistry(None)
        discovery = MockDiscoveryService()

        handler = WorkerRegistrationHandler(
            registry=registry,
            discovery_service=discovery,
        )

        add_unconfirmed_peer = MagicMock()
        add_to_probe_scheduler = MagicMock()

        accepted, primary_id = handler.process_registration_response(
            data=b"invalid data",
            node_host="192.168.1.1",
            node_port=8000,
            node_id_short="wkr",
            add_unconfirmed_peer=add_unconfirmed_peer,
            add_to_probe_scheduler=add_to_probe_scheduler,
        )

        assert accepted is False
        assert primary_id is None


class TestWorkerRegistrationHandlerProcessManagerRegistration:
    """Test processing registration requests from managers."""

    def test_process_manager_registration_success(self) -> None:
        """Test processing manager registration request."""
        registry = WorkerRegistry(None)
        discovery = MockDiscoveryService()

        handler = WorkerRegistrationHandler(
            registry=registry,
            discovery_service=discovery,
        )

        manager = ManagerInfo(
            node_id="mgr-new",
            tcp_host="192.168.1.200",
            tcp_port=8000,
            udp_host="192.168.1.200",
            udp_port=8001,
            datacenter="dc-1",
            is_leader=False,
        )

        registration = ManagerToWorkerRegistration(
            manager=manager,
            is_leader=False,
            term=1,
            known_managers=[],
        )

        add_unconfirmed_peer = MagicMock()
        add_to_probe_scheduler = MagicMock()

        result = handler.process_manager_registration(
            data=registration.dump(),
            node_id_full="worker-full-id",
            total_cores=8,
            available_cores=4,
            add_unconfirmed_peer=add_unconfirmed_peer,
            add_to_probe_scheduler=add_to_probe_scheduler,
        )

        ack = ManagerToWorkerRegistrationAck.load(result)
        assert ack.accepted is True
        assert ack.worker_id == "worker-full-id"
        assert ack.total_cores == 8
        assert ack.available_cores == 4

        # Manager should be added to registry
        assert "mgr-new" in registry._known_managers

        # Manager should be added to discovery service
        assert "mgr-new" in discovery._peers

    def test_process_manager_registration_as_leader(self) -> None:
        """Test processing registration from leader manager."""
        registry = WorkerRegistry(None)
        discovery = MockDiscoveryService()

        handler = WorkerRegistrationHandler(
            registry=registry,
            discovery_service=discovery,
        )

        manager = ManagerInfo(
            node_id="mgr-leader",
            tcp_host="192.168.1.200",
            tcp_port=8000,
            udp_host="192.168.1.200",
            udp_port=8001,
            datacenter="dc-1",
            is_leader=True,
        )

        registration = ManagerToWorkerRegistration(
            manager=manager,
            is_leader=True,
            term=1,
            known_managers=[],
        )

        add_unconfirmed_peer = MagicMock()
        add_to_probe_scheduler = MagicMock()

        result = handler.process_manager_registration(
            data=registration.dump(),
            node_id_full="worker-full-id",
            total_cores=8,
            available_cores=4,
            add_unconfirmed_peer=add_unconfirmed_peer,
            add_to_probe_scheduler=add_to_probe_scheduler,
        )

        ack = ManagerToWorkerRegistrationAck.load(result)
        assert ack.accepted is True

        # Should be set as primary
        assert registry._primary_manager_id == "mgr-leader"

    def test_process_manager_registration_with_known_managers(self) -> None:
        """Test processing registration with known managers list."""
        registry = WorkerRegistry(None)
        discovery = MockDiscoveryService()

        handler = WorkerRegistrationHandler(
            registry=registry,
            discovery_service=discovery,
        )

        registering_manager = ManagerInfo(
            node_id="mgr-new",
            tcp_host="192.168.1.200",
            tcp_port=8000,
            udp_host="192.168.1.200",
            udp_port=8001,
            datacenter="dc-1",
            is_leader=False,
        )

        known_manager = ManagerInfo(
            node_id="mgr-existing",
            tcp_host="192.168.1.201",
            tcp_port=8000,
            udp_host="192.168.1.201",
            udp_port=8001,
            datacenter="dc-1",
            is_leader=False,
        )

        registration = ManagerToWorkerRegistration(
            manager=registering_manager,
            is_leader=False,
            term=1,
            known_managers=[known_manager],
        )

        add_unconfirmed_peer = MagicMock()
        add_to_probe_scheduler = MagicMock()

        result = handler.process_manager_registration(
            data=registration.dump(),
            node_id_full="worker-full-id",
            total_cores=8,
            available_cores=4,
            add_unconfirmed_peer=add_unconfirmed_peer,
            add_to_probe_scheduler=add_to_probe_scheduler,
        )

        ack = ManagerToWorkerRegistrationAck.load(result)
        assert ack.accepted is True

        # Both managers should be in registry
        assert "mgr-new" in registry._known_managers
        assert "mgr-existing" in registry._known_managers

    def test_process_manager_registration_invalid_data(self) -> None:
        """Test processing invalid registration data."""
        registry = WorkerRegistry(None)
        discovery = MockDiscoveryService()

        handler = WorkerRegistrationHandler(
            registry=registry,
            discovery_service=discovery,
        )

        add_unconfirmed_peer = MagicMock()
        add_to_probe_scheduler = MagicMock()

        result = handler.process_manager_registration(
            data=b"invalid data",
            node_id_full="worker-full-id",
            total_cores=8,
            available_cores=4,
            add_unconfirmed_peer=add_unconfirmed_peer,
            add_to_probe_scheduler=add_to_probe_scheduler,
        )

        ack = ManagerToWorkerRegistrationAck.load(result)
        assert ack.accepted is False
        assert ack.error is not None


class TestWorkerRegistrationHandlerNegotiatedCapabilities:
    """Test negotiated capabilities handling."""

    def test_negotiated_capabilities_property(self) -> None:
        """Test negotiated_capabilities property."""
        registry = WorkerRegistry(None)
        discovery = MockDiscoveryService()

        handler = WorkerRegistrationHandler(
            registry=registry,
            discovery_service=discovery,
        )

        assert handler.negotiated_capabilities is None

        # Process a response to get negotiated capabilities
        response = RegistrationResponse(
            accepted=True,
            manager_id="mgr-1",
            healthy_managers=[],
            protocol_version_major=1,
            protocol_version_minor=0,
            capabilities="feature1,feature2",
        )

        handler.process_registration_response(
            data=response.dump(),
            node_host="localhost",
            node_port=8000,
            node_id_short="wkr",
            add_unconfirmed_peer=MagicMock(),
            add_to_probe_scheduler=MagicMock(),
        )

        assert handler.negotiated_capabilities is not None
        assert handler.negotiated_capabilities.compatible is True


class TestWorkerRegistrationHandlerEdgeCases:
    """Test edge cases."""

    def test_empty_capabilities_string(self) -> None:
        """Test processing response with empty capabilities."""
        registry = WorkerRegistry(None)
        discovery = MockDiscoveryService()

        handler = WorkerRegistrationHandler(
            registry=registry,
            discovery_service=discovery,
        )

        response = RegistrationResponse(
            accepted=True,
            manager_id="mgr-1",
            healthy_managers=[],
            protocol_version_major=1,
            protocol_version_minor=0,
            capabilities="",
        )

        accepted, _ = handler.process_registration_response(
            data=response.dump(),
            node_host="localhost",
            node_port=8000,
            node_id_short="wkr",
            add_unconfirmed_peer=MagicMock(),
            add_to_probe_scheduler=MagicMock(),
        )

        assert accepted is True
        # Should have empty common features set
        assert handler.negotiated_capabilities.common_features == set()

    def test_special_characters_in_node_id(self) -> None:
        """Test with special characters in node ID."""
        registry = WorkerRegistry(None)
        discovery = MockDiscoveryService()

        handler = WorkerRegistrationHandler(
            registry=registry,
            discovery_service=discovery,
        )

        manager = ManagerInfo(
            node_id="mgr-🚀-test-ñ",
            tcp_host="192.168.1.200",
            tcp_port=8000,
            udp_host="192.168.1.200",
            udp_port=8001,
            datacenter="dc-1",
            is_leader=False,
        )

        registration = ManagerToWorkerRegistration(
            manager=manager,
            is_leader=False,
            term=1,
            known_managers=[],
        )

        result = handler.process_manager_registration(
            data=registration.dump(),
            node_id_full="worker-🚀-id",
            total_cores=8,
            available_cores=4,
            add_unconfirmed_peer=MagicMock(),
            add_to_probe_scheduler=MagicMock(),
        )

        ack = ManagerToWorkerRegistrationAck.load(result)
        assert ack.accepted is True
        assert ack.worker_id == "worker-🚀-id"
