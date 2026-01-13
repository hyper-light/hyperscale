"""
Integration tests for WorkerRegistry (Section 15.2.6.2).

Tests WorkerRegistry for manager registration, health tracking, and circuit breakers.

Covers:
- Happy path: Normal manager registration and health tracking
- Negative path: Invalid manager operations
- Failure mode: Circuit breaker transitions
- Concurrency: Thread-safe lock management
- Edge cases: Empty registry, many managers
"""

import asyncio
import time
from unittest.mock import MagicMock

import pytest

from hyperscale.distributed.nodes.worker.registry import WorkerRegistry
from hyperscale.distributed.models import ManagerInfo
from hyperscale.distributed.swim.core import CircuitState


class TestWorkerRegistryInitialization:
    """Test WorkerRegistry initialization."""

    def test_happy_path_instantiation(self):
        """Test normal registry initialization."""
        logger = MagicMock()
        registry = WorkerRegistry(logger)

        assert registry._logger == logger
        assert isinstance(registry._known_managers, dict)
        assert isinstance(registry._healthy_manager_ids, set)
        assert registry._primary_manager_id is None

    def test_custom_recovery_settings(self):
        """Test with custom recovery settings."""
        logger = MagicMock()
        registry = WorkerRegistry(
            logger,
            recovery_jitter_min=0.5,
            recovery_jitter_max=2.0,
            recovery_semaphore_size=10,
        )

        assert registry._recovery_jitter_min == 0.5
        assert registry._recovery_jitter_max == 2.0
        assert isinstance(registry._recovery_semaphore, asyncio.Semaphore)


class TestWorkerRegistryManagerOperations:
    """Test manager add/get operations."""

    def test_add_manager(self):
        """Test adding a manager."""
        logger = MagicMock()
        registry = WorkerRegistry(logger)

        manager_info = MagicMock(spec=ManagerInfo)
        manager_info.tcp_host = "192.168.1.1"
        manager_info.tcp_port = 8000
        manager_info.udp_host = "192.168.1.1"
        manager_info.udp_port = 8001

        registry.add_manager("mgr-1", manager_info)

        assert "mgr-1" in registry._known_managers
        assert registry._known_managers["mgr-1"] == manager_info

    def test_get_manager(self):
        """Test getting a manager by ID."""
        logger = MagicMock()
        registry = WorkerRegistry(logger)

        manager_info = MagicMock(spec=ManagerInfo)
        registry.add_manager("mgr-1", manager_info)

        result = registry.get_manager("mgr-1")
        assert result == manager_info

    def test_get_manager_not_found(self):
        """Test getting a non-existent manager."""
        logger = MagicMock()
        registry = WorkerRegistry(logger)

        result = registry.get_manager("non-existent")
        assert result is None

    def test_get_manager_by_addr(self):
        """Test getting a manager by TCP address."""
        logger = MagicMock()
        registry = WorkerRegistry(logger)

        manager_info = MagicMock(spec=ManagerInfo)
        manager_info.tcp_host = "192.168.1.1"
        manager_info.tcp_port = 8000
        registry.add_manager("mgr-1", manager_info)

        result = registry.get_manager_by_addr(("192.168.1.1", 8000))
        assert result == manager_info

    def test_get_manager_by_addr_not_found(self):
        """Test getting manager by non-existent address."""
        logger = MagicMock()
        registry = WorkerRegistry(logger)

        result = registry.get_manager_by_addr(("192.168.1.1", 8000))
        assert result is None


class TestWorkerRegistryHealthTracking:
    @pytest.mark.asyncio
    async def test_mark_manager_healthy(self):
        logger = MagicMock()
        registry = WorkerRegistry(logger)

        await registry.mark_manager_healthy("mgr-1")

        assert "mgr-1" in registry._healthy_manager_ids
        assert registry.is_manager_healthy("mgr-1") is True

    @pytest.mark.asyncio
    async def test_mark_manager_unhealthy(self):
        logger = MagicMock()
        registry = WorkerRegistry(logger)

        await registry.mark_manager_healthy("mgr-1")
        await registry.mark_manager_unhealthy("mgr-1")

        assert "mgr-1" not in registry._healthy_manager_ids
        assert registry.is_manager_healthy("mgr-1") is False

    @pytest.mark.asyncio
    async def test_mark_manager_unhealthy_records_timestamp(self):
        logger = MagicMock()
        registry = WorkerRegistry(logger)

        before = time.monotonic()
        await registry.mark_manager_unhealthy("mgr-1")
        after = time.monotonic()

        assert "mgr-1" in registry._manager_unhealthy_since
        assert before <= registry._manager_unhealthy_since["mgr-1"] <= after

    @pytest.mark.asyncio
    async def test_mark_manager_healthy_clears_unhealthy(self):
        logger = MagicMock()
        registry = WorkerRegistry(logger)

        await registry.mark_manager_unhealthy("mgr-1")
        await registry.mark_manager_healthy("mgr-1")

        assert "mgr-1" not in registry._manager_unhealthy_since

    @pytest.mark.asyncio
    async def test_get_healthy_manager_tcp_addrs(self):
        logger = MagicMock()
        registry = WorkerRegistry(logger)

        mgr1 = MagicMock(spec=ManagerInfo)
        mgr1.tcp_host = "192.168.1.1"
        mgr1.tcp_port = 8000

        mgr2 = MagicMock(spec=ManagerInfo)
        mgr2.tcp_host = "192.168.1.2"
        mgr2.tcp_port = 8001

        registry.add_manager("mgr-1", mgr1)
        registry.add_manager("mgr-2", mgr2)
        await registry.mark_manager_healthy("mgr-1")
        await registry.mark_manager_healthy("mgr-2")

        addrs = registry.get_healthy_manager_tcp_addrs()

        assert len(addrs) == 2
        assert ("192.168.1.1", 8000) in addrs
        assert ("192.168.1.2", 8001) in addrs

    def test_get_healthy_manager_tcp_addrs_empty(self):
        logger = MagicMock()
        registry = WorkerRegistry(logger)

        addrs = registry.get_healthy_manager_tcp_addrs()
        assert addrs == []


class TestWorkerRegistryPrimaryManager:
    """Test primary manager selection."""

    def test_set_primary_manager(self):
        """Test setting primary manager."""
        logger = MagicMock()
        registry = WorkerRegistry(logger)

        registry.set_primary_manager("mgr-1")

        assert registry._primary_manager_id == "mgr-1"

    def test_get_primary_manager_tcp_addr(self):
        """Test getting primary manager TCP address."""
        logger = MagicMock()
        registry = WorkerRegistry(logger)

        mgr = MagicMock(spec=ManagerInfo)
        mgr.tcp_host = "192.168.1.1"
        mgr.tcp_port = 8000

        registry.add_manager("mgr-1", mgr)
        registry.set_primary_manager("mgr-1")

        addr = registry.get_primary_manager_tcp_addr()
        assert addr == ("192.168.1.1", 8000)

    def test_get_primary_manager_tcp_addr_no_primary(self):
        """Test getting primary when none set."""
        logger = MagicMock()
        registry = WorkerRegistry(logger)

        addr = registry.get_primary_manager_tcp_addr()
        assert addr is None

    def test_get_primary_manager_tcp_addr_not_found(self):
        """Test getting primary when manager not in registry."""
        logger = MagicMock()
        registry = WorkerRegistry(logger)

        registry.set_primary_manager("non-existent")

        addr = registry.get_primary_manager_tcp_addr()
        assert addr is None

    @pytest.mark.asyncio
    async def test_select_new_primary_manager_leader(self):
        """Test selecting new primary manager (leader preferred)."""
        logger = MagicMock()
        registry = WorkerRegistry(logger)

        mgr1 = MagicMock(spec=ManagerInfo)
        mgr1.is_leader = False

        mgr2 = MagicMock(spec=ManagerInfo)
        mgr2.is_leader = True

        registry.add_manager("mgr-1", mgr1)
        registry.add_manager("mgr-2", mgr2)
        registry.mark_manager_healthy("mgr-1")
        registry.mark_manager_healthy("mgr-2")

        selected = await registry.select_new_primary_manager()

        assert selected == "mgr-2"  # Leader preferred

    @pytest.mark.asyncio
    async def test_select_new_primary_manager_no_leader(self):
        """Test selecting new primary when no leader."""
        logger = MagicMock()
        registry = WorkerRegistry(logger)

        mgr1 = MagicMock(spec=ManagerInfo)
        mgr1.is_leader = False

        registry.add_manager("mgr-1", mgr1)
        registry.mark_manager_healthy("mgr-1")

        selected = await registry.select_new_primary_manager()

        assert selected == "mgr-1"

    @pytest.mark.asyncio
    async def test_select_new_primary_manager_none_healthy(self):
        """Test selecting new primary when none healthy."""
        logger = MagicMock()
        registry = WorkerRegistry(logger)

        selected = await registry.select_new_primary_manager()

        assert selected is None


class TestWorkerRegistryLockManagement:
    """Test manager state lock management."""

    def test_get_or_create_manager_lock(self):
        """Test getting or creating a manager lock."""
        logger = MagicMock()
        registry = WorkerRegistry(logger)

        lock1 = registry.get_or_create_manager_lock("mgr-1")
        lock2 = registry.get_or_create_manager_lock("mgr-1")

        assert lock1 is lock2
        assert isinstance(lock1, asyncio.Lock)

    def test_different_managers_get_different_locks(self):
        """Test that different managers get different locks."""
        logger = MagicMock()
        registry = WorkerRegistry(logger)

        lock1 = registry.get_or_create_manager_lock("mgr-1")
        lock2 = registry.get_or_create_manager_lock("mgr-2")

        assert lock1 is not lock2


class TestWorkerRegistryEpochManagement:
    """Test manager epoch management."""

    def test_increment_manager_epoch(self):
        """Test incrementing manager epoch."""
        logger = MagicMock()
        registry = WorkerRegistry(logger)

        assert registry.get_manager_epoch("mgr-1") == 0

        epoch1 = registry.increment_manager_epoch("mgr-1")
        assert epoch1 == 1
        assert registry.get_manager_epoch("mgr-1") == 1

        epoch2 = registry.increment_manager_epoch("mgr-1")
        assert epoch2 == 2

    def test_get_manager_epoch_default(self):
        """Test getting epoch for unknown manager."""
        logger = MagicMock()
        registry = WorkerRegistry(logger)

        epoch = registry.get_manager_epoch("unknown")
        assert epoch == 0


class TestWorkerRegistryCircuitBreakers:
    """Test circuit breaker management."""

    def test_get_or_create_circuit(self):
        """Test getting or creating a circuit breaker."""
        logger = MagicMock()
        registry = WorkerRegistry(logger)

        circuit1 = registry.get_or_create_circuit("mgr-1")
        circuit2 = registry.get_or_create_circuit("mgr-1")

        assert circuit1 is circuit2

    def test_get_or_create_circuit_with_custom_thresholds(self):
        """Test creating circuit with custom thresholds."""
        logger = MagicMock()
        registry = WorkerRegistry(logger)

        circuit = registry.get_or_create_circuit(
            "mgr-1",
            error_threshold=10,
            error_rate_threshold=0.8,
            half_open_after=60.0,
        )

        assert circuit.error_threshold == 10
        assert circuit.error_rate_threshold == 0.8
        assert circuit.half_open_after == 60.0

    def test_get_or_create_circuit_by_addr(self):
        """Test getting or creating circuit by address."""
        logger = MagicMock()
        registry = WorkerRegistry(logger)

        addr = ("192.168.1.1", 8000)
        circuit1 = registry.get_or_create_circuit_by_addr(addr)
        circuit2 = registry.get_or_create_circuit_by_addr(addr)

        assert circuit1 is circuit2

    def test_is_circuit_open_closed(self):
        """Test checking closed circuit."""
        logger = MagicMock()
        registry = WorkerRegistry(logger)

        registry.get_or_create_circuit("mgr-1")

        assert registry.is_circuit_open("mgr-1") is False

    def test_is_circuit_open_no_circuit(self):
        """Test checking circuit for unknown manager."""
        logger = MagicMock()
        registry = WorkerRegistry(logger)

        assert registry.is_circuit_open("unknown") is False

    def test_is_circuit_open_by_addr_closed(self):
        """Test checking closed circuit by address."""
        logger = MagicMock()
        registry = WorkerRegistry(logger)

        addr = ("192.168.1.1", 8000)
        registry.get_or_create_circuit_by_addr(addr)

        assert registry.is_circuit_open_by_addr(addr) is False

    def test_get_circuit_status_specific(self):
        """Test getting circuit status for specific manager."""
        logger = MagicMock()
        registry = WorkerRegistry(logger)

        registry.get_or_create_circuit("mgr-1")

        status = registry.get_circuit_status("mgr-1")

        assert status["manager_id"] == "mgr-1"
        assert status["circuit_state"] == CircuitState.CLOSED.name
        assert "error_count" in status
        assert "error_rate" in status

    def test_get_circuit_status_not_found(self):
        """Test getting circuit status for unknown manager."""
        logger = MagicMock()
        registry = WorkerRegistry(logger)

        status = registry.get_circuit_status("unknown")

        assert "error" in status

    def test_get_circuit_status_summary(self):
        """Test getting circuit status summary."""
        logger = MagicMock()
        registry = WorkerRegistry(logger)

        registry.get_or_create_circuit("mgr-1")
        registry.get_or_create_circuit("mgr-2")
        registry.mark_manager_healthy("mgr-1")

        status = registry.get_circuit_status()

        assert "managers" in status
        assert "mgr-1" in status["managers"]
        assert "mgr-2" in status["managers"]
        assert "open_circuits" in status
        assert status["healthy_managers"] == 1


class TestWorkerRegistryUDPLookup:
    """Test UDP address lookup."""

    def test_find_manager_by_udp_addr(self):
        """Test finding manager by UDP address."""
        logger = MagicMock()
        registry = WorkerRegistry(logger)

        mgr = MagicMock(spec=ManagerInfo)
        mgr.udp_host = "192.168.1.1"
        mgr.udp_port = 8001

        registry.add_manager("mgr-1", mgr)

        found = registry.find_manager_by_udp_addr(("192.168.1.1", 8001))
        assert found == "mgr-1"

    def test_find_manager_by_udp_addr_not_found(self):
        """Test finding manager by unknown UDP address."""
        logger = MagicMock()
        registry = WorkerRegistry(logger)

        found = registry.find_manager_by_udp_addr(("192.168.1.1", 8001))
        assert found is None


class TestWorkerRegistryConcurrency:
    """Test concurrency aspects of WorkerRegistry."""

    @pytest.mark.asyncio
    async def test_concurrent_lock_access(self):
        """Test concurrent access to manager locks."""
        logger = MagicMock()
        registry = WorkerRegistry(logger)

        access_order = []

        async def access_with_lock(worker_id: int):
            lock = registry.get_or_create_manager_lock("mgr-1")
            async with lock:
                access_order.append(f"start-{worker_id}")
                await asyncio.sleep(0.01)
                access_order.append(f"end-{worker_id}")

        await asyncio.gather(
            access_with_lock(1),
            access_with_lock(2),
        )

        # Verify serialized access
        assert access_order[0] == "start-1"
        assert access_order[1] == "end-1"
        assert access_order[2] == "start-2"
        assert access_order[3] == "end-2"

    @pytest.mark.asyncio
    async def test_concurrent_manager_registration(self):
        """Test concurrent manager registration."""
        logger = MagicMock()
        registry = WorkerRegistry(logger)

        async def register_manager(manager_id: str):
            mgr = MagicMock(spec=ManagerInfo)
            mgr.tcp_host = f"192.168.1.{manager_id[-1]}"
            mgr.tcp_port = 8000
            registry.add_manager(manager_id, mgr)
            registry.mark_manager_healthy(manager_id)
            await asyncio.sleep(0.001)

        await asyncio.gather(*[register_manager(f"mgr-{i}") for i in range(10)])

        assert len(registry._known_managers) == 10
        assert len(registry._healthy_manager_ids) == 10


class TestWorkerRegistryEdgeCases:
    """Test edge cases for WorkerRegistry."""

    def test_many_managers(self):
        """Test with many managers."""
        logger = MagicMock()
        registry = WorkerRegistry(logger)

        for i in range(100):
            mgr = MagicMock(spec=ManagerInfo)
            mgr.tcp_host = f"192.168.1.{i % 256}"
            mgr.tcp_port = 8000 + i
            mgr.udp_host = mgr.tcp_host
            mgr.udp_port = mgr.tcp_port + 1
            mgr.is_leader = i == 0
            registry.add_manager(f"mgr-{i}", mgr)
            registry.mark_manager_healthy(f"mgr-{i}")

        assert len(registry._known_managers) == 100
        assert len(registry._healthy_manager_ids) == 100

    def test_special_characters_in_manager_id(self):
        """Test manager IDs with special characters."""
        logger = MagicMock()
        registry = WorkerRegistry(logger)

        special_id = "mgr-🚀-test-ñ-中文"
        mgr = MagicMock(spec=ManagerInfo)
        mgr.tcp_host = "localhost"
        mgr.tcp_port = 8000

        registry.add_manager(special_id, mgr)

        assert special_id in registry._known_managers
        assert registry.get_manager(special_id) == mgr

    def test_replace_manager(self):
        """Test replacing manager info."""
        logger = MagicMock()
        registry = WorkerRegistry(logger)

        mgr1 = MagicMock(spec=ManagerInfo)
        mgr1.tcp_host = "192.168.1.1"
        mgr1.tcp_port = 8000

        mgr2 = MagicMock(spec=ManagerInfo)
        mgr2.tcp_host = "192.168.1.2"
        mgr2.tcp_port = 9000

        registry.add_manager("mgr-1", mgr1)
        registry.add_manager("mgr-1", mgr2)  # Replace

        result = registry.get_manager("mgr-1")
        assert result == mgr2
