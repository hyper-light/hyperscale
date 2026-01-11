"""
Integration tests for Datacenter Management (AD-27 Phase 5.2).

Tests:
- DatacenterHealthManager health classification
- ManagerDispatcher dispatch and fallback
- LeaseManager lease lifecycle
"""

import asyncio
import time
import pytest

from hyperscale.distributed_rewrite.datacenters import (
    DatacenterHealthManager,
    ManagerInfo,
    ManagerDispatcher,
    DispatchResult,
    DispatchStats,
    LeaseManager,
    LeaseStats,
)
from hyperscale.distributed_rewrite.models import (
    ManagerHeartbeat,
    DatacenterHealth,
    DatacenterStatus,
    DatacenterLease,
    LeaseTransfer,
)


class TestDatacenterHealthManager:
    """Test DatacenterHealthManager operations."""

    def test_create_manager(self) -> None:
        """Test creating a DatacenterHealthManager."""
        manager = DatacenterHealthManager()

        assert manager.count_active_datacenters() == 0

    def test_update_manager_heartbeat(self) -> None:
        """Test updating manager heartbeat."""
        health_mgr = DatacenterHealthManager()

        heartbeat = ManagerHeartbeat(
            node_id="manager-1",
            datacenter="dc-1",
            is_leader=True,
            term=1,
            version=1,
            active_jobs=5,
            active_workflows=10,
            worker_count=4,
            healthy_worker_count=4,
            available_cores=32,
            total_cores=40,
        )

        health_mgr.update_manager("dc-1", ("10.0.0.1", 8080), heartbeat)

        info = health_mgr.get_manager_info("dc-1", ("10.0.0.1", 8080))
        assert info is not None
        assert info.heartbeat.node_id == "manager-1"

    def test_datacenter_healthy(self) -> None:
        """Test healthy datacenter classification."""
        health_mgr = DatacenterHealthManager()

        heartbeat = ManagerHeartbeat(
            node_id="manager-1",
            datacenter="dc-1",
            is_leader=True,
            term=1,
            version=1,
            active_jobs=0,
            active_workflows=0,
            worker_count=4,
            healthy_worker_count=4,
            available_cores=32,
            total_cores=40,
        )

        health_mgr.update_manager("dc-1", ("10.0.0.1", 8080), heartbeat)

        status = health_mgr.get_datacenter_health("dc-1")
        assert status.health == DatacenterHealth.HEALTHY.value
        assert status.available_capacity == 32

    def test_datacenter_unhealthy_no_managers(self) -> None:
        """Test unhealthy classification when no managers."""
        health_mgr = DatacenterHealthManager()
        health_mgr.add_datacenter("dc-1")

        status = health_mgr.get_datacenter_health("dc-1")
        assert status.health == DatacenterHealth.UNHEALTHY.value

    def test_datacenter_unhealthy_no_workers(self) -> None:
        """Test unhealthy classification when no workers."""
        health_mgr = DatacenterHealthManager()

        heartbeat = ManagerHeartbeat(
            node_id="manager-1",
            datacenter="dc-1",
            is_leader=True,
            term=1,
            version=1,
            active_jobs=0,
            active_workflows=0,
            worker_count=0,  # No workers
            healthy_worker_count=0,
            available_cores=0,
            total_cores=0,
        )

        health_mgr.update_manager("dc-1", ("10.0.0.1", 8080), heartbeat)

        status = health_mgr.get_datacenter_health("dc-1")
        assert status.health == DatacenterHealth.UNHEALTHY.value

    def test_datacenter_busy(self) -> None:
        """Test busy classification when no available capacity."""
        health_mgr = DatacenterHealthManager()

        heartbeat = ManagerHeartbeat(
            node_id="manager-1",
            datacenter="dc-1",
            is_leader=True,
            term=1,
            version=1,
            active_jobs=10,
            active_workflows=100,
            worker_count=4,
            healthy_worker_count=4,
            available_cores=0,  # No capacity
            total_cores=40,
        )

        health_mgr.update_manager("dc-1", ("10.0.0.1", 8080), heartbeat)

        status = health_mgr.get_datacenter_health("dc-1")
        assert status.health == DatacenterHealth.BUSY.value

    def test_datacenter_degraded_workers(self) -> None:
        """Test degraded classification when majority workers unhealthy."""
        health_mgr = DatacenterHealthManager()

        heartbeat = ManagerHeartbeat(
            node_id="manager-1",
            datacenter="dc-1",
            is_leader=True,
            term=1,
            version=1,
            active_jobs=5,
            active_workflows=10,
            worker_count=10,
            healthy_worker_count=3,  # Minority healthy
            available_cores=20,
            total_cores=100,
        )

        health_mgr.update_manager("dc-1", ("10.0.0.1", 8080), heartbeat)

        status = health_mgr.get_datacenter_health("dc-1")
        assert status.health == DatacenterHealth.DEGRADED.value

    def test_get_leader_address(self) -> None:
        """Test getting leader address."""
        health_mgr = DatacenterHealthManager()

        # Non-leader
        heartbeat1 = ManagerHeartbeat(
            node_id="manager-1",
            datacenter="dc-1",
            is_leader=False,
            term=1,
            version=1,
            active_jobs=0,
            active_workflows=0,
            worker_count=4,
            healthy_worker_count=4,
            available_cores=32,
            total_cores=40,
        )

        # Leader
        heartbeat2 = ManagerHeartbeat(
            node_id="manager-2",
            datacenter="dc-1",
            is_leader=True,
            term=1,
            version=1,
            active_jobs=0,
            active_workflows=0,
            worker_count=4,
            healthy_worker_count=4,
            available_cores=32,
            total_cores=40,
        )

        health_mgr.update_manager("dc-1", ("10.0.0.1", 8080), heartbeat1)
        health_mgr.update_manager("dc-1", ("10.0.0.2", 8080), heartbeat2)

        leader = health_mgr.get_leader_address("dc-1")
        assert leader == ("10.0.0.2", 8080)

    def test_get_alive_managers(self) -> None:
        """Test getting alive managers."""
        health_mgr = DatacenterHealthManager()

        heartbeat = ManagerHeartbeat(
            node_id="manager-1",
            datacenter="dc-1",
            is_leader=True,
            term=1,
            version=1,
            active_jobs=0,
            active_workflows=0,
            worker_count=4,
            healthy_worker_count=4,
            available_cores=32,
            total_cores=40,
        )

        health_mgr.update_manager("dc-1", ("10.0.0.1", 8080), heartbeat)
        health_mgr.update_manager("dc-1", ("10.0.0.2", 8080), heartbeat)

        alive = health_mgr.get_alive_managers("dc-1")
        assert len(alive) == 2

    def test_mark_manager_dead(self) -> None:
        """Test marking a manager as dead."""
        health_mgr = DatacenterHealthManager()

        heartbeat = ManagerHeartbeat(
            node_id="manager-1",
            datacenter="dc-1",
            is_leader=True,
            term=1,
            version=1,
            active_jobs=0,
            active_workflows=0,
            worker_count=4,
            healthy_worker_count=4,
            available_cores=32,
            total_cores=40,
        )

        health_mgr.update_manager("dc-1", ("10.0.0.1", 8080), heartbeat)
        health_mgr.mark_manager_dead("dc-1", ("10.0.0.1", 8080))

        alive = health_mgr.get_alive_managers("dc-1")
        assert len(alive) == 0


class TestManagerDispatcher:
    """Test ManagerDispatcher operations."""

    def test_create_dispatcher(self) -> None:
        """Test creating a ManagerDispatcher."""
        dispatcher = ManagerDispatcher()

        assert dispatcher.get_all_datacenters() == []

    def test_add_datacenter(self) -> None:
        """Test adding a datacenter."""
        dispatcher = ManagerDispatcher()

        dispatcher.add_datacenter("dc-1", [("10.0.0.1", 8080), ("10.0.0.2", 8080)])

        assert dispatcher.has_datacenter("dc-1")
        assert len(dispatcher.get_managers("dc-1")) == 2

    def test_set_leader(self) -> None:
        """Test setting DC leader."""
        dispatcher = ManagerDispatcher()

        dispatcher.add_datacenter("dc-1", [("10.0.0.1", 8080), ("10.0.0.2", 8080)])
        dispatcher.set_leader("dc-1", ("10.0.0.2", 8080))

        assert dispatcher.get_leader("dc-1") == ("10.0.0.2", 8080)

    @pytest.mark.asyncio
    async def test_dispatch_success(self) -> None:
        """Test successful dispatch."""
        dispatcher = ManagerDispatcher()
        dispatcher.add_datacenter("dc-1", [("10.0.0.1", 8080)])

        async def mock_send_tcp(
            addr: tuple[str, int],
            endpoint: str,
            data: bytes,
            timeout: float = 5.0,
        ) -> tuple[bytes, float]:
            return (b"success", 0.01)

        result = await dispatcher.dispatch_to_datacenter(
            dc_id="dc-1",
            endpoint="job_submission",
            data=b"test_data",
            send_tcp=mock_send_tcp,
        )

        assert result.success is True
        assert result.datacenter == "dc-1"
        assert result.response == b"success"

    @pytest.mark.asyncio
    async def test_dispatch_no_managers(self) -> None:
        """Test dispatch with no managers configured."""
        dispatcher = ManagerDispatcher()

        async def mock_send_tcp(
            addr: tuple[str, int],
            endpoint: str,
            data: bytes,
            timeout: float = 5.0,
        ) -> tuple[bytes, float]:
            return (b"success", 0.01)

        result = await dispatcher.dispatch_to_datacenter(
            dc_id="dc-unknown",
            endpoint="job_submission",
            data=b"test_data",
            send_tcp=mock_send_tcp,
        )

        assert result.success is False
        assert "No managers" in (result.error or "")

    @pytest.mark.asyncio
    async def test_dispatch_with_retry(self) -> None:
        """Test dispatch retries on failure."""
        dispatcher = ManagerDispatcher()
        dispatcher.add_datacenter("dc-1", [("10.0.0.1", 8080), ("10.0.0.2", 8080)])

        call_count = 0

        async def mock_send_tcp(
            addr: tuple[str, int],
            endpoint: str,
            data: bytes,
            timeout: float = 5.0,
        ) -> tuple[bytes, float]:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ConnectionError("First manager failed")
            return (b"success", 0.01)

        result = await dispatcher.dispatch_to_datacenter(
            dc_id="dc-1",
            endpoint="job_submission",
            data=b"test_data",
            send_tcp=mock_send_tcp,
        )

        assert result.success is True
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_dispatch_with_fallback(self) -> None:
        """Test dispatch with fallback to another DC."""
        dispatcher = ManagerDispatcher()
        dispatcher.add_datacenter("dc-1", [("10.0.0.1", 8080)])
        dispatcher.add_datacenter("dc-2", [("10.0.0.2", 8080)])

        async def mock_send_tcp(
            addr: tuple[str, int],
            endpoint: str,
            data: bytes,
            timeout: float = 5.0,
        ) -> tuple[bytes, float]:
            if addr[0] == "10.0.0.1":
                raise ConnectionError("DC-1 failed")
            return (b"success", 0.01)

        successful, failed = await dispatcher.dispatch_with_fallback(
            endpoint="job_submission",
            data=b"test_data",
            send_tcp=mock_send_tcp,
            primary_dcs=["dc-1"],
            fallback_dcs=["dc-2"],
        )

        assert "dc-2" in successful
        assert len(failed) == 0

    @pytest.mark.asyncio
    async def test_broadcast_to_all(self) -> None:
        """Test broadcasting to all DCs."""
        dispatcher = ManagerDispatcher()
        dispatcher.add_datacenter("dc-1", [("10.0.0.1", 8080)])
        dispatcher.add_datacenter("dc-2", [("10.0.0.2", 8080)])

        async def mock_send_tcp(
            addr: tuple[str, int],
            endpoint: str,
            data: bytes,
            timeout: float = 5.0,
        ) -> tuple[bytes, float]:
            return (b"ok", 0.01)

        results = await dispatcher.broadcast_to_all(
            endpoint="notification",
            data=b"broadcast_data",
            send_tcp=mock_send_tcp,
        )

        assert len(results) == 2
        assert results["dc-1"].success is True
        assert results["dc-2"].success is True


class TestLeaseManager:
    """Test LeaseManager operations."""

    def test_create_manager(self) -> None:
        """Test creating a LeaseManager."""
        manager = LeaseManager(node_id="gate-1")

        stats = manager.get_stats()
        assert stats["active_leases"] == 0

    def test_acquire_lease(self) -> None:
        """Test acquiring a lease."""
        manager = LeaseManager(node_id="gate-1", lease_timeout=30.0)

        lease = manager.acquire_lease("job-123", "dc-1")

        assert lease.job_id == "job-123"
        assert lease.datacenter == "dc-1"
        assert lease.lease_holder == "gate-1"
        assert lease.fence_token == 1

    def test_get_lease(self) -> None:
        """Test getting an existing lease."""
        manager = LeaseManager(node_id="gate-1", lease_timeout=30.0)

        manager.acquire_lease("job-123", "dc-1")

        lease = manager.get_lease("job-123", "dc-1")
        assert lease is not None
        assert lease.job_id == "job-123"

    def test_get_nonexistent_lease(self) -> None:
        """Test getting a non-existent lease."""
        manager = LeaseManager(node_id="gate-1")

        lease = manager.get_lease("job-123", "dc-1")
        assert lease is None

    def test_is_lease_holder(self) -> None:
        """Test checking lease holder status."""
        manager = LeaseManager(node_id="gate-1", lease_timeout=30.0)

        manager.acquire_lease("job-123", "dc-1")

        assert manager.is_lease_holder("job-123", "dc-1") is True
        assert manager.is_lease_holder("job-123", "dc-2") is False

    def test_release_lease(self) -> None:
        """Test releasing a lease."""
        manager = LeaseManager(node_id="gate-1", lease_timeout=30.0)

        manager.acquire_lease("job-123", "dc-1")
        released = manager.release_lease("job-123", "dc-1")

        assert released is not None
        assert manager.get_lease("job-123", "dc-1") is None

    def test_release_job_leases(self) -> None:
        """Test releasing all leases for a job."""
        manager = LeaseManager(node_id="gate-1", lease_timeout=30.0)

        manager.acquire_lease("job-123", "dc-1")
        manager.acquire_lease("job-123", "dc-2")
        manager.acquire_lease("job-456", "dc-1")

        released = manager.release_job_leases("job-123")

        assert len(released) == 2
        assert manager.get_lease("job-123", "dc-1") is None
        assert manager.get_lease("job-123", "dc-2") is None
        assert manager.get_lease("job-456", "dc-1") is not None

    def test_renew_lease(self) -> None:
        """Test renewing an existing lease."""
        manager = LeaseManager(node_id="gate-1", lease_timeout=30.0)

        lease1 = manager.acquire_lease("job-123", "dc-1")
        original_expires = lease1.expires_at

        # Simulate some time passing
        time.sleep(0.01)

        lease2 = manager.acquire_lease("job-123", "dc-1")

        # Should be same lease with extended expiration
        assert lease2.fence_token == lease1.fence_token
        assert lease2.expires_at > original_expires

    def test_create_transfer(self) -> None:
        """Test creating a lease transfer."""
        manager = LeaseManager(node_id="gate-1", lease_timeout=30.0)

        manager.acquire_lease("job-123", "dc-1")

        transfer = manager.create_transfer("job-123", "dc-1", "gate-2")

        assert transfer is not None
        assert transfer.job_id == "job-123"
        assert transfer.from_gate == "gate-1"
        assert transfer.to_gate == "gate-2"

    def test_accept_transfer(self) -> None:
        """Test accepting a lease transfer."""
        gate1_manager = LeaseManager(node_id="gate-1", lease_timeout=30.0)
        gate2_manager = LeaseManager(node_id="gate-2", lease_timeout=30.0)

        # Gate 1 acquires and transfers
        gate1_manager.acquire_lease("job-123", "dc-1")
        transfer = gate1_manager.create_transfer("job-123", "dc-1", "gate-2")

        # Gate 2 accepts
        assert transfer is not None
        new_lease = gate2_manager.accept_transfer(transfer)

        assert new_lease.lease_holder == "gate-2"
        assert gate2_manager.is_lease_holder("job-123", "dc-1") is True

    def test_validate_fence_token(self) -> None:
        """Test fence token validation."""
        manager = LeaseManager(node_id="gate-1", lease_timeout=30.0)

        lease = manager.acquire_lease("job-123", "dc-1")

        # Valid token
        assert manager.validate_fence_token("job-123", "dc-1", lease.fence_token) is True
        assert manager.validate_fence_token("job-123", "dc-1", lease.fence_token + 1) is True

        # Invalid (stale) token
        assert manager.validate_fence_token("job-123", "dc-1", lease.fence_token - 1) is False

    def test_cleanup_expired(self) -> None:
        """Test cleaning up expired leases."""
        manager = LeaseManager(node_id="gate-1", lease_timeout=0.01)  # Short timeout

        manager.acquire_lease("job-123", "dc-1")

        # Wait for expiration
        time.sleep(0.02)

        expired = manager.cleanup_expired()

        assert expired == 1
        assert manager.get_lease("job-123", "dc-1") is None


class TestIntegrationScenarios:
    """Test realistic integration scenarios."""

    @pytest.mark.asyncio
    async def test_job_dispatch_with_health_check(self) -> None:
        """
        Test job dispatch with health checking.

        Scenario:
        1. Gate checks DC health
        2. Gate acquires lease
        3. Gate dispatches to healthy DC
        4. DC becomes unhealthy
        5. Gate fails over to another DC
        """
        # Setup
        health_mgr = DatacenterHealthManager()
        dispatcher = ManagerDispatcher()
        lease_mgr = LeaseManager(node_id="gate-1", lease_timeout=30.0)

        # Configure DCs
        dispatcher.add_datacenter("dc-1", [("10.0.0.1", 8080)])
        dispatcher.add_datacenter("dc-2", [("10.0.0.2", 8080)])

        # DC-1 is healthy
        heartbeat1 = ManagerHeartbeat(
            node_id="manager-1",
            datacenter="dc-1",
            is_leader=True,
            term=1,
            version=1,
            active_jobs=0,
            active_workflows=0,
            worker_count=4,
            healthy_worker_count=4,
            available_cores=32,
            total_cores=40,
        )
        health_mgr.update_manager("dc-1", ("10.0.0.1", 8080), heartbeat1)

        # DC-2 is healthy
        heartbeat2 = ManagerHeartbeat(
            node_id="manager-2",
            datacenter="dc-2",
            is_leader=True,
            term=1,
            version=1,
            active_jobs=0,
            active_workflows=0,
            worker_count=4,
            healthy_worker_count=4,
            available_cores=32,
            total_cores=40,
        )
        health_mgr.update_manager("dc-2", ("10.0.0.2", 8080), heartbeat2)

        # Step 1: Check health
        assert health_mgr.is_datacenter_healthy("dc-1") is True

        # Step 2: Acquire lease
        lease = lease_mgr.acquire_lease("job-123", "dc-1")
        assert lease_mgr.is_lease_holder("job-123", "dc-1") is True

        # Step 3: Dispatch succeeds
        dispatch_success = False

        async def mock_send_tcp(
            addr: tuple[str, int],
            endpoint: str,
            data: bytes,
            timeout: float = 5.0,
        ) -> tuple[bytes, float]:
            nonlocal dispatch_success
            if addr[0] == "10.0.0.1":
                raise ConnectionError("DC-1 is down")
            dispatch_success = True
            return (b"ok", 0.01)

        # Step 4 & 5: DC-1 fails, fall back to DC-2
        successful, failed = await dispatcher.dispatch_with_fallback(
            endpoint="job_submission",
            data=b"test",
            send_tcp=mock_send_tcp,
            primary_dcs=["dc-1"],
            fallback_dcs=["dc-2"],
            get_dc_health=lambda dc: health_mgr.get_datacenter_health(dc).health,
        )

        assert "dc-2" in successful
        assert dispatch_success is True

    def test_lease_lifecycle(self) -> None:
        """
        Test complete lease lifecycle.

        Scenario:
        1. Gate-1 acquires lease for job
        2. Gate-1 dispatches successfully
        3. Gate-1 fails, Gate-2 takes over
        4. Gate-2 accepts lease transfer
        5. Job completes, lease released
        """
        gate1_mgr = LeaseManager(node_id="gate-1", lease_timeout=30.0)
        gate2_mgr = LeaseManager(node_id="gate-2", lease_timeout=30.0)

        # Step 1: Gate-1 acquires lease
        lease = gate1_mgr.acquire_lease("job-123", "dc-1")
        assert lease.lease_holder == "gate-1"

        # Step 2: Gate-1 dispatches (simulated success)
        assert gate1_mgr.is_lease_holder("job-123", "dc-1") is True

        # Step 3: Gate-1 fails, creates transfer
        transfer = gate1_mgr.create_transfer("job-123", "dc-1", "gate-2")
        assert transfer is not None

        # Step 4: Gate-2 accepts transfer
        new_lease = gate2_mgr.accept_transfer(transfer)
        assert new_lease.lease_holder == "gate-2"
        assert gate2_mgr.is_lease_holder("job-123", "dc-1") is True

        # Step 5: Job completes, release lease
        released = gate2_mgr.release_lease("job-123", "dc-1")
        assert released is not None

        stats = gate2_mgr.get_stats()
        assert stats["active_leases"] == 0
