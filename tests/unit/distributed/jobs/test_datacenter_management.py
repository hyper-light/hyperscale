"""
Integration tests for Datacenter Management (AD-27 Phase 5.2).

Tests:
- DatacenterHealthManager health classification
- LeaseManager lease lifecycle
"""

import asyncio
import time
import pytest

from hyperscale.distributed.env import Env
from hyperscale.distributed.health.phi_accrual_config import PhiAccrualConfig
from hyperscale.distributed.datacenters import (
    DatacenterHealthManager,
    ManagerInfo,
    LeaseManager,
    LeaseStats,
)
from hyperscale.distributed.models import (
    ManagerHeartbeat,
    DatacenterHealth,
    DatacenterStatus,
    DatacenterLease,
    LeaseTransfer,
)

MANAGER_HEARTBEAT_PHI = PhiAccrualConfig.for_manager_heartbeats(Env())


class TestDatacenterHealthManager:
    """Test DatacenterHealthManager operations."""

    def test_create_manager(self) -> None:
        """Test creating a DatacenterHealthManager."""
        manager = DatacenterHealthManager(MANAGER_HEARTBEAT_PHI)

        assert manager.count_active_datacenters() == 0

    def test_update_manager_heartbeat(self) -> None:
        """Test updating manager heartbeat."""
        health_mgr = DatacenterHealthManager(MANAGER_HEARTBEAT_PHI)

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
        health_mgr = DatacenterHealthManager(MANAGER_HEARTBEAT_PHI)

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
        health_mgr = DatacenterHealthManager(MANAGER_HEARTBEAT_PHI)
        health_mgr.add_datacenter("dc-1")

        status = health_mgr.get_datacenter_health("dc-1")
        assert status.health == DatacenterHealth.UNHEALTHY.value

    def test_datacenter_busy_when_managers_alive_but_no_workers(self) -> None:
        """Live managers with zero workers classify BUSY, not UNHEALTHY.

        Per the DatacenterHealth contract, BUSY means "transient, will
        clear -> accept job (queued)": the tier that accepts and queues
        work is up, capacity is momentarily zero (worker warmup, or
        total worker loss with a live manager that will re-register
        them). Classifying this UNHEALTHY made warmup indistinguishable
        from outage — gates insta-failed accepted jobs during cluster
        bring-up. Zero available capacity is still reported so routing
        prefers datacenters with real capacity.
        """
        health_mgr = DatacenterHealthManager(MANAGER_HEARTBEAT_PHI)

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
        assert status.health == DatacenterHealth.BUSY.value
        assert status.available_capacity == 0
        assert status.worker_count == 0

    def test_datacenter_initializing_before_any_heartbeat(self) -> None:
        """A configured datacenter no manager has ever reported from is
        INITIALIZING (pre-first-heartbeat warmup), distinct from
        UNHEALTHY (heartbeats existed and stopped, or a broken DC)."""
        health_mgr = DatacenterHealthManager(
            MANAGER_HEARTBEAT_PHI,
            get_configured_managers=lambda dc_id: [("10.0.0.1", 8080)],
        )

        status = health_mgr.get_datacenter_health("dc-1")
        assert status.health == DatacenterHealth.INITIALIZING.value

    def test_datacenter_busy(self) -> None:
        """Test busy classification when capacity utilization is 75%."""
        health_mgr = DatacenterHealthManager(MANAGER_HEARTBEAT_PHI)

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
            available_cores=25,
            total_cores=100,
        )

        health_mgr.update_manager("dc-1", ("10.0.0.1", 8080), heartbeat)

        status = health_mgr.get_datacenter_health("dc-1")
        assert status.health == DatacenterHealth.BUSY.value

    def test_datacenter_degraded_workers(self) -> None:
        """Test degraded classification when worker overload ratio exceeds 50%."""
        health_mgr = DatacenterHealthManager(MANAGER_HEARTBEAT_PHI)

        heartbeat = ManagerHeartbeat(
            node_id="manager-1",
            datacenter="dc-1",
            is_leader=True,
            term=1,
            version=1,
            active_jobs=5,
            active_workflows=10,
            worker_count=10,
            healthy_worker_count=4,
            overloaded_worker_count=6,
            available_cores=60,
            total_cores=100,
        )

        health_mgr.update_manager("dc-1", ("10.0.0.1", 8080), heartbeat)

        status = health_mgr.get_datacenter_health("dc-1")
        assert status.health == DatacenterHealth.DEGRADED.value

    def test_get_leader_address(self) -> None:
        """Test getting leader address."""
        health_mgr = DatacenterHealthManager(MANAGER_HEARTBEAT_PHI)

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
        health_mgr = DatacenterHealthManager(MANAGER_HEARTBEAT_PHI)

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
        health_mgr = DatacenterHealthManager(MANAGER_HEARTBEAT_PHI)

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
        assert (
            manager.validate_fence_token("job-123", "dc-1", lease.fence_token) is True
        )
        assert (
            manager.validate_fence_token("job-123", "dc-1", lease.fence_token + 1)
            is True
        )

        # Invalid (stale) token
        assert (
            manager.validate_fence_token("job-123", "dc-1", lease.fence_token - 1)
            is False
        )

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
