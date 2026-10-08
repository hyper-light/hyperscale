"""
Integration tests for Datacenter Management (AD-27 Phase 5.2).

Tests:
- DatacenterHealthManager health classification
"""

import asyncio
import pytest

from hyperscale.distributed.env import Env
from hyperscale.distributed.health.phi_accrual_config import PhiAccrualConfig
from hyperscale.distributed.datacenters import (
    DatacenterHealthManager,
    ManagerInfo,
)
from hyperscale.distributed.models import (
    ManagerHeartbeat,
    DatacenterHealth,
    DatacenterStatus,
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
