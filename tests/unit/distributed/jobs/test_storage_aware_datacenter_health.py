"""
A datacenter whose authoritative manager cannot write durably is
UNHEALTHY for placement.

Storage exhaustion was invisible to datacenter health: a manager with a
full disk kept classifying its datacenter HEALTHY, so gates kept
placing jobs there and every one failed at its first ledger write. The
manager now reports ``storage_writable`` in its heartbeat; the
classifier routes around a datacenter whose authoritative (leader)
heartbeat reports False, and the datacenter returns to HEALTHY when the
manager's storage probe succeeds.

Driven through the real DatacenterHealthManager.
"""

from hyperscale.distributed.env import Env
from hyperscale.distributed.health.phi_accrual_config import PhiAccrualConfig
from hyperscale.distributed.datacenters import DatacenterHealthManager
from hyperscale.distributed.models import DatacenterHealth, ManagerHeartbeat

MANAGER_HEARTBEAT_PHI = PhiAccrualConfig.for_manager_heartbeats(Env())

DATACENTER = "dc-1"
MANAGER_ADDR = ("10.0.0.1", 8080)


def leader_heartbeat(storage_writable: bool, version: int) -> ManagerHeartbeat:
    return ManagerHeartbeat(
        node_id="manager-1",
        datacenter=DATACENTER,
        is_leader=True,
        term=1,
        version=version,
        active_jobs=0,
        active_workflows=0,
        worker_count=4,
        healthy_worker_count=4,
        available_cores=32,
        total_cores=40,
        storage_writable=storage_writable,
    )


def test_a_datacenter_whose_leader_cannot_write_is_unhealthy_until_it_can() -> None:
    health_manager = DatacenterHealthManager(MANAGER_HEARTBEAT_PHI)

    health_manager.update_manager(DATACENTER, MANAGER_ADDR, leader_heartbeat(True, 1))
    assert health_manager.get_datacenter_health(DATACENTER).health == DatacenterHealth.HEALTHY.value

    health_manager.update_manager(DATACENTER, MANAGER_ADDR, leader_heartbeat(False, 2))
    status = health_manager.get_datacenter_health(DATACENTER)
    assert status.health == DatacenterHealth.UNHEALTHY.value
    assert status.available_capacity == 0

    health_manager.update_manager(DATACENTER, MANAGER_ADDR, leader_heartbeat(True, 3))
    assert health_manager.get_datacenter_health(DATACENTER).health == DatacenterHealth.HEALTHY.value


def test_a_heartbeat_from_a_manager_without_the_field_is_writable() -> None:
    assert ManagerHeartbeat(
        node_id="manager-1",
        datacenter=DATACENTER,
        is_leader=True,
        term=1,
        version=1,
        active_jobs=0,
        active_workflows=0,
        worker_count=4,
        healthy_worker_count=4,
        available_cores=32,
        total_cores=40,
    ).storage_writable
