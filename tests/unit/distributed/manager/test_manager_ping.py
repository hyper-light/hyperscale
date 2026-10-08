"""
A manager answers a ping (client ``ping_manager``, peer recovery checks).

The handler built its response without the required request id,
datacenter, address and term (and with a field the model does not have),
so it raised; its error path then built another invalid response, and
every ping was answered with the server's generic error reply. A client
could never read a manager's status.

* a ping is answered with the manager's identity, its workers' health
  and capacity, and its active (non-terminal) jobs.
* AD-41: it carries the datacenter's resource view the manager's gossip
  holds -- its own report and its peers' -- so a client running jobs on
  the datacenter without a gate sees what a gate would.
"""

from types import SimpleNamespace

import pytest

from hyperscale.distributed.models import ManagerPingResponse, PingRequest
from hyperscale.distributed.nodes.manager.server import ManagerServer
from hyperscale.distributed.resources import (
    ManagerResourceGossip,
    ManagerResourceGossipEntry,
    ManagerResourceGossipMessage,
)
from hyperscale.distributed.resources.datacenter_resource_aggregator import (
    DatacenterResourceAggregator,
)
from hyperscale.distributed.resources.manager_resource_report import ManagerResourceReport
from hyperscale.distributed.resources.workload_resource_totals import WorkloadResourceTotals
from hyperscale.distributed.runtime import RealClock

HEALTHY_WORKER = "worker-healthy"
UNHEALTHY_WORKER = "worker-unhealthy"
STALENESS_SECONDS = 30.0
CPU_CAPACITY = 800.0
MEMORY_CAPACITY = 16 * 1024**3


def report(cpu_percent: float) -> ManagerResourceReport:
    return ManagerResourceReport(
        manager_metrics=None,
        workload=WorkloadResourceTotals(cpu_percent=cpu_percent, memory_bytes=1024**3),
        cpu_capacity_percent=CPU_CAPACITY,
        memory_capacity_bytes=MEMORY_CAPACITY,
    )


def make_manager() -> ManagerServer:
    manager = object.__new__(ManagerServer)
    workers = [
        (HEALTHY_WORKER, SimpleNamespace(available_cores=3, total_cores=4)),
        (UNHEALTHY_WORKER, SimpleNamespace(available_cores=4, total_cores=4)),
    ]
    manager._manager_state = SimpleNamespace(
        iter_workers=lambda: workers,
        manager_state_enum=SimpleNamespace(value="active"),
        get_worker_count=lambda: len(workers),
        get_active_manager_peers=lambda: {("manager-2", 8231), ("manager-1", 8231)},
    )
    manager._worker_health_monitor = SimpleNamespace(
        get_worker_health_status=lambda worker_id: "healthy" if worker_id == HEALTHY_WORKER else "unhealthy",
        get_healthy_worker_count=lambda: 1,
    )
    manager._job_manager = SimpleNamespace(
        iter_jobs=lambda: [
            SimpleNamespace(job_id="job-running", status="running", workflows_total=3, workflows_completed=1),
            SimpleNamespace(job_id="job-done", status="completed", workflows_total=2, workflows_completed=2),
        ]
    )
    manager._node_id = SimpleNamespace(full="manager-0-full", datacenter="dc-a")
    manager._host = "manager-0"
    manager._tcp_port = 8231
    manager._leader_election = SimpleNamespace(state=SimpleNamespace(current_term=7))
    manager.is_leader = lambda: True
    manager._resource_gossip = ManagerResourceGossip(
        datacenter="dc-a",
        own_address=("manager-0", 8231),
        aggregator=DatacenterResourceAggregator(RealClock(), STALENESS_SECONDS),
    )
    return manager


@pytest.mark.asyncio
async def test_a_ping_is_answered_with_the_managers_status() -> None:
    manager = make_manager()

    payload = await ManagerServer.ping(manager, ("10.0.0.20", 8500), PingRequest(request_id="ping-1").dump(), 0)

    response = ManagerPingResponse.load(payload)
    assert (response.request_id, response.manager_id, response.datacenter) == ("ping-1", "manager-0-full", "dc-a")
    assert (response.host, response.port, response.is_leader, response.term) == ("manager-0", 8231, True, 7)
    assert (response.total_cores, response.available_cores) == (8, 3)
    assert (response.worker_count, response.healthy_worker_count) == (2, 1)
    assert response.active_job_ids == ["job-running"]
    assert (response.active_job_count, response.active_workflow_count) == (1, 2)
    assert response.peer_managers == [("manager-1", 8231), ("manager-2", 8231)]
    # No report yet knows the datacenter's capacity.
    assert response.resources is None


@pytest.mark.asyncio
async def test_a_ping_carries_the_datacenters_gossiped_resource_view() -> None:
    manager = make_manager()
    manager._resource_gossip.record_own_report(report(cpu_percent=100.0))
    assert manager._resource_gossip.receive(
        ManagerResourceGossipMessage(
            datacenter="dc-a",
            entries=[
                ManagerResourceGossipEntry(
                    manager_address=("manager-1", 8231),
                    report=report(cpu_percent=300.0),
                    age_seconds=1.0,
                )
            ],
        )
    )

    payload = await ManagerServer.ping(manager, ("10.0.0.20", 8500), PingRequest(request_id="ping-2").dump(), 0)

    resources = ManagerPingResponse.load(payload).resources
    assert resources is not None
    assert (resources.datacenter, resources.reporting_manager_count) == ("dc-a", 2)
    assert resources.workload_cpu_percent == 400.0
    assert resources.cpu_pressure == 400.0 / CPU_CAPACITY
