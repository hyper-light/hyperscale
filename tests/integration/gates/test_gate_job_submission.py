"""
A job submitted to the gate tier reaches it: with a three-gate cluster, a
three-manager datacenter registered with the gates and two workers
registered with the managers, each tier elects a leader, the manager
leader holds both workers, and a job a client submits to the gate leader
is tracked by that gate.
"""

import asyncio
import pathlib
from collections.abc import AsyncIterator

import pytest

from hyperscale.distributed.nodes import GateServer, ManagerServer, WorkerServer
from hyperscale.distributed.nodes.client import HyperscaleClient
from hyperscale.graph import Workflow, step
from hyperscale.testing import URL, HTTPResponse
from tests.integration.gates.gate_cluster import new_gates, new_managers, new_workers, start_nodes, tcp_addresses
from tests.integration.in_process_nodes import (
    LOCALHOST,
    node_env,
    reserve_node_ports,
    reserve_cluster_ports,
    stop_nodes,
    wait_until,
)

DATACENTER_ID = "DC-EAST"
GATE_COUNT = 3
MANAGER_COUNT = 3
WORKER_COUNT = 2
WORKER_CORES = 4
NODE_START_SECONDS = 30.0
WORKER_START_SECONDS = 60.0
# The script this replaces waited 20s for the gate and manager clusters,
# 8s for worker registration and 10s more for a manager heartbeat to
# carry the workers to the gates; these bound the same convergence.
CLUSTER_CONVERGENCE_SECONDS = 60.0
WORKER_REGISTRATION_SECONDS = 60.0
GATE_CAPACITY_VIEW_SECONDS = 60.0
CLIENT_STOP_SECONDS = 10.0
SHUTDOWN_SECONDS = 30.0


class HttpbinWorkflow(Workflow):
    vus: int = 2000
    duration: str = "15s"

    @step()
    async def get_httpbin(
        self,
        url: URL = "https://httpbin.org/get",
    ) -> HTTPResponse:
        return await self.client.http.get(url)


@pytest.fixture
async def gate_tier_with_one_datacenter(
    node_directory: pathlib.Path,
) -> AsyncIterator[tuple[list[GateServer], list[ManagerServer], list[WorkerServer]]]:
    """Gates, then the datacenter's managers, both converged on a leader;
    then the workers, registered with the manager leader and visible to
    the gate leader through the managers' heartbeats. Every node is
    stopped at teardown."""
    node_ports, worker_ports = reserve_cluster_ports(GATE_COUNT + MANAGER_COUNT, [WORKER_CORES] * WORKER_COUNT)
    gate_ports, manager_ports = node_ports[:GATE_COUNT], node_ports[GATE_COUNT:]
    gates = new_gates(node_directory, gate_ports, {DATACENTER_ID: manager_ports}, MERCURY_SYNC_LOG_LEVEL="error")
    managers = new_managers(
        node_directory, DATACENTER_ID, manager_ports, gate_ports, MERCURY_SYNC_LOG_LEVEL="error"
    )
    workers = new_workers(
        node_directory, DATACENTER_ID, worker_ports, manager_ports, WORKER_CORES, MERCURY_SYNC_LOG_LEVEL="error"
    )
    try:
        await start_nodes(gates, NODE_START_SECONDS)
        await start_nodes(managers, NODE_START_SECONDS)
        await wait_until(
            lambda: any(gate.is_leader() for gate in gates) and any(manager.is_leader() for manager in managers),
            within_seconds=CLUSTER_CONVERGENCE_SECONDS,
            description="a gate leader and a manager leader being elected",
        )
        await start_nodes(workers, WORKER_START_SECONDS)
        yield gates, managers, workers
    finally:
        await stop_nodes([*workers, *managers, *gates], SHUTDOWN_SECONDS)


async def test_job_submitted_to_the_gate_leader_is_tracked_by_it(
    node_directory: pathlib.Path,
    gate_tier_with_one_datacenter: tuple[list[GateServer], list[ManagerServer], list[WorkerServer]],
) -> None:
    gates, managers, _ = gate_tier_with_one_datacenter

    assert any(gate.is_leader() for gate in gates), "no gate leader was elected"
    assert any(manager.is_leader() for manager in managers), "no manager leader was elected"

    await wait_until(
        lambda: any(
            manager.is_leader() and manager._manager_state.get_worker_count() >= WORKER_COUNT for manager in managers
        ),
        within_seconds=WORKER_REGISTRATION_SECONDS,
        description=f"{WORKER_COUNT} workers registering with the manager leader",
    )
    manager_leader = next(manager for manager in managers if manager.is_leader())
    registered_worker_count = manager_leader._manager_state.get_worker_count()
    assert registered_worker_count >= WORKER_COUNT, (
        f"only {registered_worker_count}/{WORKER_COUNT} workers registered with the manager leader"
    )

    # The gate leader dispatches by the capacity the managers' heartbeats
    # report: wait for a heartbeat carrying the registered workers.
    await wait_until(
        lambda: any(
            gate.is_leader()
            and any(
                manager_heartbeat.worker_count >= WORKER_COUNT
                for manager_heartbeat in gate._modular_state.get_datacenter_manager_statuses(DATACENTER_ID).values()
            )
            for gate in gates
        ),
        within_seconds=GATE_CAPACITY_VIEW_SECONDS,
        description=f"the gate leader seeing {WORKER_COUNT} workers in {DATACENTER_ID} through manager heartbeats",
    )

    gate_leader = next((gate for gate in gates if gate.is_leader()), None)
    assert gate_leader is not None, "no current gate leader to submit the job to"

    # Every node is bound by now, so this scan skips their ports.
    [client_port] = reserve_node_ports(1)
    client = HyperscaleClient(
        host=LOCALHOST,
        port=client_port,
        env=node_env(node_directory, MERCURY_SYNC_REQUEST_TIMEOUT="5s"),
        gates=tcp_addresses([gate_leader._tcp_port]),
    )
    await client.start()
    try:
        job_id = await client.submit_job(
            workflows=[([], HttpbinWorkflow())],
            vus=1,
            timeout_seconds=30.0,
            datacenter_count=1,
        )

        assert gate_leader._job_manager.has_job(job_id), (
            f"job {job_id} is not in the gate leader's job tracker; it tracks "
            f"{gate_leader._job_manager.get_all_job_ids()}"
        )
    finally:
        await asyncio.wait_for(client.stop(), timeout=CLIENT_STOP_SECONDS)
