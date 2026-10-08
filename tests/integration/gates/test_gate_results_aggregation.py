"""
A gate aggregates a job's results across datacenters: with one gate, a
manager and a worker in each of DC-ALPHA and DC-BETA, a job a client
submits to the gate for both datacenters completes with a per-datacenter
breakdown from both, totals that are the sum of the datacenters', and
cross-datacenter AggregatedJobStats that are internally consistent.

Topology::

  Client -> Gate -> [Manager DC-ALPHA, Manager DC-BETA] -> Workers
            Gate <- JobFinalResult from each datacenter
  Client <- GlobalJobResult (per-datacenter results + aggregated stats)
"""

import asyncio
import pathlib
from collections.abc import AsyncIterator

import pytest

from hyperscale.distributed.models import JobStatus
from hyperscale.distributed.nodes import GateServer, ManagerServer, WorkerServer
from hyperscale.distributed.nodes.client import HyperscaleClient
from hyperscale.graph import Workflow, step
from hyperscale.testing import URL, HTTPResponse
from tests.integration.gates.gate_cluster import new_gates, new_workers, start_nodes, tcp_addresses
from tests.integration.in_process_nodes import (
    LOCALHOST,
    node_env,
    reserve_node_ports,
    reserve_cluster_ports,
    stop_nodes,
    wait_until,
)

DATACENTER_IDS = ("DC-ALPHA", "DC-BETA")
WORKER_CORES = 2
# The script left every node's request timeout at the Env default.
REQUEST_TIMEOUT = "30s"
GATE_START_SECONDS = 15.0
MANAGER_START_SECONDS = 15.0
WORKER_START_SECONDS = 30.0
# The script this replaces gave each node 20s to become leader, each
# worker 2s to register and the gate 5s to learn both datacenters'
# managers from their heartbeats; these bound the same convergence.
LEADER_ELECTION_SECONDS = 20.0
WORKER_REGISTRATION_SECONDS = 30.0
GATE_DATACENTER_DISCOVERY_SECONDS = 30.0
JOB_SUBMISSION_SECONDS = 15.0
JOB_COMPLETION_SECONDS = 120.0
CLIENT_STOP_SECONDS = 5.0
SHUTDOWN_SECONDS = 30.0


class HttpbinWorkflow(Workflow):
    """Makes HTTP calls; distributed across both datacenters' workers."""

    vus: int = 2
    duration: str = "2s"

    @step()
    async def load_test_step(
        self,
        url: URL = "https://httpbin.org/get",
    ) -> HTTPResponse:
        return await self.client.http.get(url)


@pytest.fixture
async def gate_over_two_datacenters(
    node_directory: pathlib.Path,
) -> AsyncIterator[tuple[GateServer, list[ManagerServer], list[WorkerServer]]]:
    """The gate, leader of itself; then one manager per datacenter, each
    registering with the gate and leading its datacenter; then one worker
    per datacenter registered with its manager, and the gate tracking both
    datacenters' managers. Every node is stopped at teardown."""
    node_ports, worker_ports = reserve_cluster_ports(1 + len(DATACENTER_IDS), [WORKER_CORES] * len(DATACENTER_IDS))
    gate_port, manager_ports = node_ports[0], node_ports[1:]
    [gate] = new_gates(node_directory, [gate_port], {}, MERCURY_SYNC_REQUEST_TIMEOUT=REQUEST_TIMEOUT)
    # Each manager knows only the gate's TCP address and registers with it.
    managers = [
        ManagerServer(
            host=LOCALHOST,
            tcp_port=manager_port,
            udp_port=manager_port + 1,
            env=node_env(node_directory, MERCURY_SYNC_REQUEST_TIMEOUT=REQUEST_TIMEOUT),
            dc_id=datacenter_id,
            gate_addrs=tcp_addresses([gate_port]),
        )
        for datacenter_id, manager_port in zip(DATACENTER_IDS, manager_ports, strict=True)
    ]
    workers = [
        worker
        for datacenter_id, manager_port, worker_port in zip(DATACENTER_IDS, manager_ports, worker_ports, strict=True)
        for worker in new_workers(
            node_directory,
            datacenter_id,
            [worker_port],
            [manager_port],
            WORKER_CORES,
            MERCURY_SYNC_REQUEST_TIMEOUT=REQUEST_TIMEOUT,
        )
    ]
    try:
        await start_nodes([gate], GATE_START_SECONDS)
        await wait_until(gate.is_leader, within_seconds=LEADER_ELECTION_SECONDS, description="the gate becoming leader")

        for manager in managers:
            await start_nodes([manager], MANAGER_START_SECONDS)
            await wait_until(
                manager.is_leader,
                within_seconds=LEADER_ELECTION_SECONDS,
                description=f"manager {manager._node_id.short} becoming its datacenter's leader",
            )

        for manager, worker in zip(managers, workers, strict=True):
            await start_nodes([worker], WORKER_START_SECONDS)
            await wait_until(
                lambda: manager._manager_state.get_worker_count() > 0,
                within_seconds=WORKER_REGISTRATION_SECONDS,
                description=f"worker {worker._node_id.short} registering with manager {manager._node_id.short}",
            )

        await wait_until(
            lambda: len(gate._modular_state._datacenter_manager_status) >= len(DATACENTER_IDS),
            within_seconds=GATE_DATACENTER_DISCOVERY_SECONDS,
            description=f"the gate tracking managers in {len(DATACENTER_IDS)} datacenters",
        )
        yield gate, managers, workers
    finally:
        await stop_nodes([*workers, *managers, gate], SHUTDOWN_SECONDS)


async def test_gate_aggregates_job_results_across_both_datacenters(
    node_directory: pathlib.Path,
    gate_over_two_datacenters: tuple[GateServer, list[ManagerServer], list[WorkerServer]],
) -> None:
    gate, _, _ = gate_over_two_datacenters

    # Every node is bound by now, so this scan skips their ports.
    [client_port] = reserve_node_ports(1)
    client = HyperscaleClient(
        host=LOCALHOST,
        port=client_port,
        env=node_env(node_directory, MERCURY_SYNC_REQUEST_TIMEOUT=REQUEST_TIMEOUT),
        gates=tcp_addresses([gate._tcp_port]),
    )
    await client.start()
    try:
        job_id = await asyncio.wait_for(
            client.submit_job(
                workflows=[([], HttpbinWorkflow())],
                vus=2,
                timeout_seconds=60.0,
                datacenter_count=len(DATACENTER_IDS),
            ),
            timeout=JOB_SUBMISSION_SECONDS,
        )
        result = await asyncio.wait_for(
            client.wait_for_job(job_id, timeout=JOB_COMPLETION_SECONDS),
            timeout=JOB_COMPLETION_SECONDS + 5.0,
        )
    finally:
        await asyncio.wait_for(client.stop(), timeout=CLIENT_STOP_SECONDS)

    per_datacenter_results = result.per_datacenter_results
    aggregated = result.aggregated

    assert aggregated is not None, "the job result carries no cross-datacenter aggregated stats"
    assert len(per_datacenter_results) >= len(DATACENTER_IDS), (
        f"results arrived from only {len(per_datacenter_results)} datacenters"
    )
    reported_datacenters = [datacenter_result.datacenter for datacenter_result in per_datacenter_results]
    assert set(DATACENTER_IDS) <= set(reported_datacenters), (
        f"per-datacenter stats missing a datacenter: {reported_datacenters}"
    )

    summed_completed = sum(datacenter_result.total_completed for datacenter_result in per_datacenter_results)
    summed_failed = sum(datacenter_result.total_failed for datacenter_result in per_datacenter_results)
    assert result.total_completed == summed_completed, (
        f"aggregated completed {result.total_completed} != sum of datacenters {summed_completed}"
    )
    assert result.total_failed == summed_failed, (
        f"aggregated failed {result.total_failed} != sum of datacenters {summed_failed}"
    )

    assert aggregated.total_requests == aggregated.successful_requests + aggregated.failed_requests, (
        f"AggregatedJobStats total_requests {aggregated.total_requests} != successful "
        f"{aggregated.successful_requests} + failed {aggregated.failed_requests}"
    )
    percentiles = (aggregated.p50_latency_ms, aggregated.p95_latency_ms, aggregated.p99_latency_ms)
    assert percentiles == (0.0, 0.0, 0.0) or percentiles[0] <= percentiles[1] <= percentiles[2], (
        f"latency percentiles out of order: p50={percentiles[0]}, p95={percentiles[1]}, p99={percentiles[2]}"
    )

    assert result.status == JobStatus.COMPLETED.value, (
        f"job ended {result.status}, not completed; per datacenter: {result.per_datacenter_statuses}"
    )
