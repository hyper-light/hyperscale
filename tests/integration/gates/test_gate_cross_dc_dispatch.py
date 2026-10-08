"""
A gate cluster runs one job across two datacenters: with three gates and,
in each of DC-EAST and DC-WEST, three managers (a quorum) and four
two-core workers, a job of four workflows submitted through the gates
starts its two independent workflows concurrently, runs the dependent
ones after their dependencies, completes every workflow, streams
windowed stats for both independent workflows to the client, pushes
each workflow's aggregated result to the client, and fills the job's
workflow results.

Workflow dependencies:
  - LongHttpbinWorkflow: none
  - ShortHttpbinWorkflow: none
  - AfterShortWorkflow: ShortHttpbinWorkflow
  - AfterBothWorkflow: LongHttpbinWorkflow and ShortHttpbinWorkflow
"""

import asyncio
import pathlib
import time
from collections.abc import AsyncIterator

import pytest

from hyperscale.distributed.jobs import WindowedStatsPush
from hyperscale.distributed.models import WorkflowResultPush
from hyperscale.distributed.nodes import GateServer, ManagerServer, WorkerServer
from hyperscale.distributed.nodes.client import HyperscaleClient
from hyperscale.graph import Workflow, depends, step
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

DATACENTER_IDS = ("DC-EAST", "DC-WEST")
GATE_COUNT = 3
MANAGERS_PER_DATACENTER = 3
WORKERS_PER_DATACENTER = 4
WORKER_CORES = 2
NODE_START_SECONDS = 30.0
WORKER_START_SECONDS = 60.0
# The script this replaces waited 15s each for the gate cluster, the
# manager clusters and worker registration; these bound the same
# convergence.
CLUSTER_CONVERGENCE_SECONDS = 60.0
WORKER_REGISTRATION_SECONDS = 60.0
# The script checked the independent workflows 3s after submission.
CONCURRENT_START_SECONDS = 30.0
WORKFLOW_COMPLETION_SECONDS = 90.0
# The script gave the final pushes 2s after the workflows completed.
FINAL_PUSH_SECONDS = 30.0
STATUS_POLL_SECONDS = 1.0
CLIENT_STOP_SECONDS = 10.0
SHUTDOWN_SECONDS = 30.0
NODE_ENV_OVERRIDES: dict[str, str] = {"MERCURY_SYNC_LOG_LEVEL": "error", "MERCURY_SYNC_REQUEST_TIMEOUT": "5s"}


class LongHttpbinWorkflow(Workflow):
    vus: int = 2000
    duration: str = "20s"

    @step()
    async def get_httpbin(
        self,
        url: URL = "https://httpbin.org/get",
    ) -> HTTPResponse:
        return await self.client.http.get(url)


class ShortHttpbinWorkflow(Workflow):
    vus: int = 500
    duration: str = "5s"

    @step()
    async def get_httpbin(
        self,
        url: URL = "https://httpbin.org/get",
    ) -> HTTPResponse:
        return await self.client.http.get(url)


@depends("ShortHttpbinWorkflow")
class AfterShortWorkflow(Workflow):
    """Waits for ShortHttpbinWorkflow to complete."""

    vus: int = 100
    duration: str = "3s"

    @step()
    async def second_step(self) -> dict[str, str]:
        return {"status": "done"}


@depends("LongHttpbinWorkflow", "ShortHttpbinWorkflow")
class AfterBothWorkflow(Workflow):
    """Waits for both httpbin workflows to complete."""

    vus: int = 100
    duration: str = "3s"

    @step()
    async def second_step(self) -> dict[str, str]:
        return {"status": "done"}


INDEPENDENT_WORKFLOW_NAMES = frozenset({"LongHttpbinWorkflow", "ShortHttpbinWorkflow"})
ALL_WORKFLOW_NAMES = frozenset(
    {"LongHttpbinWorkflow", "ShortHttpbinWorkflow", "AfterShortWorkflow", "AfterBothWorkflow"}
)


@pytest.fixture
async def gate_tier_over_two_datacenters(
    node_directory: pathlib.Path,
) -> AsyncIterator[list[GateServer]]:
    """The gates, converged on a leader; then both datacenters' managers,
    each datacenter converged on a leader; then the workers, all
    registered with their datacenter's leader. Every node is stopped at
    teardown."""
    node_ports, worker_ports = reserve_cluster_ports(
        GATE_COUNT + MANAGERS_PER_DATACENTER * len(DATACENTER_IDS),
        [WORKER_CORES] * WORKERS_PER_DATACENTER * len(DATACENTER_IDS),
    )
    gate_ports = node_ports[:GATE_COUNT]
    manager_ports_by_datacenter = {
        datacenter_id: node_ports[
            GATE_COUNT + datacenter_index * MANAGERS_PER_DATACENTER : GATE_COUNT
            + (datacenter_index + 1) * MANAGERS_PER_DATACENTER
        ]
        for datacenter_index, datacenter_id in enumerate(DATACENTER_IDS)
    }
    gates = new_gates(node_directory, gate_ports, manager_ports_by_datacenter, **NODE_ENV_OVERRIDES)
    managers_by_datacenter: dict[str, list[ManagerServer]] = {
        datacenter_id: new_managers(node_directory, datacenter_id, manager_ports, gate_ports, **NODE_ENV_OVERRIDES)
        for datacenter_id, manager_ports in manager_ports_by_datacenter.items()
    }
    workers: list[WorkerServer] = [
        worker
        for datacenter_index, (datacenter_id, manager_ports) in enumerate(manager_ports_by_datacenter.items())
        for worker in new_workers(
            node_directory,
            datacenter_id,
            worker_ports[
                datacenter_index * WORKERS_PER_DATACENTER : (datacenter_index + 1) * WORKERS_PER_DATACENTER
            ],
            manager_ports,
            WORKER_CORES,
            **NODE_ENV_OVERRIDES,
        )
    ]
    all_managers = [manager for managers in managers_by_datacenter.values() for manager in managers]
    try:
        await start_nodes(gates, NODE_START_SECONDS)
        await wait_until(
            lambda: any(gate.is_leader() for gate in gates),
            within_seconds=CLUSTER_CONVERGENCE_SECONDS,
            description="the gate cluster electing a leader",
        )
        await start_nodes(all_managers, NODE_START_SECONDS)
        await wait_until(
            lambda: all(
                any(manager.is_leader() for manager in managers) for managers in managers_by_datacenter.values()
            ),
            within_seconds=CLUSTER_CONVERGENCE_SECONDS,
            description="every datacenter's managers electing a leader",
        )
        await start_nodes(workers, WORKER_START_SECONDS)
        await wait_until(
            lambda: all(
                any(
                    manager.is_leader() and manager._manager_state.get_worker_count() >= WORKERS_PER_DATACENTER
                    for manager in managers
                )
                for managers in managers_by_datacenter.values()
            ),
            within_seconds=WORKER_REGISTRATION_SECONDS,
            description=f"{WORKERS_PER_DATACENTER} workers registering with each datacenter's manager leader",
        )
        yield gates
    finally:
        await stop_nodes([*workers, *all_managers, *gates], SHUTDOWN_SECONDS)


@pytest.fixture
async def gate_client(
    node_directory: pathlib.Path,
    gate_tier_over_two_datacenters: list[GateServer],
) -> AsyncIterator[HyperscaleClient]:
    """A client of every gate, stopped at teardown."""
    # Every node is bound by now, so this scan skips their ports.
    [client_port] = reserve_node_ports(1)
    client = HyperscaleClient(
        host=LOCALHOST,
        port=client_port,
        env=node_env(node_directory, MERCURY_SYNC_REQUEST_TIMEOUT="10s"),
        gates=tcp_addresses(gate._tcp_port for gate in gate_tier_over_two_datacenters),
    )
    await client.start()
    try:
        yield client
    finally:
        await asyncio.wait_for(client.stop(), timeout=CLIENT_STOP_SECONDS)


async def test_job_runs_its_workflows_across_both_datacenters_and_reports_to_the_client(
    gate_client: HyperscaleClient,
) -> None:
    workflow_result_statuses: dict[str, str] = {}
    progress_pushes: list[WindowedStatsPush] = []

    def on_workflow_result(push: WorkflowResultPush) -> None:
        workflow_result_statuses[push.workflow_name] = push.status

    def on_progress_update(push: WindowedStatsPush) -> None:
        progress_pushes.append(push)

    job_id = await gate_client.submit_job(
        workflows=[
            ([], LongHttpbinWorkflow()),
            ([], ShortHttpbinWorkflow()),
            (["ShortHttpbinWorkflow"], AfterShortWorkflow()),
            (["LongHttpbinWorkflow", "ShortHttpbinWorkflow"], AfterBothWorkflow()),
        ],
        timeout_seconds=120.0,
        datacenter_count=len(DATACENTER_IDS),
        on_workflow_result=on_workflow_result,
        on_progress_update=on_progress_update,
    )

    # Both independent workflows are running (or assigned) at once in
    # some datacenter.
    running_workflow_names: set[str] = set()
    concurrent_start_deadline = time.monotonic() + CONCURRENT_START_SECONDS
    while not INDEPENDENT_WORKFLOW_NAMES <= running_workflow_names and time.monotonic() < concurrent_start_deadline:
        workflow_statuses = await gate_client.query_workflows_via_gate(list(ALL_WORKFLOW_NAMES), job_id=job_id)
        running_workflow_names = {
            status_info.workflow_name
            for datacenter_statuses in workflow_statuses.values()
            for status_info in datacenter_statuses
            if status_info.status in ("running", "assigned")
        }
        await asyncio.sleep(STATUS_POLL_SECONDS)
    assert INDEPENDENT_WORKFLOW_NAMES <= running_workflow_names, (
        f"the independent workflows were not running together within {CONCURRENT_START_SECONDS}s; "
        f"last seen running or assigned: {sorted(running_workflow_names)}"
    )

    # Every workflow completes in at least one datacenter.
    completed_workflow_names: set[str] = set()
    completion_deadline = time.monotonic() + WORKFLOW_COMPLETION_SECONDS
    while completed_workflow_names != ALL_WORKFLOW_NAMES and time.monotonic() < completion_deadline:
        workflow_statuses = await gate_client.query_workflows_via_gate(list(ALL_WORKFLOW_NAMES), job_id=job_id)
        completed_workflow_names = {
            status_info.workflow_name
            for datacenter_statuses in workflow_statuses.values()
            for status_info in datacenter_statuses
            if status_info.status == "completed"
        }
        await asyncio.sleep(STATUS_POLL_SECONDS)
    assert completed_workflow_names == ALL_WORKFLOW_NAMES, (
        f"not every workflow completed within {WORKFLOW_COMPLETION_SECONDS}s; "
        f"completed: {sorted(completed_workflow_names)}"
    )

    await wait_until(
        lambda: set(workflow_result_statuses) == ALL_WORKFLOW_NAMES,
        within_seconds=FINAL_PUSH_SECONDS,
        description="a workflow result push reaching the client for each of the four workflows",
    )
    assert set(workflow_result_statuses) == ALL_WORKFLOW_NAMES, (
        f"workflow results missing {sorted(ALL_WORKFLOW_NAMES - set(workflow_result_statuses))}, "
        f"unexpected {sorted(set(workflow_result_statuses) - ALL_WORKFLOW_NAMES)}"
    )

    assert progress_pushes, "no windowed stats (progress) update reached the client"
    progress_push_counts = {
        workflow_name: sum(push.workflow_name == workflow_name for push in progress_pushes)
        for workflow_name in ALL_WORKFLOW_NAMES
    }
    assert all(progress_push_counts[workflow_name] > 0 for workflow_name in INDEPENDENT_WORKFLOW_NAMES), (
        f"windowed stats missing for an independent workflow; updates per workflow: {progress_push_counts}"
    )

    await wait_until(
        lambda: (job_result := gate_client.get_job_status(job_id)) is not None
        and len(job_result.workflow_results) == len(ALL_WORKFLOW_NAMES),
        within_seconds=FINAL_PUSH_SECONDS,
        description=f"the job's workflow results holding all {len(ALL_WORKFLOW_NAMES)} workflows",
    )
    job_result = gate_client.get_job_status(job_id)
    assert job_result is not None, f"the client holds no result for job {job_id}"
    assert len(job_result.workflow_results) == len(ALL_WORKFLOW_NAMES), (
        f"the job's workflow results hold {len(job_result.workflow_results)}/{len(ALL_WORKFLOW_NAMES)} workflows: "
        f"{sorted(job_result.workflow_results)}"
    )
