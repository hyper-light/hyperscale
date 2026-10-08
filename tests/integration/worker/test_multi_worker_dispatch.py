"""
A job of four workflows on two 4-core workers under a three-manager
datacenter, submitted by a client straight to the managers (no gates):

- the two workflows with no dependencies run together, while both
  dependents wait pending;
- the dependent of the short workflow is dispatched once that workflow
  completes, while the long one still runs and the dependent of both
  still waits;
- the dependent of both is dispatched once the long workflow completes;
- every workflow completes, the client is pushed each workflow's result
  and windowed progress stats for both independent workflows, and the
  client's job result holds all four workflow results.

The two independent workflows call https://httpbin.org/get, so the test
needs outbound network access.
"""

import asyncio
import pathlib
from collections.abc import Collection

from hyperscale.distributed.jobs import WindowedStatsPush
from hyperscale.distributed.models import JobStatusPush, WorkflowResultPush, WorkflowStatus, WorkflowStatusInfo
from hyperscale.distributed.nodes import HyperscaleClient, WorkerServer
from hyperscale.graph import Workflow, depends, step
from hyperscale.testing import URL, HTTPResponse
from tests.integration.in_process_nodes import (
    LOCALHOST,
    node_env,
    reserve_cluster_ports,
    wait_until,
)
from tests.integration.worker.worker_cluster import (
    WORKER_REGISTRATION_SECONDS,
    new_manager_cluster,
    running_cluster,
)


class LongHttpWorkflow(Workflow):
    vus = 2000
    duration = "20s"

    @step()
    async def get_httpbin(
        self,
        url: URL = "https://httpbin.org/get",
    ) -> HTTPResponse:
        return await self.client.http.get(url)


class ShortHttpWorkflow(Workflow):
    vus = 500
    duration = "5s"

    @step()
    async def get_httpbin(
        self,
        url: URL = "https://httpbin.org/get",
    ) -> HTTPResponse:
        return await self.client.http.get(url)


@depends("ShortHttpWorkflow")
class DependsOnShortWorkflow(Workflow):
    """Waits for ShortHttpWorkflow to complete."""

    vus = 100
    duration = "3s"

    @step()
    async def second_step(self) -> dict:
        return {"status": "done"}


@depends("LongHttpWorkflow", "ShortHttpWorkflow")
class DependsOnBothWorkflow(Workflow):
    """Waits for both LongHttpWorkflow and ShortHttpWorkflow to complete."""

    vus = 100
    duration = "3s"

    @step()
    async def second_step(self) -> dict:
        return {"status": "done"}


DATACENTER_ID = "DC-EAST"
MANAGER_COUNT = 3
WORKER_CORES = [4, 4]
LOG_LEVEL = "error"
NODE_REQUEST_TIMEOUT = "5s"
CLIENT_REQUEST_TIMEOUT = "10s"
JOB_TIMEOUT_SECONDS = 120.0
CLIENT_STOP_SECONDS = 30.0
INDEPENDENT_WORKFLOWS = ["LongHttpWorkflow", "ShortHttpWorkflow"]
DEPENDENT_WORKFLOWS = ["DependsOnShortWorkflow", "DependsOnBothWorkflow"]
ALL_WORKFLOWS = [*INDEPENDENT_WORKFLOWS, *DEPENDENT_WORKFLOWS]
DISPATCHED_STATUSES = {WorkflowStatus.RUNNING.value, WorkflowStatus.ASSIGNED.value}
DISPATCHED_OR_DONE_STATUSES = {*DISPATCHED_STATUSES, WorkflowStatus.COMPLETED.value}
PENDING_STATUSES = {WorkflowStatus.PENDING.value}
COMPLETED_STATUSES = {WorkflowStatus.COMPLETED.value}
# The script this test replaces waited 2 s for dispatch to begin and 1 s
# for each dependent's eager dispatch; each wait here ends once the
# statuses hold, within these bounds.
DISPATCH_SECONDS = 10.0
# The script polled up to 60 s for each workflow to complete.
COMPLETION_SECONDS = 60.0
PUSH_DELIVERY_SECONDS = 10.0
STATUS_POLL_SECONDS = 1.0


async def wait_for_workflow_statuses(
    client: HyperscaleClient,
    job_id: str,
    expected_statuses: dict[str, Collection[str]],
    within_seconds: float,
) -> dict[str, WorkflowStatusInfo]:
    """Poll the managers until each named workflow's status is one of its
    expected statuses; the statuses of the last poll. Fails naming the
    statuses last seen if they do not all hold within ``within_seconds``."""
    deadline = asyncio.get_running_loop().time() + within_seconds
    while True:
        results = await client.query_workflows(list(expected_statuses), job_id=job_id)
        statuses_by_name = {
            workflow.workflow_name: workflow
            for datacenter_workflows in results.values()
            for workflow in datacenter_workflows
        }
        if all(
            name in statuses_by_name and statuses_by_name[name].status in allowed
            for name, allowed in expected_statuses.items()
        ):
            return statuses_by_name
        last_seen = {name: workflow.status for name, workflow in statuses_by_name.items()}
        assert asyncio.get_running_loop().time() < deadline, (
            f"workflow statuses did not reach {expected_statuses} within {within_seconds}s; last seen: {last_seen}"
        )
        await asyncio.sleep(STATUS_POLL_SECONDS)


async def test_dependent_workflows_dispatch_as_their_dependencies_complete(node_directory: pathlib.Path) -> None:
    # The client binds a TCP/UDP pair like a manager: one more node block.
    node_tcp_ports, worker_tcp_ports = reserve_cluster_ports(MANAGER_COUNT + 1, WORKER_CORES)
    *manager_tcp_ports, client_tcp_port = node_tcp_ports
    managers = new_manager_cluster(
        node_directory,
        DATACENTER_ID,
        manager_tcp_ports,
        MERCURY_SYNC_LOG_LEVEL=LOG_LEVEL,
        MERCURY_SYNC_REQUEST_TIMEOUT=NODE_REQUEST_TIMEOUT,
    )
    manager_addresses = [(LOCALHOST, manager._tcp_port) for manager in managers]
    workers = [
        WorkerServer(
            host=LOCALHOST,
            tcp_port=worker_tcp_port,
            udp_port=worker_tcp_port + 1,
            env=node_env(
                node_directory,
                MERCURY_SYNC_LOG_LEVEL=LOG_LEVEL,
                MERCURY_SYNC_REQUEST_TIMEOUT=NODE_REQUEST_TIMEOUT,
                WORKER_MAX_CORES=worker_cores,
            ),
            dc_id=DATACENTER_ID,
            seed_managers=manager_addresses,
        )
        for worker_tcp_port, worker_cores in zip(worker_tcp_ports, WORKER_CORES, strict=True)
    ]

    status_pushes: list[JobStatusPush] = []
    progress_pushes: list[WindowedStatsPush] = []
    workflow_result_statuses: dict[str, str] = {}

    def on_workflow_result(push: WorkflowResultPush) -> None:
        workflow_result_statuses[push.workflow_name] = push.status

    async with running_cluster(managers, workers):
        worker_ids = {worker._node_id.full for worker in workers}
        await wait_until(
            lambda: all(set(manager._manager_state.get_all_workers()) == worker_ids for manager in managers),
            within_seconds=WORKER_REGISTRATION_SECONDS,
            description=f"every manager tracking both workers ({sum(WORKER_CORES)} cores)",
        )

        client = HyperscaleClient(
            host=LOCALHOST,
            port=client_tcp_port,
            env=node_env(node_directory, MERCURY_SYNC_REQUEST_TIMEOUT=CLIENT_REQUEST_TIMEOUT),
            managers=manager_addresses,
        )
        try:
            await client.start()
            job_id = await client.submit_job(
                workflows=[
                    ([], LongHttpWorkflow()),
                    ([], ShortHttpWorkflow()),
                    (["ShortHttpWorkflow"], DependsOnShortWorkflow()),
                    (["LongHttpWorkflow", "ShortHttpWorkflow"], DependsOnBothWorkflow()),
                ],
                timeout_seconds=JOB_TIMEOUT_SECONDS,
                on_status_update=status_pushes.append,
                on_workflow_result=on_workflow_result,
                on_progress_update=progress_pushes.append,
            )

            # Both independent workflows run together; both dependents wait.
            initial_statuses = await wait_for_workflow_statuses(
                client,
                job_id,
                {
                    **{name: DISPATCHED_STATUSES for name in INDEPENDENT_WORKFLOWS},
                    **{name: PENDING_STATUSES for name in DEPENDENT_WORKFLOWS},
                },
                within_seconds=DISPATCH_SECONDS,
            )
            enqueued_dependents = {name: initial_statuses[name].is_enqueued for name in DEPENDENT_WORKFLOWS}
            assert all(enqueued_dependents.values()), f"pending dependents should be enqueued: {enqueued_dependents}"

            # The short workflow completes: its dependent is dispatched, the
            # long workflow still runs, the dependent of both still waits.
            await wait_for_workflow_statuses(
                client, job_id, {"ShortHttpWorkflow": COMPLETED_STATUSES}, within_seconds=COMPLETION_SECONDS
            )
            await wait_for_workflow_statuses(
                client, job_id, {"DependsOnShortWorkflow": DISPATCHED_OR_DONE_STATUSES}, within_seconds=DISPATCH_SECONDS
            )
            await wait_for_workflow_statuses(
                client,
                job_id,
                {"LongHttpWorkflow": DISPATCHED_STATUSES, "DependsOnBothWorkflow": PENDING_STATUSES},
                within_seconds=0.0,
            )

            # The long workflow completes: the dependent of both is dispatched.
            await wait_for_workflow_statuses(
                client, job_id, {"LongHttpWorkflow": COMPLETED_STATUSES}, within_seconds=COMPLETION_SECONDS
            )
            await wait_for_workflow_statuses(
                client, job_id, {"DependsOnBothWorkflow": DISPATCHED_OR_DONE_STATUSES}, within_seconds=DISPATCH_SECONDS
            )

            await wait_for_workflow_statuses(
                client,
                job_id,
                {name: COMPLETED_STATUSES for name in DEPENDENT_WORKFLOWS},
                within_seconds=COMPLETION_SECONDS,
            )

            await wait_until(
                lambda: set(workflow_result_statuses) >= set(ALL_WORKFLOWS),
                within_seconds=PUSH_DELIVERY_SECONDS,
                description="a workflow result pushed to the client for each of the four workflows",
            )
            assert set(workflow_result_statuses) == set(ALL_WORKFLOWS), (
                f"workflow results pushed for {sorted(workflow_result_statuses)}, expected {sorted(ALL_WORKFLOWS)}"
            )

            assert len(progress_pushes) > 0, "no windowed progress stats were pushed to the client"
            progress_push_counts = {
                name: sum(1 for push in progress_pushes if push.workflow_name == name) for name in ALL_WORKFLOWS
            }
            assert all(progress_push_counts[name] > 0 for name in INDEPENDENT_WORKFLOWS), (
                f"both independent workflows should have pushed progress stats: {progress_push_counts}"
            )

            job_result = client.get_job_status(job_id)
            assert job_result is not None, f"the client holds no result for job {job_id}"
            job_workflow_results = {
                workflow_id: (result.workflow_name, result.status)
                for workflow_id, result in job_result.workflow_results.items()
            }
            assert len(job_workflow_results) == len(ALL_WORKFLOWS), (
                f"the job result should hold {len(ALL_WORKFLOWS)} workflow results, got {job_workflow_results}"
            )
        finally:
            await asyncio.wait_for(client.stop(), timeout=CLIENT_STOP_SECONDS)
