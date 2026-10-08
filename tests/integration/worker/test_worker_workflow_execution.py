"""
A standalone 4-core worker executes dispatched workflows through its
workflow executor (the path a manager's dispatch takes once cores are
allocated): a plain workflow, a workflow whose ``Provide`` hook sets
context for a dependent, and a dependent workflow whose ``Use`` hook
reads context handed to it in the dispatch. Each completes, publishes a
COMPLETED final result, and frees every core it held.

No manager runs here: the final result is captured instead of sent, so
context written by the provider is not routed to the consumer; the
consumer's dispatch carries the provider's context as a manager would.
"""

import asyncio
import pathlib
from collections.abc import AsyncIterator

import cloudpickle
import pytest

from hyperscale.distributed.models import WorkflowDispatch, WorkflowFinalResult, WorkflowProgress, WorkflowStatus
from hyperscale.distributed.nodes import WorkerServer
from hyperscale.graph import Provide, Use, Workflow, depends, state, step
from tests.integration.in_process_nodes import LOCALHOST, node_env, reserve_worker_ports, stop_nodes

DATACENTER_ID = "DC-TEST"
JOB_ID = "test-job-001"
WORKER_CORES = 4
DISPATCH_VUS = 2
DISPATCH_TIMEOUT_SECONDS = 30.0
WORKER_START_SECONDS = 30.0
WORKER_STOP_SECONDS = 30.0


class SimpleWorkflow(Workflow):
    """Simple workflow with no context - just executes and returns."""

    vus = 10
    duration = "5s"

    @step()
    async def simple_action(self) -> dict:
        """Simple action that returns a dict."""
        await asyncio.sleep(0.1)
        return {"status": "ok", "value": 42}


class ProviderWorkflow(Workflow):
    """Workflow that provides context to dependent workflows."""

    vus = 10
    duration = "5s"

    @step()
    async def do_work(self) -> dict:
        """Do some work before providing context."""
        await asyncio.sleep(0.1)
        return {"computed": True}

    @state("ConsumerWorkflow")
    def provide_data(self) -> Provide[dict]:
        """Provide data to ConsumerWorkflow."""
        return {"shared_key": "shared_value", "counter": 100}


@depends("ProviderWorkflow")
class ConsumerWorkflow(Workflow):
    """Workflow that consumes context from ProviderWorkflow."""

    vus = 10
    duration = "5s"

    @state("ProviderWorkflow")
    def consume_data(self, provide_data: dict | None = None) -> Use[dict]:
        """Consume data from ProviderWorkflow."""
        self._received_context = provide_data
        return provide_data

    @step()
    async def process_with_context(self) -> dict:
        """Process using the consumed context."""
        await asyncio.sleep(0.1)
        return {
            "received_context": getattr(self, "_received_context", None),
            "processed": True,
        }


@pytest.fixture
async def standalone_worker(node_directory: pathlib.Path) -> AsyncIterator[WorkerServer]:
    """A started standalone worker of ``WORKER_CORES`` cores, stopped at
    teardown."""
    (worker_tcp_port,) = reserve_worker_ports([WORKER_CORES])
    worker = WorkerServer(
        host=LOCALHOST,
        tcp_port=worker_tcp_port,
        udp_port=worker_tcp_port + 1,
        env=node_env(node_directory, MERCURY_SYNC_LOG_LEVEL="info"),
        total_cores=WORKER_CORES,
        dc_id=DATACENTER_ID,
        seed_managers=[],
    )
    try:
        await asyncio.wait_for(worker.start(), timeout=WORKER_START_SECONDS)
        yield worker
    finally:
        await stop_nodes([worker], within_seconds=WORKER_STOP_SECONDS)


async def execute_dispatch(
    worker: WorkerServer,
    workflow: Workflow,
    workflow_id: str,
    context: dict[str, dict[str, object]],
) -> tuple[WorkflowProgress, list[WorkflowFinalResult]]:
    """Allocate cores for a dispatch of ``workflow`` and run it through the
    worker's workflow executor; the final progress and every final result
    the executor published."""
    dispatch = WorkflowDispatch(
        job_id=JOB_ID,
        workflow_id=workflow_id,
        workflow=cloudpickle.dumps(workflow),
        context=cloudpickle.dumps(context),
        vus=DISPATCH_VUS,
        timeout_seconds=DISPATCH_TIMEOUT_SECONDS,
        fence_token=1,
        context_version=0,
    )
    allocated_core_count = min(dispatch.vus, worker._total_cores)
    allocation = await worker._core_allocator.allocate(workflow_id, allocated_core_count)
    assert allocation.success, f"allocating {allocated_core_count} cores for {workflow_id} failed: {allocation.error}"

    progress = WorkflowProgress(
        job_id=dispatch.job_id,
        workflow_id=dispatch.workflow_id,
        workflow_name="",
        status=WorkflowStatus.PENDING.value,
        completed_count=0,
        failed_count=0,
        rate_per_second=0.0,
        elapsed_seconds=0.0,
        assigned_cores=allocation.allocated_cores,
    )
    published_final_results: list[WorkflowFinalResult] = []

    async def capture_final_result(final_result: WorkflowFinalResult) -> None:
        published_final_results.append(final_result)

    await asyncio.wait_for(
        worker._workflow_executor._execute_workflow(
            dispatch,
            progress,
            asyncio.Event(),
            dispatch.vus,
            allocated_core_count,
            worker._increment_version,
            worker._node_id.full,
            worker._host,
            worker._tcp_port,
            capture_final_result,
        ),
        timeout=DISPATCH_TIMEOUT_SECONDS,
    )
    return progress, published_final_results


async def assert_completed_and_cores_freed(
    worker: WorkerServer,
    workflow_name: str,
    progress: WorkflowProgress,
    published_final_results: list[WorkflowFinalResult],
) -> None:
    """The run COMPLETED, published one COMPLETED final result with no
    error, and left every core of the worker free."""
    assert progress.status == WorkflowStatus.COMPLETED.value, (
        f"{workflow_name} ended {progress.status}, expected {WorkflowStatus.COMPLETED.value}"
    )
    assert [final_result.status for final_result in published_final_results] == [WorkflowStatus.COMPLETED.value], (
        f"{workflow_name} should publish exactly one COMPLETED final result, got "
        f"{[(final_result.status, final_result.error) for final_result in published_final_results]}"
    )
    assert published_final_results[0].error is None, (
        f"{workflow_name} failed: {published_final_results[0].error}"
    )
    still_assigned = {
        core: workflow_id for core, workflow_id in (await worker.get_core_assignments()).items() if workflow_id
    }
    assert still_assigned == {}, f"cores still assigned after {workflow_name} ended: {still_assigned}"


async def test_simple_workflow_completes_and_frees_its_cores(standalone_worker: WorkerServer) -> None:
    progress, published_final_results = await execute_dispatch(
        standalone_worker, SimpleWorkflow(), "wf-simple-001", context={}
    )
    await assert_completed_and_cores_freed(standalone_worker, "SimpleWorkflow", progress, published_final_results)


async def test_providing_workflow_completes_and_frees_its_cores(standalone_worker: WorkerServer) -> None:
    progress, published_final_results = await execute_dispatch(
        standalone_worker, ProviderWorkflow(), "wf-provider-001", context={}
    )
    await assert_completed_and_cores_freed(standalone_worker, "ProviderWorkflow", progress, published_final_results)


async def test_consuming_workflow_completes_with_dispatched_context_and_frees_its_cores(
    standalone_worker: WorkerServer,
) -> None:
    provider_context: dict[str, dict[str, object]] = {
        "ProviderWorkflow": {"provide_data": {"shared_key": "shared_value", "counter": 100}},
    }
    progress, published_final_results = await execute_dispatch(
        standalone_worker, ConsumerWorkflow(), "wf-consumer-001", context=provider_context
    )
    await assert_completed_and_cores_freed(standalone_worker, "ConsumerWorkflow", progress, published_final_results)
