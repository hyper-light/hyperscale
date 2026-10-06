"""
A job's results are complete when waiting for it returns.

Workflow results travel apart from the terminal status, unordered with
it, so the client could report a job done before its last results
arrived, and a result push the manager failed to deliver was lost
outright. A directly submitted job's final result now carries every
workflow's results and fills in any the pushes did not deliver; a
completed job waits a bounded drain for results still in flight; the
workflows left without one are named on the result.

* a completed job waits for a late workflow result and returns as soon
  as it lands;
* a completed job whose result never lands returns after the drain,
  naming the workflow;
* a failed job returns at once, naming its workflows without results;
* a final result fills in the missing workflows (per-core stats merged
  into one), reports them to the caller's callback, keeps results
  already pushed, and completes the job's results;
* a gate's global result does the same for a workflow whose aggregated
  push was lost (a gate failover, a partition): every datacenter's stats
  merged, failed if any datacenter failed it, each datacenter's result
  kept;
* the manager sends a directly submitted job's final result to its
  client, and leaves a gate-routed job's to the gate.
"""

import asyncio
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from hyperscale.distributed.models import (
    GlobalJobResult,
    JobFinalResult,
    WorkflowResult,
    WorkflowResultPush,
)
from hyperscale.distributed.nodes.client.handlers import (
    global_job_result_handler as global_job_result_module,
    job_final_result_handler as job_final_result_module,
)
from hyperscale.distributed.nodes.client.handlers.tcp_job_result import (
    GlobalJobResultHandler,
    JobFinalResultHandler,
)
from hyperscale.distributed.nodes.client.handlers.tcp_workflow_result import (
    WorkflowResultPushHandler,
)
from hyperscale.distributed.nodes.client.state import ClientState
from hyperscale.distributed.nodes.client.tracking import ClientJobTracker
from hyperscale.distributed.nodes.manager.server import ManagerServer

JOB_ID = "job-1"
FIRST_WORKFLOW = "wf-1"
SECOND_WORKFLOW = "wf-2"
NEVER_REACHED_DRAIN_SECONDS = 30.0
SHORT_DRAIN_SECONDS = 0.05
CLIENT_CALLBACK = ("10.0.0.20", 8500)


class RecordingLogger:
    def __init__(self) -> None:
        self.entries: list[object] = []

    async def log(self, entry: object) -> None:
        self.entries.append(entry)


def make_client(
    drain_seconds: float,
) -> tuple[ClientState, ClientJobTracker, WorkflowResultPushHandler, RecordingLogger]:
    state = ClientState()
    logger = RecordingLogger()
    tracker = ClientJobTracker(state, logger, result_drain_timeout_seconds=drain_seconds)
    tracker.initialize_job_tracking(
        JOB_ID,
        expected_workflow_ids=frozenset({FIRST_WORKFLOW, SECOND_WORKFLOW}),
    )
    return state, tracker, WorkflowResultPushHandler(state, logger), logger


def pushed_result(workflow_id: str, stats: object) -> WorkflowResultPush:
    return WorkflowResultPush(
        job_id=JOB_ID,
        workflow_id=workflow_id,
        workflow_name=f"Workflow{workflow_id}",
        datacenter="dc-1",
        status="completed",
        results=[stats],
        is_client_ready=True,
    )


@pytest.mark.asyncio
async def test_a_completed_job_waits_for_a_late_workflow_result() -> None:
    _, tracker, workflow_results, _ = make_client(NEVER_REACHED_DRAIN_SECONDS)
    await workflow_results.apply(pushed_result(FIRST_WORKFLOW, "first-stats"))
    tracker.update_job_status(JOB_ID, "completed")

    async def deliver_late_result() -> None:
        await asyncio.sleep(0)
        await workflow_results.apply(pushed_result(SECOND_WORKFLOW, "second-stats"))

    started = time.monotonic()
    job, _ = await asyncio.gather(tracker.wait_for_job(JOB_ID), deliver_late_result())

    assert time.monotonic() - started < NEVER_REACHED_DRAIN_SECONDS / 2
    assert set(job.workflow_results) == {FIRST_WORKFLOW, SECOND_WORKFLOW}
    assert job.missing_workflow_results == []


@pytest.mark.asyncio
async def test_a_completed_job_names_a_result_that_never_lands() -> None:
    _, tracker, workflow_results, logger = make_client(SHORT_DRAIN_SECONDS)
    await workflow_results.apply(pushed_result(FIRST_WORKFLOW, "first-stats"))
    tracker.update_job_status(JOB_ID, "completed")

    job = await tracker.wait_for_job(JOB_ID)

    assert job.missing_workflow_results == [SECOND_WORKFLOW]
    assert any("not every workflow result arrived" in entry.message for entry in logger.entries)


@pytest.mark.asyncio
async def test_a_failed_job_returns_at_once_naming_unrun_workflows() -> None:
    _, tracker, workflow_results, _ = make_client(NEVER_REACHED_DRAIN_SECONDS)
    await workflow_results.apply(pushed_result(FIRST_WORKFLOW, "first-stats"))
    tracker.mark_job_failed(JOB_ID, "worker lost")

    started = time.monotonic()
    job = await tracker.wait_for_job(JOB_ID)

    assert time.monotonic() - started < NEVER_REACHED_DRAIN_SECONDS / 2
    assert job.missing_workflow_results == [SECOND_WORKFLOW]


class MergingResults:
    def merge_results(self, workflow_stats: list[str]) -> str:
        return "merged:" + "+".join(workflow_stats)


@pytest.mark.asyncio
async def test_a_final_result_fills_in_missing_workflow_results(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(job_final_result_module, "Results", MergingResults)
    state, tracker, workflow_results, logger = make_client(NEVER_REACHED_DRAIN_SECONDS)
    reported: list[WorkflowResultPush] = []
    state._workflow_callbacks[JOB_ID] = reported.append
    await workflow_results.apply(pushed_result(FIRST_WORKFLOW, "pushed-stats"))
    reported.clear()
    final_result = JobFinalResult(
        job_id=JOB_ID,
        datacenter="dc-1",
        status="completed",
        workflow_results=[
            WorkflowResult(
                workflow_id=FIRST_WORKFLOW,
                workflow_name="WorkflowFirst",
                status="completed",
                results=["core-a", "core-b"],
            ),
            WorkflowResult(
                workflow_id=SECOND_WORKFLOW,
                workflow_name="WorkflowSecond",
                status="completed",
                results=["core-c", "core-d"],
            ),
        ],
    )

    response = await JobFinalResultHandler(state, logger, workflow_results).handle(
        CLIENT_CALLBACK, final_result.dump(), 0
    )
    job = await tracker.wait_for_job(JOB_ID)

    assert response == b"ok"
    assert job.workflow_results[FIRST_WORKFLOW].stats == "pushed-stats"
    assert job.workflow_results[SECOND_WORKFLOW].stats == "merged:core-c+core-d"
    assert [push.workflow_id for push in reported] == [SECOND_WORKFLOW]
    assert state._job_results_events[JOB_ID].is_set()
    assert job.missing_workflow_results == []


@pytest.mark.asyncio
async def test_a_global_result_fills_in_lost_workflow_results(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(global_job_result_module, "Results", MergingResults)
    state, tracker, workflow_results, logger = make_client(NEVER_REACHED_DRAIN_SECONDS)
    await workflow_results.apply(pushed_result(FIRST_WORKFLOW, "pushed-stats"))
    datacenter_results = [
        JobFinalResult(
            job_id=JOB_ID,
            datacenter=datacenter,
            status="completed",
            workflow_results=[
                WorkflowResult(
                    workflow_id=FIRST_WORKFLOW,
                    workflow_name="WorkflowFirst",
                    status="completed",
                    results=[f"{datacenter}-first"],
                ),
                WorkflowResult(
                    workflow_id=SECOND_WORKFLOW,
                    workflow_name="WorkflowSecond",
                    status=status,
                    results=[f"{datacenter}-core-a", f"{datacenter}-core-b"],
                    error=error,
                ),
            ],
        )
        for datacenter, status, error in (("dc-east", "completed", None), ("dc-west", "failed", "worker lost"))
    ]
    global_result = GlobalJobResult(job_id=JOB_ID, status="completed", per_datacenter_results=datacenter_results)

    response = await GlobalJobResultHandler(state, logger, workflow_results).handle(
        CLIENT_CALLBACK, global_result.dump(), 0
    )
    job = await tracker.wait_for_job(JOB_ID)

    assert response == b"ok"
    assert job.workflow_results[FIRST_WORKFLOW].stats == "pushed-stats"
    rebuilt = job.workflow_results[SECOND_WORKFLOW]
    assert rebuilt.stats == "merged:dc-east-core-a+dc-east-core-b+dc-west-core-a+dc-west-core-b"
    assert (rebuilt.status, rebuilt.error) == ("failed", "dc-west: worker lost")
    assert [(result.datacenter, result.stats) for result in rebuilt.per_dc_results] == [
        ("dc-east", "merged:dc-east-core-a+dc-east-core-b"),
        ("dc-west", "merged:dc-west-core-a+dc-west-core-b"),
    ]
    assert job.missing_workflow_results == []


def make_completing_manager(origin_gate: tuple[str, int] | None) -> tuple[ManagerServer, AsyncMock]:
    send_to_client = AsyncMock(return_value=b"ok")
    manager = object.__new__(ManagerServer)
    manager._manager_state = SimpleNamespace(
        get_job_callback=lambda job_id: CLIENT_CALLBACK,
        get_client_callback=lambda job_id: None,
        get_job_origin_gate=lambda job_id: origin_gate,
    )
    manager._node_id = SimpleNamespace(datacenter="dc-1", short="manager-1")
    manager._config = SimpleNamespace(tcp_timeout_standard_seconds=5.0)
    manager._leases = SimpleNamespace(get_fence_token=lambda job_id: 3)
    manager._data_plane_provenance = lambda job_id: {}
    manager._send_to_client = send_to_client
    manager._udp_logger = RecordingLogger()
    return manager, send_to_client


async def send_final_result(manager: ManagerServer) -> None:
    await manager._send_final_result_to_client(
        JOB_ID,
        "completed",
        [WorkflowResult(workflow_id=FIRST_WORKFLOW, workflow_name="WorkflowFirst", status="completed")],
        [],
        10,
        0,
        1.5,
    )


@pytest.mark.asyncio
async def test_the_manager_sends_a_direct_jobs_final_result_to_its_client() -> None:
    manager, send_to_client = make_completing_manager(origin_gate=None)

    await send_final_result(manager)

    destination, action, payload = send_to_client.await_args.args
    final_result = JobFinalResult.load(payload)
    assert (destination, action) == (CLIENT_CALLBACK, "receive_job_final_result")
    assert [result.workflow_id for result in final_result.workflow_results] == [FIRST_WORKFLOW]
    assert final_result.fence_token == 3


@pytest.mark.asyncio
async def test_the_manager_leaves_a_gate_routed_jobs_final_result_to_the_gate() -> None:
    manager, send_to_client = make_completing_manager(origin_gate=("10.0.0.9", 9100))

    await send_final_result(manager)

    send_to_client.assert_not_awaited()
