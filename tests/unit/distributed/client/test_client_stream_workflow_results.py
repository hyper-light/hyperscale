"""
``HyperscaleClient.stream_workflow_results`` (architecture.md, client API):
each of a job's workflow results as it arrives, until the job is done.

The doc's iterator did not exist -- results reached a caller only through
the per-job callback or the final ``wait_for_job``. Now:

* results that arrived before iteration began come first, then each new
  one as it is recorded -- every result exactly once, in arrival order;
* the iteration ends when ``wait_for_job`` would return (terminal, results
  in flight waited out) and raises what it would (a timeout);
* a reader that stops early leaves nothing running behind it.

Driven through the real tracker, client state and result push handler.
"""

import asyncio

import pytest

from hyperscale.distributed.env import Env
from hyperscale.distributed.models import WorkflowResultPush
from hyperscale.distributed.models.client import ClientJobResult
from hyperscale.distributed.nodes.client.handlers.tcp_workflow_result import WorkflowResultPushHandler
from hyperscale.distributed.nodes.client.state import ClientState
from hyperscale.distributed.nodes.client.tracking import ClientJobTracker

JOB_ID = "job-1"
WORKFLOWS = ("wf-a", "wf-b", "wf-c")


class SilentLogger:
    async def log(self, entry: object) -> None:
        return None


def make_tracker() -> tuple[ClientJobTracker, WorkflowResultPushHandler, ClientState]:
    state = ClientState()
    state.initialize_job_tracking(JOB_ID, ClientJobResult(job_id=JOB_ID, status="running"))
    state._job_expected_workflows[JOB_ID] = frozenset(WORKFLOWS)
    state._job_results_events[JOB_ID] = asyncio.Event()
    tracker = ClientJobTracker(state, SilentLogger(), result_drain_timeout_seconds=Env().CLIENT_RESULT_DRAIN_TIMEOUT)
    return tracker, WorkflowResultPushHandler(state, SilentLogger()), state


def push(workflow_id: str) -> WorkflowResultPush:
    return WorkflowResultPush(
        job_id=JOB_ID, workflow_id=workflow_id, workflow_name=workflow_id, datacenter="dc-a", status="COMPLETED"
    )


def finish(state: ClientState) -> None:
    state._jobs[JOB_ID].status = "completed"
    state._job_events[JOB_ID].set()


@pytest.mark.asyncio
async def test_results_stream_once_each_in_arrival_order_and_end_with_the_job() -> None:
    tracker, handler, state = make_tracker()
    await handler.apply(push("wf-a"))  # before iteration began

    async def deliver_the_rest() -> None:
        for workflow_id in ("wf-b", "wf-a", "wf-c"):  # wf-a again: a duplicate push
            await asyncio.sleep(0)
            await handler.apply(push(workflow_id))
        finish(state)

    delivery = asyncio.ensure_future(deliver_the_rest())
    streamed = [result.workflow_id async for result in tracker.stream_workflow_results(JOB_ID)]
    await delivery

    assert streamed == ["wf-a", "wf-b", "wf-c"]
    assert state.workflow_result_streams(JOB_ID) == set()


@pytest.mark.asyncio
async def test_a_reader_that_stops_early_leaves_nothing_running() -> None:
    tracker, handler, state = make_tracker()
    await handler.apply(push("wf-a"))
    tasks_before = asyncio.all_tasks()

    stream = tracker.stream_workflow_results(JOB_ID)
    async for result in stream:
        assert result.workflow_id == "wf-a"
        break
    await stream.aclose()

    assert asyncio.all_tasks() == tasks_before
    assert state.workflow_result_streams(JOB_ID) == set()


@pytest.mark.asyncio
async def test_a_job_not_done_within_the_timeout_raises_it() -> None:
    tracker, _handler, _state = make_tracker()

    with pytest.raises(asyncio.TimeoutError):
        async for _result in tracker.stream_workflow_results(JOB_ID, timeout=0.05):
            pass
