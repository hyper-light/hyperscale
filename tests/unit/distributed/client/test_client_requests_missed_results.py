"""
A client asks for the workflow results it missed.

A gate records every update it owes a job's client and replays them from
the last one that reached the client when the client's callback registers
again -- but no client ever registered again: results the gate could not
send (the client out of reach a while) were left in its history, and the
job ended with them listed missing. A client whose completed job still
lacks results once the in-flight ones had time to land now re-registers
its callback, and waits for the replay.

A real ``ClientJobTracker`` and workflow-result handler on virtual time;
the gate's replay is stood in for by delivering the missed result through
the client's own handler, as the gate's replay pushes it.
"""

import asyncio
import contextvars
from collections.abc import Callable, Coroutine
from typing import Any, TypeVar

from hyperscale.distributed.models import JobStatus, WorkflowResultPush
from hyperscale.distributed.nodes.client.handlers.tcp_workflow_result import (
    WorkflowResultPushHandler,
)
from hyperscale.distributed.nodes.client.state import ClientState
from hyperscale.distributed.nodes.client.tracking import ClientJobTracker
from hyperscale.distributed.runtime import restore_defaults, snapshot_defaults, swap_defaults
from hyperscale.logging import LoggingConfig
from tests.simulation.harness.sim import SimulationLoop, VirtualClock

JOB_ID = "job-1"
DELIVERED_WORKFLOW_ID = "wf-delivered"
MISSED_WORKFLOW_ID = "wf-missed"
RESULT_DRAIN_TIMEOUT_SECONDS = 5.0

ScenarioResult = TypeVar("ScenarioResult")


class RecordingLogger:
    def __init__(self) -> None:
        self.messages: list[str] = []

    async def log(self, model) -> None:
        self.messages.append(model.message)


def on_virtual_time(scenario: Callable[[], Coroutine[Any, Any, ScenarioResult]]) -> ScenarioResult:
    defaults = snapshot_defaults()
    loop = SimulationLoop()
    swap_defaults(clock=VirtualClock(loop))
    try:
        LoggingConfig().disable()
        return contextvars.copy_context().run(loop.run_until_complete, scenario())
    finally:
        LoggingConfig().enable()
        restore_defaults(defaults)
        loop.close()


def result_of(workflow_id: str) -> WorkflowResultPush:
    return WorkflowResultPush(
        job_id=JOB_ID,
        workflow_id=workflow_id,
        workflow_name=workflow_id,
        datacenter="aggregated",
        status=JobStatus.COMPLETED.value,
        fence_token=1,
        results=[],
        is_client_ready=True,
    )


async def completed_job_missing_a_result(replay_delivers: bool) -> tuple[list[str], list[str]]:
    state = ClientState()
    logger = RecordingLogger()
    results = WorkflowResultPushHandler(state, logger)
    replay_requests: list[str] = []

    async def request_replay(job_id: str) -> bool:
        replay_requests.append(job_id)
        if replay_delivers:
            await results.apply(result_of(MISSED_WORKFLOW_ID))
        return replay_delivers

    tracker = ClientJobTracker(
        state=state,
        logger=logger,
        result_drain_timeout_seconds=RESULT_DRAIN_TIMEOUT_SECONDS,
        request_replay=request_replay,
    )
    tracker.initialize_job_tracking(
        JOB_ID, frozenset({DELIVERED_WORKFLOW_ID, MISSED_WORKFLOW_ID})
    )
    await results.apply(result_of(DELIVERED_WORKFLOW_ID))
    state._jobs[JOB_ID].status = JobStatus.COMPLETED.value
    state._job_events[JOB_ID].set()

    job = await tracker.wait_for_job(JOB_ID)
    return job.missing_workflow_results, replay_requests


def test_a_completed_job_missing_results_has_them_replayed() -> None:
    missing, replay_requests = on_virtual_time(
        lambda: completed_job_missing_a_result(replay_delivers=True)
    )

    assert replay_requests == [JOB_ID]
    assert missing == []


def test_results_no_replay_brings_are_named_missing() -> None:
    missing, replay_requests = on_virtual_time(
        lambda: completed_job_missing_a_result(replay_delivers=False)
    )

    assert replay_requests == [JOB_ID]
    assert missing == [MISSED_WORKFLOW_ID]
