"""
A client's local results reporters never hold a job's completion hostage.

When a job's final result reaches the client (``JobFinalResultHandler``,
the gateless path; the gate path's ``GlobalJobResultHandler`` shares the
same ``WorkflowResultPushHandler.apply``), each workflow's result is
recorded and handed to the local file reporters before the job is marked
complete -- so the files exist when ``wait_for_job`` returns. A reporter
that fails is logged and the next one still writes. A reporter that hangs
(a stalled disk, a wedged network mount) held the handler forever: the
job never completed and the handler's reply never went back.

Each reporter's submission is now bounded by
``REPORTER_SUBMISSION_TIMEOUT_SECONDS`` (connect and submit under one
deadline, close under another), so:

* the job completes within a bound derived from the timeout and the hung
  reporters' count, whatever the reporters do;
* every healthy reporter -- including those after a failing or hung one --
  receives every workflow's results, and is closed;
* a hung reporter is abandoned at its deadline and still closed, and no
  task outlives the handler.

Driven through the real JobFinalResultHandler, WorkflowResultPushHandler,
ClientReportingManager, ClientJobTracker, RealClock and Reporter facade,
with fault-injecting backends registered for the JSON reporter type.
"""

import asyncio
import random

import pytest

from hyperscale.distributed.env import Env
from hyperscale.distributed.models import JobFinalResult, WorkflowResult
from hyperscale.distributed.nodes.client.models.client_config import ClientConfig
from hyperscale.distributed.nodes.client.handlers.tcp_job_result import JobFinalResultHandler
from hyperscale.distributed.nodes.client.handlers.tcp_workflow_result import WorkflowResultPushHandler
from hyperscale.distributed.nodes.client.reporting import ClientReportingManager
from hyperscale.distributed.nodes.client.state import ClientState
from hyperscale.distributed.nodes.client.tracking import ClientJobTracker
from hyperscale.distributed.runtime import RealClock
from hyperscale.reporting.common import ReporterTypes
from hyperscale.reporting.json import JSONConfig
from hyperscale.reporting.reporter import Reporter

JOB_ID = "job-client-1"
SUBMISSION_TIMEOUT_SECONDS = 0.1
BEHAVIORS = (
    "healthy",
    "raise_on_connect",
    "raise_on_submit",
    "raise_on_close",
    "hang_on_connect",
    "hang_on_submit",
    "hang_on_close",
)
HANGING = {"hang_on_connect", "hang_on_submit", "hang_on_close"}


class FaultInjectingBackend:
    """A file reporter backend behaving as its config's filepath says:
    ``<behavior>:<label>``; every step lands in the shared journal."""

    def __init__(self, config: JSONConfig, journal: dict[str, list[str]]) -> None:
        self.behavior, self.label = config.workflow_results_filepath.split(":", 1)
        self.journal = journal
        self.metadata_string: str | None = None

    async def _step(self, step: str, detail: str = "") -> None:
        self.journal.setdefault(self.label, []).append(f"{step}{detail}")
        if self.behavior == f"raise_on_{step}":
            raise OSError(f"{self.label} failed to {step}")
        if self.behavior == f"hang_on_{step}":
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                self.journal[self.label].append(f"{step}_cancelled")
                raise

    async def connect(self) -> None:
        await self._step("connect")

    async def submit_workflow_results(self, workflow_results: list[dict]) -> None:
        await self._step("submit", f":{workflow_results[0]['metric_workflow']}")

    async def submit_step_results(self, step_results: list[dict]) -> None:
        self.journal[self.label].append("step_results")

    async def close(self) -> None:
        await self._step("close")


class RecordingLogger:
    def __init__(self) -> None:
        self.entries: list[object] = []

    async def log(self, entry: object) -> None:
        self.entries.append(entry)


def workflow_stats(workflow_name: str) -> dict:
    return {
        "workflow": workflow_name,
        "workflow_name": workflow_name,
        "stats": {"succeeded": 10, "failed": 0},
        "aps": 2.5,
        "elapsed": 4.0,
        "results": [],
    }


async def complete_job(behaviors: list[str], workflow_names: list[str]):
    """Deliver the job's final result; returns the handler's reply, the
    tracker's job and the logger."""
    env = Env(REPORTER_SUBMISSION_TIMEOUT_SECONDS=SUBMISSION_TIMEOUT_SECONDS)
    config = ClientConfig.from_env(env, host="127.0.0.1", tcp_port=8500, managers=[], gates=[])
    state = ClientState()
    logger = RecordingLogger()
    tracker = ClientJobTracker(state, logger, result_drain_timeout_seconds=env.CLIENT_RESULT_DRAIN_TIMEOUT)
    tracker.initialize_job_tracking(JOB_ID, expected_workflow_ids=frozenset(workflow_names))
    state._job_reporting_configs[JOB_ID] = [
        JSONConfig(workflow_results_filepath=f"{behavior}:{index}-{behavior}")
        for index, behavior in enumerate(behaviors)
    ]
    workflow_results = WorkflowResultPushHandler(
        state, logger, reporting_manager=ClientReportingManager(state, config, logger, RealClock())
    )
    final_result = JobFinalResult(
        job_id=JOB_ID,
        datacenter="dc-1",
        status="completed",
        workflow_results=[
            WorkflowResult(
                workflow_id=f"wf-{workflow_name}",
                workflow_name=workflow_name,
                status="completed",
                results=[workflow_stats(workflow_name)],
            )
            for workflow_name in workflow_names
        ],
    )
    # Each hung reporter costs at most its submission deadline and its
    # close deadline, per workflow; the rest of the work is one more
    # deadline's worth at most.
    hung_count = sum(behavior in HANGING for behavior in behaviors)
    completion_bound = len(workflow_names) * (2 * hung_count + 1) * SUBMISSION_TIMEOUT_SECONDS
    tasks_before = asyncio.all_tasks()
    reply = await asyncio.wait_for(
        JobFinalResultHandler(state, logger, workflow_results).handle(("10.0.0.1", 9000), final_result.dump(), 0),
        timeout=completion_bound,
    )
    job = await asyncio.wait_for(tracker.wait_for_job(JOB_ID), timeout=completion_bound)
    assert asyncio.all_tasks() == tasks_before, "a reporter submission outlived the handler"
    return reply, job, logger


@pytest.fixture
def journal(monkeypatch: pytest.MonkeyPatch) -> dict[str, list[str]]:
    journal: dict[str, list[str]] = {}
    monkeypatch.setitem(Reporter.reporters, ReporterTypes.JSON, lambda config: FaultInjectingBackend(config, journal))
    return journal


def assert_reporters_isolated(
    journal: dict[str, list[str]],
    logger: RecordingLogger,
    behaviors: list[str],
    workflow_names: list[str],
) -> None:
    for index, behavior in enumerate(behaviors):
        events = journal[f"{index}-{behavior}"]
        if behavior == "healthy":
            assert events == [
                event
                for workflow_name in workflow_names
                for event in ("connect", f"submit:{workflow_name}", "step_results", "close")
            ], events
        if behavior == "hang_on_submit":
            assert events == [
                event
                for workflow_name in workflow_names
                for event in ("connect", f"submit:{workflow_name}", "submit_cancelled", "close")
            ], events
        if behavior in ("hang_on_connect", "hang_on_close"):
            assert events.count(f"{behavior.removeprefix('hang_on_')}_cancelled") == len(workflow_names), events
        if behavior == "hang_on_connect":
            # Abandoned at its deadline, and what it opened still closed.
            assert events.count("close") == len(workflow_names), events
    failing_count = sum(behavior != "healthy" for behavior in behaviors)
    warnings = [entry.message for entry in logger.entries if "Reporter submission failed" in entry.message]
    assert len(warnings) == failing_count * len(workflow_names), warnings


@pytest.mark.asyncio
async def test_a_hung_reporter_delays_completion_by_its_deadline_not_forever(journal: dict[str, list[str]]) -> None:
    behaviors = ["hang_on_submit", "healthy", "hang_on_connect", "raise_on_submit", "healthy", "hang_on_close"]
    workflow_names = ["WorkflowAlpha", "WorkflowBeta"]

    reply, job, logger = await complete_job(behaviors, workflow_names)

    assert reply == b"ok"
    assert job.status == "completed"
    assert {result.workflow_name for result in job.workflow_results.values()} == set(workflow_names)
    assert_reporters_isolated(journal, logger, behaviors, workflow_names)
    assert any("TimeoutError" in entry.message for entry in logger.entries)


@pytest.mark.asyncio
@pytest.mark.parametrize("seed", range(10))
async def test_seeded_reporter_faults_never_block_completion_or_other_reporters(
    journal: dict[str, list[str]], seed: int
) -> None:
    rng = random.Random(seed)
    behaviors = [rng.choice(BEHAVIORS) for _ in range(rng.randint(1, 5))]
    workflow_names = [f"Workflow{seed}x{index}" for index in range(rng.randint(1, 3))]

    reply, job, logger = await complete_job(behaviors, workflow_names)

    assert reply == b"ok"
    assert job.status == "completed"
    assert_reporters_isolated(journal, logger, behaviors, workflow_names)
