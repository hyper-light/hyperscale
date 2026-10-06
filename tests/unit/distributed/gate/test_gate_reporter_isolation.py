"""
A job's results reporters are isolated from the job and from each other.

When a gate finalizes a job it hands the results to every reporter the
client configured (``_dispatch_to_reporters``). A reporter is a third
party -- a database, a metrics sink -- that can fail, stall, or never
answer. The gate:

* never holds a job's completion on its reporters: dispatch schedules one
  run per reporter and returns;
* lets each reporter fail or hang alone: healthy reporters deliver even
  while others hang, before any hung one's deadline;
* bounds every submission by ``REPORTER_SUBMISSION_TIMEOUT_SECONDS`` --
  connect and submit under one deadline, close under another -- so a hung
  reporter's run ends (no task outlives it) and a connected reporter is
  closed even when its submission hung;
* logs every failure and timeout against the job it belongs to.

Found by this test: submissions had no deadline (a hung reporter's run
and connection lived for the gate's lifetime), connected reporters were
never closed, and the per-reporter coroutine was a closure over the job
id -- the task runner keeps the first callable registered under a name,
so every later job's submissions were logged as the first job's.

Driven through the real ``_dispatch_to_reporters`` over a real
GateRuntimeState, TaskRunner, RealClock and Reporter facade, with
fault-injecting reporter backends registered for the configured types.
"""

import asyncio
import random
from dataclasses import dataclass, field
from types import SimpleNamespace

import cloudpickle
import pytest

from hyperscale.distributed.env import Env
from hyperscale.distributed.models import GlobalJobResult
from hyperscale.distributed.nodes.gate.server import GateServer
from hyperscale.distributed.nodes.gate.state import GateRuntimeState
from hyperscale.distributed.runtime import RealClock
from hyperscale.distributed.taskex import TaskRunner
from hyperscale.reporting.common import ReporterTypes
from hyperscale.reporting.json import JSONConfig
from hyperscale.reporting.reporter import Reporter

# Short enough to keep the suite fast; every bound below derives from it.
SUBMISSION_TIMEOUT_SECONDS = 0.2
BEHAVIORS = (
    "healthy",
    "slow",
    "raise_on_construct",
    "raise_on_connect",
    "raise_on_submit",
    "raise_on_close",
    "hang_on_connect",
    "hang_on_submit",
    "hang_on_close",
)
FAILING = {behavior for behavior in BEHAVIORS if behavior not in ("healthy", "slow")}


@dataclass
class BackendJournal:
    """What every fault-injecting backend saw, keyed by its reporter label."""

    events: dict[str, list[str]] = field(default_factory=dict)
    submissions: dict[str, list[list[dict]]] = field(default_factory=dict)
    delivered: dict[str, asyncio.Event] = field(default_factory=dict)

    def record(self, label: str, event: str) -> None:
        self.events.setdefault(label, []).append(event)


class FaultInjectingBackend:
    """A reporter backend behaving as its config's filepath says:
    ``<behavior>:<label>``."""

    def __init__(self, config: JSONConfig, journal: BackendJournal) -> None:
        self.behavior, self.label = config.workflow_results_filepath.split(":", 1)
        self.journal = journal
        self.metadata_string: str | None = None
        if self.behavior == "raise_on_construct":
            raise RuntimeError(f"{self.label} cannot be built")

    async def _step(self, step: str) -> None:
        self.journal.record(self.label, step)
        if self.behavior == f"raise_on_{step}":
            raise ConnectionError(f"{self.label} failed to {step}")
        if self.behavior == f"hang_on_{step}":
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                self.journal.record(self.label, f"{step}_cancelled")
                raise
        if self.behavior == "slow":
            # Well inside the deadline: a slow reporter still delivers.
            await asyncio.sleep(SUBMISSION_TIMEOUT_SECONDS / 10.0)

    async def connect(self) -> None:
        await self._step("connect")

    async def submit_workflow_results(self, workflow_results: list[dict]) -> None:
        await self._step("submit")
        self.journal.submissions.setdefault(self.label, []).append(workflow_results)

    async def close(self) -> None:
        await self._step("close")
        if self.behavior in ("healthy", "slow"):
            self.journal.delivered.setdefault(self.label, asyncio.Event()).set()


class RecordingLogger:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str]] = []

    async def log(self, entry: object) -> None:
        self.messages.append((type(entry).__name__, entry.message))


def make_gate(task_runner: TaskRunner, logger: RecordingLogger) -> GateServer:
    gate = object.__new__(GateServer)
    gate._modular_state = GateRuntimeState(forward_throughput_interval_start=0.0)
    gate._task_runner = task_runner
    gate._clock = RealClock()
    gate._udp_logger = logger
    gate._host = "127.0.0.1"
    gate._tcp_port = 9100
    gate._node_id = SimpleNamespace(short="gate-1")
    gate._reporter_submission_timeout_seconds = Env(
        REPORTER_SUBMISSION_TIMEOUT_SECONDS=SUBMISSION_TIMEOUT_SECONDS
    ).REPORTER_SUBMISSION_TIMEOUT_SECONDS
    return gate


def submit_job(gate: GateServer, job_id: str, reporters: list[tuple[str, str]]) -> None:
    gate._modular_state._job_submissions[job_id] = SimpleNamespace(
        job_id=job_id,
        reporting_configs=cloudpickle.dumps(
            [JSONConfig(workflow_results_filepath=f"{behavior}:{label}") for behavior, label in reporters]
        ),
    )


def job_result(job_id: str, completed: int) -> GlobalJobResult:
    return GlobalJobResult(
        job_id=job_id,
        status="COMPLETED",
        total_completed=completed,
        total_failed=1,
        successful_datacenters=2,
        elapsed_seconds=4.0,
    )


@pytest.fixture
def journal(monkeypatch: pytest.MonkeyPatch) -> BackendJournal:
    journal = BackendJournal()
    monkeypatch.setitem(
        Reporter.reporters,
        ReporterTypes.JSON,
        lambda config: FaultInjectingBackend(config, journal),
    )
    return journal


async def run_scenario(
    journal: BackendJournal,
    jobs: dict[str, list[tuple[str, str]]],
) -> tuple[RecordingLogger, float]:
    """Dispatch every job's reporters, then wait for every run to end.
    Returns the logger and the seconds dispatch took."""
    task_runner = TaskRunner(0, Env())
    logger = RecordingLogger()
    gate = make_gate(task_runner, logger)
    loop = asyncio.get_running_loop()
    tasks_before = asyncio.all_tasks()
    for job_id, reporters in jobs.items():
        submit_job(gate, job_id, reporters)
        for behavior, label in reporters:
            if behavior in ("healthy", "slow"):
                journal.delivered.setdefault(label, asyncio.Event())

    dispatch_started = loop.time()
    for job_id in jobs:
        await GateServer._dispatch_to_reporters(gate, job_id, job_result(job_id, completed=len(job_id)))
    dispatch_seconds = loop.time() - dispatch_started

    # Healthy reporters deliver before any hung one's deadline passes.
    await asyncio.wait_for(
        asyncio.gather(*(event.wait() for event in journal.delivered.values())),
        timeout=SUBMISSION_TIMEOUT_SECONDS,
    )

    runs = [
        run
        for task in task_runner.tasks.values()
        for run in task._runs.values()
    ]
    assert len(runs) == sum(len(reporters) for reporters in jobs.values())
    # Each run ends by its two deadlines (submission, then close); the
    # extra one only guards the harness.
    _, still_running = await asyncio.wait(
        [run._task for run in runs], timeout=3.0 * SUBMISSION_TIMEOUT_SECONDS
    )
    assert not still_running, "a reporter submission outlived its deadline"

    await task_runner.shutdown()
    leaked = asyncio.all_tasks() - tasks_before
    assert not leaked, leaked
    return logger, dispatch_seconds


def assert_isolation(journal: BackendJournal, logger: RecordingLogger, jobs: dict[str, list[tuple[str, str]]]) -> None:
    warnings = [message for kind, message in logger.messages if kind == "ServerWarning"]
    debugs = [message for kind, message in logger.messages if kind == "ServerDebug"]
    for job_id, reporters in jobs.items():
        for behavior, label in reporters:
            events = journal.events.get(label, [])
            if behavior in ("healthy", "slow"):
                # Delivered exactly once, with this job's results, and closed.
                assert events == ["connect", "submit", "close"], (label, events)
                (rows,) = journal.submissions[label]
                assert {row["metric_workflow"] for row in rows} == {job_id}
                assert {row["metric_name"]: row["metric_value"] for row in rows}["total_completed"] == len(job_id)
            if behavior == "hang_on_submit":
                # Abandoned at its deadline, and the connection still closed.
                assert events == ["connect", "submit", "submit_cancelled", "close"], (label, events)
            if behavior == "hang_on_connect":
                # Abandoned at its deadline, and what it opened still closed.
                assert events == ["connect", "connect_cancelled", "close"], (label, events)
            if behavior == "hang_on_close":
                assert events[-1] == "close_cancelled", (label, events)
            if behavior == "raise_on_submit":
                assert events == ["connect", "submit", "close"], (label, events)
        failing_count = sum(behavior in FAILING for behavior, _ in reporters)
        healthy_count = len(reporters) - failing_count
        # Every outcome is logged, against its own job.
        assert sum(job_id[:8] in message for message in warnings) == failing_count, (job_id, warnings)
        assert sum(job_id[:8] in message for message in debugs) == healthy_count, (job_id, debugs)


@pytest.mark.asyncio
async def test_every_fault_stays_with_its_reporter(journal: BackendJournal) -> None:
    jobs = {"job-alpha-0001": [(behavior, f"alpha-{behavior}") for behavior in BEHAVIORS]}

    logger, dispatch_seconds = await run_scenario(journal, jobs)

    # Dispatch only schedules: it never waits on a reporter.
    assert dispatch_seconds < SUBMISSION_TIMEOUT_SECONDS
    assert_isolation(journal, logger, jobs)
    warnings = " ".join(message for kind, message in logger.messages if kind == "ServerWarning")
    assert "TimeoutError" in warnings


@pytest.mark.asyncio
async def test_each_jobs_submissions_are_logged_as_that_job(journal: BackendJournal) -> None:
    jobs = {
        "job-first-0001": [("healthy", "first-ok"), ("raise_on_submit", "first-bad")],
        "job-second-002": [("healthy", "second-ok"), ("raise_on_submit", "second-bad")],
    }

    logger, _ = await run_scenario(journal, jobs)

    assert_isolation(journal, logger, jobs)


@pytest.mark.asyncio
@pytest.mark.parametrize("seed", range(12))
async def test_seeded_reporter_faults_never_cross_reporters_or_jobs(journal: BackendJournal, seed: int) -> None:
    rng = random.Random(seed)
    jobs = {
        f"{rng.randrange(16**8):08x}-job-{seed}-{job_index}": [
            (rng.choice(BEHAVIORS), f"{seed}-{job_index}-{reporter_index}")
            for reporter_index in range(rng.randint(1, 6))
        ]
        for job_index in range(rng.randint(1, 3))
    }

    logger, dispatch_seconds = await run_scenario(journal, jobs)

    assert dispatch_seconds < SUBMISSION_TIMEOUT_SECONDS
    assert_isolation(journal, logger, jobs)
