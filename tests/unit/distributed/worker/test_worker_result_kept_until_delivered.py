"""
A worker keeps a final result it could not deliver until it can (AD-52
section 10) -- not for a fixed age.

Results were dropped 300s after they were queued, reachable manager or not:
a worker isolated from every manager for longer lost the job's output on
its first retry pass after reconnecting. The retry loop runs only while a
manager is reachable, so the attempt budget (and the pending-result cap)
bound a kept result; age alone never drops it. Driven through the real
progress reporter on a stepped clock.
"""

from types import SimpleNamespace

import pytest

from hyperscale.distributed.env import Env
from hyperscale.distributed.models import WorkflowFinalResult
from hyperscale.distributed.nodes.worker import worker_progress_reporter
from hyperscale.distributed.nodes.worker.models.worker_config import WorkerConfig
from hyperscale.distributed.nodes.worker.progress import WorkerProgressReporter

SETTINGS = Env()
# Longer than any age a result was ever kept for.
ISOLATION_SECONDS = 3600.0


class SteppedClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def monotonic(self) -> float:
        return self.now


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> SteppedClock:
    stepped_clock = SteppedClock()
    monkeypatch.setattr(worker_progress_reporter, "_DEFAULT_CLOCK", stepped_clock)
    return stepped_clock


def reporter() -> WorkerProgressReporter:
    config = WorkerConfig.from_env(SETTINGS, host="127.0.0.1", tcp_port=9000, udp_port=9001)
    return WorkerProgressReporter(registry=SimpleNamespace(), state=SimpleNamespace(), config=config)


def final_result() -> WorkflowFinalResult:
    return WorkflowFinalResult(
        job_id="job-1",
        workflow_id="workflow-1",
        workflow_name="Workflow",
        status="completed",
        results=[],
        context_updates=b"",
    )


async def retry(progress_reporter: WorkerProgressReporter, deliverable: bool) -> list[str]:
    """One retry pass: the workflow ids it tried to send."""
    attempted: list[str] = []

    async def send(final_result, *args, **kwargs) -> tuple[bool, float | None]:
        attempted.append(final_result.workflow_id)
        # Delivered, or failed -- never refused under AD-24.
        return deliverable, None

    progress_reporter._try_send_pending_result = send
    await progress_reporter.retry_pending_results(
        None, "127.0.0.1", 9000, "worker-1", lambda *args, **kwargs: None
    )
    return attempted


@pytest.mark.asyncio
async def test_a_result_queued_through_a_long_isolation_is_delivered_after_it(clock: SteppedClock) -> None:
    progress_reporter = reporter()
    progress_reporter._enqueue_pending_result(final_result())

    # No manager was reachable for an hour: the retry loop never ran. The
    # first pass after reconnecting delivers it.
    clock.now += ISOLATION_SECONDS
    attempted = await retry(progress_reporter, deliverable=True)

    assert attempted == ["workflow-1"]
    assert len(progress_reporter._pending_results) == 0


@pytest.mark.asyncio
async def test_a_result_refused_every_attempt_is_dropped_by_the_attempt_budget(clock: SteppedClock) -> None:
    progress_reporter = reporter()
    progress_reporter._enqueue_pending_result(final_result())

    passes = 0
    while progress_reporter._pending_results:
        clock.now += ISOLATION_SECONDS
        await retry(progress_reporter, deliverable=False)
        passes += 1

    # One attempt per pass until the budget is spent, then one pass that
    # drops it.
    assert passes == SETTINGS.WORKER_RESULT_MAX_RETRIES + 1
