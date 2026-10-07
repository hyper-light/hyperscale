import asyncio
import operator
from typing import BinaryIO

from hyperscale.core.graph import Workflow
from hyperscale.distributed.runtime import Clock
from hyperscale.distributed.taskex import TaskRunner
from hyperscale.distributed.taskex.run import Run
from hyperscale.reporting.common.results_types import WorkflowStats
from hyperscale.ui.ci_safe.summary_line_output import SUMMARY_ENCODING, format_duration, write_summary_line
from hyperscale.ui.components.terminal import Terminal

from .run_summary_stopped import RunSummaryStopped
from .run_summary_text import results_text
from .run_workflow_progress import ProgressUpdate, RunWorkflowProgress

# The channel the run's own step (before any workflow runs) arrives on.
INITIALIZING_CHANNEL = "update_run_message_initializing"
# The run's step before anything has been published.
INITIAL_STEP = "Initializing..."


class RunSummaryLines:
    """The CI-safe form of ``hyperscale run workflow``'s progress UI:
    append-only lines of plain ASCII -- no cursor movement, screen clears,
    color or Unicode -- for a CI job's log, a pipe or a container's output.

    It reads the run UI's actions (``hyperscale.ui.actions``) as the full
    UI's components do, rendering nothing. ``start`` writes a line naming
    the test file and each workflow's VUs and duration; then, once every
    ``update_interval_seconds`` (the full UI's update interval), a line
    with the run's elapsed time and each running workflow's step, total
    actions and actions per second -- when those have changed, and again
    after a heartbeat without a change: the time the full UI takes to show
    each of the run's workflows once (one update interval apiece).
    ``stop`` writes the final summary -- from the workflows' final
    results, the ones their reporters were given -- where results were
    written and the outcome.

    Each line is written off the event loop and awaited. Once a write
    fails (a reader gone away: EPIPE) the output is closed for good and
    the run goes on; ``stop`` returns the error for its owner to report.
    The owner calls ``start`` once and ``stop`` on every exit path: it
    cancels the progress loop, shuts the task runner down (it owns it)
    and unsubscribes every update.
    """

    def __init__(
        self,
        output: BinaryIO,
        workflows: list[Workflow],
        clock: Clock,
        task_runner: TaskRunner,
        update_interval_seconds: float,
    ) -> None:
        self._output = output
        self._workflows = workflows
        self._clock = clock
        self._task_runner = task_runner
        self._update_interval_seconds = update_interval_seconds
        self._heartbeat_interval_seconds = update_interval_seconds * len(workflows)
        self._workflow_progress = [RunWorkflowProgress(workflow.name, clock) for workflow in workflows]
        self._subscriptions: dict[str, ProgressUpdate] = {INITIALIZING_CHANNEL: self._update_initializing_step}
        for workflow, progress in zip(workflows, self._workflow_progress):
            self._subscriptions.update(progress.subscriptions(workflow.name.lower()))

        self._initializing_step = INITIAL_STEP
        self._started_at = 0.0
        self._last_summary: str | None = None
        self._last_written_at = 0.0
        self._write_error: OSError | ValueError | None = None
        self._progress_run: Run | None = None

    async def start(self, start_text: str) -> None:
        """Subscribe to the run's updates, write ``start_text`` and start
        writing progress."""
        for channel, update in self._subscriptions.items():
            Terminal.subscribe(channel, update)

        self._started_at = self._clock.monotonic()
        self._last_written_at = self._started_at
        await self._write_line(f"{format_duration(0)} | {start_text}")
        self._progress_run = self._task_runner.run(self._write_progress_until_cancelled)

    async def stop(
        self,
        outcome_text: str,
        final_workflow_stats: dict[str, WorkflowStats],
    ) -> OSError | ValueError | None:
        """Stop writing progress, unsubscribe, then write the final summary
        from ``final_workflow_stats`` (each workflow's final results, by
        name), where results were written and ``outcome_text``; the error a write
        failed with (nothing more was written after it), else None. Raises
        ``RunSummaryStopped`` if the progress loop ended on an error of its
        own."""
        await self._task_runner.cancel(self._progress_run.token)
        await self._task_runner.shutdown()
        Terminal.unsubscribe(list(self._subscriptions.values()))
        final_summary = " | ".join(
            progress.final_text(final_workflow_stats.get(workflow.name))
            for workflow, progress in zip(self._workflows, self._workflow_progress)
        )
        await self._write_line(
            f"{self._elapsed_text()} | run end | {final_summary} | {results_text(self._workflows)}"
        )
        await self._write_line(outcome_text)
        self._raise_if_progress_failed()
        return self._write_error

    async def _update_initializing_step(self, step: str) -> None:
        self._initializing_step = step

    async def _write_progress_until_cancelled(self) -> None:
        while True:
            await self._clock.sleep(self._update_interval_seconds)
            await self._write_progress_if_due()

    async def _write_progress_if_due(self) -> None:
        summary = self._progress_summary()
        heartbeat_due = self._clock.monotonic() - self._last_written_at >= self._heartbeat_interval_seconds
        if summary == self._last_summary and not heartbeat_due:
            return

        await self._write_line(f"{self._elapsed_text()} | {summary}")
        self._last_summary = summary
        self._last_written_at = self._clock.monotonic()

    def _progress_summary(self) -> str:
        """Each started workflow's progress, or the run's own step until a
        workflow starts."""
        started_progress = filter(operator.attrgetter("has_started"), self._workflow_progress)
        return " | ".join(map(RunWorkflowProgress.progress_text, started_progress)) or (
            f"step {self._initializing_step}"
        )

    def _elapsed_text(self) -> str:
        return format_duration(self._clock.monotonic() - self._started_at)

    async def _write_line(self, line: str) -> None:
        if self._write_error is not None:
            return

        line_bytes = f"{line}\n".encode(SUMMARY_ENCODING, errors="replace")
        try:
            await asyncio.get_running_loop().run_in_executor(None, write_summary_line, self._output, line_bytes)

        except (OSError, ValueError) as write_error:
            self._write_error = write_error

    def _raise_if_progress_failed(self) -> None:
        # The run's error outlives its cancellation (which resets its status).
        if self._progress_run.error is not None:
            raise RunSummaryStopped(
                f"the run summary stopped writing progress: {self._progress_run.error}\n{self._progress_run.trace}"
            )
