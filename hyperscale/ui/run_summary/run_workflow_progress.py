import math
from collections.abc import Awaitable, Callable

from hyperscale.distributed.runtime import Clock
from hyperscale.reporting.common.results_types import WorkflowStats

from .run_summary_text import final_results_text

RateUpdate = tuple[int | float | None, bool | None] | tuple[int | float, bool, float]
ProgressUpdate = Callable[..., Awaitable[None]]


class RunWorkflowProgress:
    """One workflow's progress in a run, as the run UI's actions publish
    it (``hyperscale.ui.actions``): its current step (the status message),
    its total actions and its clock. Its final summary is its final
    results, never these streamed values (which trail them).

    Its clock runs from the first time the run timer starts to the last
    time it stops, as the full UI's timer does; once the run's final rate
    arrives with the elapsed time its total was counted over, that elapsed
    time is the workflow's, as the full UI's rate shows it.
    """

    def __init__(self, workflow_title: str, clock: Clock) -> None:
        self._workflow_title = workflow_title
        self._clock = clock
        self._status: str | None = None
        self._executed = 0
        # Not started: no start yet (the earliest start wins) and no stop.
        self._started_at = math.inf
        self._stopped_at = math.inf
        self._final_elapsed: float | None = None

    @property
    def has_started(self) -> bool:
        """Whether the workflow has published its status yet."""
        return self._status is not None

    def subscriptions(self, workflow_slug: str) -> dict[str, ProgressUpdate]:
        """The action channel each of the workflow's values arrives on, and
        the update that records it."""
        return {
            f"update_run_message_{workflow_slug}": self.update_status,
            f"update_total_executions_{workflow_slug}": self.update_executed,
            f"update_total_executions_rate_{workflow_slug}": self.update_rate,
            f"update_run_timer_{workflow_slug}": self.update_timer,
        }

    async def update_status(self, status: str) -> None:
        self._status = status

    async def update_executed(self, executed: int) -> None:
        self._executed = executed

    async def update_rate(self, rate_update: RateUpdate) -> None:
        # Only the final rate carries a third value: the elapsed time the
        # final total was counted over.
        _, _, *final_elapsed = rate_update
        self._final_elapsed = next(iter(final_elapsed), self._final_elapsed)

    async def update_timer(self, running: bool) -> None:
        now = self._clock.monotonic()
        self._started_at = min(self._started_at, now) if running else self._started_at
        self._stopped_at = math.inf if running else now

    def elapsed_seconds(self) -> float:
        """The workflow's elapsed time: zero before it starts, its final
        elapsed time once that arrives."""
        if self._final_elapsed is not None:
            return self._final_elapsed

        return max(min(self._stopped_at, self._clock.monotonic()) - self._started_at, 0.0)

    def actions_per_second(self) -> float:
        """Total actions over the workflow's elapsed time."""
        elapsed_seconds = self.elapsed_seconds()
        return self._executed / elapsed_seconds if elapsed_seconds > 0 else 0.0

    def progress_text(self) -> str:
        """The workflow's progress line entry: its step, total actions and
        actions per second."""
        return (
            f"{self._workflow_title}: {self._status}, {self._executed} actions, "
            f"{self.actions_per_second():.1f} actions/s"
        )

    def final_text(self, final_stats: WorkflowStats | None) -> str:
        """The workflow's final summary, from ``final_stats`` -- the results
        its reporters were given -- with its last step; without them, that
        it has none."""
        last_step = self._status or "not run"
        if final_stats is None:
            return f"{self._workflow_title}: no final results, {last_step}"

        return final_results_text(self._workflow_title, final_stats, last_step)
