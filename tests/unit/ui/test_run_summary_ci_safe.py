"""
`hyperscale run workflow` configured for the full UI detects at runtime
whether its stdout can show it, with the node dashboards' checks, and
where it cannot writes the CI-safe form instead: append-only plain ASCII
lines.

- The run UI's own layout judges the terminal: too small, or an encoding
  that cannot write its glyphs, selects "ci-safe"; --quiet and an
  explicitly configured mode win; off a terminal "full" falls back.
- The summary reads the run UI's actions: a start line, progress lines
  written when the values change and repeated after the heartbeat (the
  full UI's update interval per workflow), then the final summary -- from
  the workflows' final results, never the streamed values -- where results
  were written and the outcome, all ASCII.
- A reader that goes away (EPIPE) never stops the run: the error is
  returned once, and nothing more is written.
- Stopped, the summary leaves no subscription and no task behind.
"""

import asyncio
import io
import os
import pathlib

from hyperscale.graph import Workflow, step
from hyperscale.reporting.json import JSONConfig
from hyperscale.distributed.taskex import TaskRunner
from hyperscale.ui.actions import (
    update_active_workflow_message,
    update_workflow_execution_stats,
    update_workflow_executions_counter,
    update_workflow_executions_final_rate,
    update_workflow_run_timer,
)
from hyperscale.ui.components.terminal import Terminal
from hyperscale.ui.run_summary import RunSummaryLines
from hyperscale.ui.run_summary.run_terminal_capability import run_layout_fallback, select_run_terminal_mode

UPDATE_INTERVAL_SECONDS = 5.0
ESCAPE = b"\x1b"
RESULTS_DIRECTORY = "/results"
# How long a test waits for the summary's loop to write a line it is due.
LINE_WAIT_SECONDS = 5.0


class SummaryTarget(Workflow):
    vus = 8
    duration = "30s"
    reporting = JSONConfig(
        workflow_results_filepath=f"{RESULTS_DIRECTORY}/workflow_results.json",
        step_results_filepath=f"{RESULTS_DIRECTORY}/step_results.json",
    )

    @step()
    async def get_target(self) -> int:
        return 1


class SteppedClock:
    """A clock the test moves: ``sleep`` returns when the test calls
    ``advance``, which moves the time on by the sleep's seconds."""

    def __init__(self) -> None:
        self.now = 0.0
        self._wakeups: asyncio.Queue[None] = asyncio.Queue()

    def monotonic(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        await self._wakeups.get()
        self.now += seconds

    def advance(self) -> None:
        self._wakeups.put_nowait(None)


def written_lines(output: io.BytesIO) -> list[str]:
    return output.getvalue().decode("ascii").splitlines()


async def wait_for_line_count(output: io.BytesIO, count: int) -> list[str]:
    """The lines once there are ``count`` of them (or the wait ran out)."""
    async with asyncio.timeout(LINE_WAIT_SECONDS):
        while len(written_lines(output)) < count:
            await asyncio.sleep(0.01)

    return written_lines(output)


def subscribed_updates() -> int:
    return sum(map(len, Terminal._updates.updates.values()))


async def test_the_run_ui_layout_decides_whether_a_terminal_can_show_it() -> None:
    workflows = [SummaryTarget()]
    assert await run_layout_fallback(workflows, 120, 38, "utf-8") is None
    assert await run_layout_fallback(workflows, 160, 48, "utf-8") is None
    for columns, lines in ((40, 10), (0, 0), (120, 14), (120, 20), (100, 30)):
        assert (await run_layout_fallback(workflows, columns, lines, "utf-8"))[0] == "ci-safe", (columns, lines)
    assert (await run_layout_fallback(workflows, 120, 38, "ascii"))[0] == "ci-safe"


async def test_quiet_and_an_explicit_mode_win_and_full_falls_back_off_a_terminal() -> None:
    # pytest's stdout is not a terminal.
    workflows = [SummaryTarget()]
    assert (await select_run_terminal_mode("full", True, workflows)).mode == "disabled"
    for configured_mode in ("ci", "ci-safe", "disabled"):
        selection = await select_run_terminal_mode(configured_mode, False, workflows)
        assert (selection.mode, selection.degraded_reason) == (configured_mode, None)
    selection = await select_run_terminal_mode("full", False, workflows)
    assert selection.mode == "ci-safe"
    assert "the full run UI cannot show here (stdout is not a terminal)" in selection.degraded_reason


async def test_summary_lines_follow_the_run_and_end_with_its_outcome() -> None:
    output = io.BytesIO()
    clock = SteppedClock()
    updates_before = subscribed_updates()
    summary = RunSummaryLines(output, [SummaryTarget()], clock, TaskRunner(), UPDATE_INTERVAL_SECONDS)

    await summary.start("run start | file summary_test.py")
    await update_active_workflow_message("initializing", "Starting worker servers...")
    clock.advance()
    lines = await wait_for_line_count(output, 2)
    assert lines == ["0h00m00s | run start | file summary_test.py", "0h00m05s | step Starting worker servers..."]

    # Unchanged: nothing until the heartbeat (one update interval for one
    # workflow), then the same line again.
    await update_workflow_run_timer("summarytarget", True)
    await update_active_workflow_message("summarytarget", "Running - SummaryTarget")
    await update_workflow_executions_counter("summarytarget", 50)
    clock.advance()
    lines = await wait_for_line_count(output, 3)
    assert lines[2] == "0h00m10s | SummaryTarget: Running - SummaryTarget, 50 actions, 10.0 actions/s"

    await update_workflow_run_timer("summarytarget", False)
    await update_workflow_executions_counter("summarytarget", 120)
    await update_workflow_executions_final_rate("summarytarget", 120, 12.0)
    await update_workflow_execution_stats("summarytarget", {"get_target": {"total": 120, "ok": 119, "err": 1}})
    await update_active_workflow_message("summarytarget", "Complete - SummaryTarget")
    clock.advance()
    lines = await wait_for_line_count(output, 4)
    assert lines[3] == "0h00m15s | SummaryTarget: Complete - SummaryTarget, 120 actions, 10.0 actions/s"

    # The final summary is the final results', never the streamed values
    # (120 actions, 119/1), which trail them.
    final_stats = {
        "workflow": "SummaryTarget",
        "stats": {"executed": 131, "succeeded": 129, "failed": 2},
        "elapsed": 13.1,
        "aps": 10.0,
        "results": [{"step": "get_target", "counts": {"executed": 131, "succeeded": 129, "failed": 2}}],
    }
    assert await summary.stop("default: completed", {"SummaryTarget": final_stats}) is None
    lines = written_lines(output)
    assert lines[4] == (
        "0h00m15s | run end | SummaryTarget: 131 actions in 13.1s, 10.0 actions/s, "
        "steps [get_target total 131 ok 129 err 2], Complete - SummaryTarget"
        f" | results JSON {RESULTS_DIRECTORY}/workflow_results.json {RESULTS_DIRECTORY}/step_results.json"
    )
    assert lines[5] == "default: completed"
    assert ESCAPE not in output.getvalue() and output.getvalue().isascii()
    assert subscribed_updates() == updates_before


async def test_an_unchanged_summary_waits_for_the_heartbeat() -> None:
    output = io.BytesIO()
    clock = SteppedClock()
    # Two workflows: the heartbeat is two update intervals.
    summary = RunSummaryLines(output, [SummaryTarget(), SummaryTarget()], clock, TaskRunner(), UPDATE_INTERVAL_SECONDS)
    await summary.start("run start")
    clock.advance()
    await wait_for_line_count(output, 2)
    clock.advance()
    clock.advance()
    lines = await wait_for_line_count(output, 3)
    assert lines[1:] == ["0h00m05s | step Initializing...", "0h00m15s | step Initializing..."]
    await summary.stop("default: failed", {})
    # A workflow without final results says so: no streamed value stands in.
    assert written_lines(output)[3].endswith(
        "run end | SummaryTarget: no final results, not run | SummaryTarget: no final results, not run"
        f" | results JSON {RESULTS_DIRECTORY}/workflow_results.json {RESULTS_DIRECTORY}/step_results.json"
    )


async def test_a_reader_gone_away_never_stops_the_run(tmp_path: pathlib.Path) -> None:
    read_descriptor, write_descriptor = os.pipe()
    os.close(read_descriptor)
    clock = SteppedClock()
    with open(write_descriptor, "wb", closefd=True) as output:
        summary = RunSummaryLines(output, [SummaryTarget()], clock, TaskRunner(), UPDATE_INTERVAL_SECONDS)
        await summary.start("run start")
        clock.advance()
        write_error = await summary.stop("default: completed", {})

    assert isinstance(write_error, BrokenPipeError)
