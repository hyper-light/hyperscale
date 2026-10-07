import asyncio
import math
from typing import BinaryIO

from hyperscale.ui.ci_safe.summary_line_output import SUMMARY_ENCODING, format_duration, write_summary_line

from .models import NodeDashboardFrame


def summary_text(frame: NodeDashboardFrame, readings: list[str]) -> str:
    """A frame's summary, all but its uptime: who the node is and its
    lifecycle state, then each panel's lines and the readings."""
    return " | ".join(
        "; ".join(lines)
        for lines in (
            [*frame.identity_lines, frame.lifecycle_state],
            frame.cluster_lines,
            frame.summary_lines,
            frame.detail_lines,
            readings,
        )
    )


class NodeDashboardSummaryLines:
    """The CI-safe form of a node dashboard: append-only lines of plain
    ASCII -- no cursor movement, screen clears, color or Unicode -- one per
    summary of the node, for a log collector, a CI job's log or a
    container's output.

    A line carries the node's identity and lifecycle state, its panels'
    values and every chart's current reading, prefixed by its uptime. A
    line is written when the summary (all but the uptime, which changes
    every sample) has changed, at most once every
    ``change_interval_seconds``, and repeated after
    ``heartbeat_interval_seconds`` without a change, so a quiet node still
    shows it is alive. Intervals are on the node's clock.

    Each line is written off the event loop and awaited: a reader that
    stalls holds the sampling back, never the node, and nothing queues up.
    Once a write fails the output is closed for good: the error is returned
    once for the dashboard to log, and nothing more is written.
    """

    def __init__(
        self,
        output: BinaryIO,
        change_interval_seconds: float,
        heartbeat_interval_seconds: float,
    ) -> None:
        self._output = output
        self._change_interval_seconds = change_interval_seconds
        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._last_summary: str | None = None
        # No line written yet: the first is due at once.
        self._last_written_at = -math.inf

    async def write(self, frame: NodeDashboardFrame, readings: list[str]) -> OSError | ValueError | None:
        """Write ``frame``'s summary line if it is due; the error a failed
        write raised (the output is then closed), else None."""
        summary = summary_text(frame, readings)
        if not self._is_due(summary, frame.sampled_at):
            return None

        line = f"up {format_duration(frame.uptime_seconds)} | {summary}\n".encode(SUMMARY_ENCODING, errors="replace")
        try:
            await asyncio.get_running_loop().run_in_executor(None, write_summary_line, self._output, line)

        except (OSError, ValueError) as write_error:
            # The output is closed for good: placing the last write at the
            # end of time leaves no line ever due again.
            self._last_written_at = math.inf
            return write_error

        self._last_summary = summary
        self._last_written_at = frame.sampled_at
        return None

    def _is_due(self, summary: str, sampled_at: float) -> bool:
        elapsed_seconds = sampled_at - self._last_written_at
        return elapsed_seconds >= self._heartbeat_interval_seconds or (
            summary != self._last_summary and elapsed_seconds >= self._change_interval_seconds
        )
