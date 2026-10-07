import asyncio
import functools
import os
import pathlib
import sys
from collections.abc import Awaitable, Callable

from hyperscale.core.jobs.models import TerminalMode
from hyperscale.distributed.env import Env
from hyperscale.distributed.swim.health_aware_server import HealthAwareServer
from hyperscale.distributed.taskex import TaskRunner
from hyperscale.distributed.taskex.run import Run
from hyperscale.logging import Logger
from hyperscale.logging.hyperscale_logging_models import ServerError, ServerWarning
from hyperscale.ui.components.scatter_plot import SeriesUpdate
from hyperscale.ui.components.stat_tile import StatTileReading
from hyperscale.ui.components.stat_tile.stat_tile_separators import STAT_TILE_SEPARATORS
from hyperscale.ui.components.status_badge import BADGE_GAP, StatusBadgeReading, badge_line_count
from hyperscale.ui.components.table import TableConfig
from hyperscale.ui.components.table.tabulate import TableCell
from hyperscale.ui.components.terminal import Terminal
from hyperscale.ui.ci_safe.summary_line_output import format_duration
from hyperscale.ui.config.mode import TerminalDisplayMode
from hyperscale.ui.config.mode import TerminalMode as DisplayMode

from .dashboard_formatting import format_rate, format_reading
from .models import NodeDashboardFrame, NodeDashboardLayout
from .node_dashboard_actions import (
    tile_channel,
    update_node_dashboard_badges,
    update_node_dashboard_chart,
    update_node_dashboard_identity,
    update_node_dashboard_status,
    update_node_dashboard_table,
    update_node_dashboard_tile,
)
from .node_dashboard_chart_series import NodeDashboardChartSeries
from .node_dashboard_config import NodeDashboardConfig
from .node_dashboard_reader import NodeDashboardReader
from .node_dashboard_rows import NodeDashboardRows
from .node_dashboard_sampling_stopped import NodeDashboardSamplingStopped
from .node_dashboard_sections import (
    content_width,
    generate_node_dashboard_sections,
    node_dashboard_table_config,
)
from .node_dashboard_summary_lines import NodeDashboardSummaryLines
from .node_dashboard_table_cells import cell_meter_width, table_cells

PanelContent = list[str] | list[dict[str, TableCell]] | list[StatusBadgeReading] | StatTileReading | str
PanelPublisher = Callable[[PanelContent], Awaitable[object]]

# The terminal framework's display mode for each rendering terminal mode:
# "ci" renders without the extended (color) sequences.
DISPLAY_MODES: dict[TerminalMode, TerminalDisplayMode] = {
    "full": "extended",
    "ci": "compatability",
}
# The dashboard spans the terminal's width, as btop and k9s do, less one
# column of padding on each side (the canvas never reaches the terminal's
# last column, Terminal's canvas_size), and every row but one blank row
# above and below.
WIDTH_SHARE = 1.0
HORIZONTAL_PADDING = 1
VERTICAL_PADDING = 1
# What the status line says before where the node's logs go.
STOP_HINT = "ctrl-c stops the node"
# What stands in for a log path's directories where the whole path does
# not fit the status line, by mode.
ELLIPSES: dict[DisplayMode, str] = {DisplayMode.EXTENDED: "\u2026", DisplayMode.COMPATIBILITY: "..."}


def dashboard_terminal(
    layout: NodeDashboardLayout,
    terminal_mode: TerminalMode,
    table_config: TableConfig,
    rows: NodeDashboardRows,
) -> Terminal | None:
    """The terminal a dashboard renders its frames through, in a mode
    that renders frames; None in any other."""
    if terminal_mode not in DISPLAY_MODES:
        return None

    return Terminal(
        generate_node_dashboard_sections(layout, table_config, DISPLAY_MODES[terminal_mode], rows),
        width_share=WIDTH_SHARE,
    )


def dashboard_summary_lines(
    terminal_mode: TerminalMode,
    change_interval_seconds: float,
    heartbeat_interval_seconds: float,
) -> NodeDashboardSummaryLines | None:
    """The summary lines a "ci-safe" dashboard writes to stdout; None in
    any other mode."""
    if terminal_mode != "ci-safe":
        return None

    return NodeDashboardSummaryLines(sys.stdout.buffer, change_interval_seconds, heartbeat_interval_seconds)


def status_text(log_path: pathlib.Path, width: int, separator: str, ellipsis: str) -> str:
    """The status line in ``width`` columns: how to stop the node and its
    log file's whole path, or -- where that does not fit -- the file's
    name after an ellipsis for its directories (the name is never cut)."""
    whole_text = f"{STOP_HINT}{separator}logs {log_path}"
    if len(whole_text) < width:
        return whole_text

    return f"{STOP_HINT}{separator}logs {ellipsis}{os.sep}{log_path.name}"


class NodeDashboard:
    """A live terminal dashboard for a running node.

    It renders through the hyperscale terminal framework the way `run
    workflow` does (``Terminal`` sections updated by ``@action()``
    publishers) and samples its node on its own interval, through a reader
    that reads the node's state synchronously: the dashboard never awaits
    the node or the network, so it cannot stall the node, and a reader
    that raises is logged and shown on the status line while the sampling
    goes on.

    With ``terminal_mode`` "ci-safe" it renders no frames: it appends a
    plain ASCII summary line to stdout when the node's values change
    (``NodeDashboardSummaryLines``) -- at most once per interval its table
    pages its rows in -- and again after a chart window without a change.
    With "disabled" it renders nothing and ``start`` does nothing. Its
    owner calls ``start`` once and ``stop`` on every exit path: ``stop``
    cancels and awaits the sampling loop, stops the terminal (restoring
    the cursor), releases it, and closes the logger the dashboard was
    given (it owns it). ``degraded_reason`` -- why the mode differs from
    the one configured, if it does -- is logged once at ``start``, as is
    a failure to write a summary line (after which none is written).

    Its chart plots the role's series over the last
    ``SLO_EVALUATION_WINDOW_SECONDS`` of samples -- the horizon the cluster
    judges a node's latency over (AD-42) -- one point per series per
    sample, so it holds at most that window over the sampling interval;
    each series' newest value is in the chart's legend.

    The sections take the rows a sample needs (``NodeDashboardRows``): when
    the lines the badges flow onto or the table's row count change, the
    sections are laid out again within the frame being published, and
    every section is published afresh into the new layout.
    """

    def __init__(
        self,
        reader: NodeDashboardReader,
        node: HealthAwareServer,
        terminal_mode: TerminalMode,
        env: Env,
        log_path: pathlib.Path,
        config: NodeDashboardConfig,
        logger: Logger,
        degraded_reason: str | None = None,
    ) -> None:
        self._reader = reader
        self._degraded_reason = degraded_reason
        self._node = node
        self._env = env
        self._config = config
        self._logger = logger
        self._log_path = log_path
        window_seconds = env.SLO_EVALUATION_WINDOW_SECONDS
        display_mode = DISPLAY_MODES.get(terminal_mode, DISPLAY_MODES["ci"])
        self._display_mode = DisplayMode.to_mode(display_mode)
        self._separator = STAT_TILE_SEPARATORS[self._display_mode]
        self._rows = NodeDashboardRows()
        table_config = node_dashboard_table_config(reader.layout, display_mode)
        self._terminal = dashboard_terminal(reader.layout, terminal_mode, table_config, self._rows)
        self._summary_lines = dashboard_summary_lines(
            terminal_mode, table_config.pagination_refresh_rate, window_seconds
        )
        self._sample_interval_seconds = max(
            config.sample_interval_seconds,
            self._terminal.refresh_interval if self._terminal is not None else 0.0,
        )
        self._chart_series = NodeDashboardChartSeries(window_seconds, self._sample_interval_seconds)
        self._task_runner: TaskRunner | None = None
        self._sampling_run: Run | None = None
        self._published: dict[str, PanelContent] = {}

    async def start(self) -> None:
        """Render the dashboard and start sampling the node."""
        await self._log_degraded_reason()
        if self._terminal is None and self._summary_lines is None:
            return

        self._task_runner = TaskRunner(0, self._env)
        await self._render_terminal()
        self._sampling_run = self._task_runner.run(self._sample_until_cancelled)

    async def _log_degraded_reason(self) -> None:
        if self._degraded_reason is not None:
            await self._log_warning(self._degraded_reason)

    async def _render_terminal(self) -> None:
        if self._terminal is not None:
            await self._terminal.render(
                horizontal_padding=HORIZONTAL_PADDING,
                vertical_padding=VERTICAL_PADDING,
            )

    async def stop(self) -> None:
        """Stop sampling, stop the terminal and release it. Raises
        ``NodeDashboardSamplingStopped`` if the sampling loop had ended on
        an error of its own (after the terminal is restored)."""
        if self._sampling_run is None:
            await self._logger.close()
            return

        await self._task_runner.cancel(self._sampling_run.token)
        await self._task_runner.shutdown()
        if self._terminal is not None:
            await self._terminal.stop()
            await self._terminal.close()

        await self._logger.close()
        self._raise_if_sampling_failed()

    async def _sample_until_cancelled(self) -> None:
        while True:
            await self._sample_once()
            await asyncio.sleep(self._sample_interval_seconds)

    async def _sample_once(self) -> None:
        try:
            frame = self._reader.read()

        except Exception as sample_error:
            await self._report_sample_failure(sample_error)
            return

        await self._publish(frame)

    async def _publish(self, frame: NodeDashboardFrame) -> None:
        self._chart_series.record(frame.sampled_at, frame.chart_values)
        if self._summary_lines is not None:
            await self._write_summary_line(frame, self._readings(frame))
            return

        # One sample is one frame: no frame shows some sections of a sample
        # beside the table of the sample before.
        async with self._terminal.updating():
            await self._lay_out_for(frame)
            for panel_name, publish, content in self._panels(frame):
                await self._publish_changed(panel_name, publish, content)

            await self._publish_chart(frame)

    async def _lay_out_for(self, frame: NodeDashboardFrame) -> None:
        """Lay the sections out again where ``frame``'s badges or table need
        other rows than the last frame's; every section is then published
        afresh (laying out refits each component)."""
        canvas = self._terminal.canvas
        badge_lines = badge_line_count(frame.badges, content_width(canvas.width), len(BADGE_GAP), self._display_mode)
        if self._rows.need(badge_lines, len(frame.table_rows)):
            await self._terminal.resize(width=canvas.width, height=canvas.height)
            self._published.clear()

    def _panels(self, frame: NodeDashboardFrame) -> list[tuple[str, PanelPublisher, PanelContent]]:
        """Each section's name, publisher and content for ``frame``."""
        canvas_width = self._terminal.canvas.width
        table_width = content_width(canvas_width)
        meter_width = cell_meter_width(table_width, len(self._reader.layout.table_headers))
        return [
            ("identity", update_node_dashboard_identity, self._identity_lines(frame)),
            ("badges", update_node_dashboard_badges, frame.badges),
            *self._tile_panels(frame),
            ("table", update_node_dashboard_table, table_cells(frame.table_rows, self._display_mode, meter_width)),
            ("status", update_node_dashboard_status, self._status_line(table_width)),
        ]

    def _tile_panels(self, frame: NodeDashboardFrame) -> list[tuple[str, PanelPublisher, PanelContent]]:
        return [
            (f"tile {tile_index}", functools.partial(update_node_dashboard_tile, tile_channel(tile_index)), tile)
            for tile_index, tile in enumerate(frame.tiles)
        ]

    def _identity_lines(self, frame: NodeDashboardFrame) -> list[str]:
        """The identity column: who the node is, its lifecycle state and
        uptime, and where it listens. It holds no more lines than the header
        is tall (IDENTITY_LINE_COUNT): it never pages, so no line moves
        between frames unless its value changes."""
        role_line, *address_lines = frame.identity_lines
        uptime = f"up {format_duration(frame.uptime_seconds)}"
        return [role_line, f"{frame.lifecycle_state}{self._separator}{uptime}", *address_lines]

    def _status_line(self, width: int) -> str:
        return status_text(self._log_path, width, self._separator, ELLIPSES[self._display_mode])

    def _readings(self, frame: NodeDashboardFrame) -> list[str]:
        """Each series' newest value in the window, then the role's values
        in other units: the CI-safe summary line's readings."""
        return [
            *(
                f"{chart.title} {format_reading(self._chart_series.newest_value(chart_index))}"
                for chart_index, chart in enumerate(self._reader.layout.charts)
            ),
            *frame.value_lines,
        ]

    async def _write_summary_line(self, frame: NodeDashboardFrame, readings: list[str]) -> None:
        # A failed write closes the summary output: the error is returned
        # once, and logged.
        if (write_error := await self._summary_lines.write(frame, readings)) is not None:
            await self._log_warning(
                f"the dashboard stopped writing summary lines: {type(write_error).__name__}: {write_error}"
            )

    async def _publish_chart(self, frame: NodeDashboardFrame) -> None:
        # Every sample moves each series' points along the window, so the
        # chart is published every sample -- a series with no point in the
        # window plots none, never a gap as a zero nor stale points.
        layout = self._reader.layout
        await update_node_dashboard_chart(
            SeriesUpdate(
                points={
                    chart.title: self._chart_series.points(chart_index, chart.plots_zero)
                    for chart_index, chart in enumerate(layout.charts)
                },
                readings={
                    chart.title: format_rate(self._chart_series.newest_value(chart_index), layout.chart_reading_unit)
                    for chart_index, chart in enumerate(layout.charts)
                },
                extra_reading=frame.chart_extra_reading,
            )
        )

    async def _publish_changed(self, panel_name: str, publish: PanelPublisher, content: PanelContent) -> None:
        # Only changed panels are published: an unchanged sample costs no
        # render, and no component queues an update it already shows.
        if self._published.get(panel_name) == content:
            return

        self._published[panel_name] = content
        await publish(content)

    async def _log_warning(self, message: str) -> None:
        node_host, node_port = self._node.tcp_address
        await self._logger.log(
            ServerWarning(
                message=message,
                node_id=self._node.node_id.short,
                node_host=node_host,
                node_port=node_port,
            )
        )

    async def _report_sample_failure(self, sample_error: Exception) -> None:
        message = f"dashboard sample failed: {type(sample_error).__name__}: {sample_error}"
        node_host, node_port = self._node.tcp_address
        await self._logger.log(
            ServerError(
                message=message,
                node_id=self._node.node_id.short,
                node_host=node_host,
                node_port=node_port,
            )
        )
        await self._publish_changed("status", update_node_dashboard_status, message)

    def _raise_if_sampling_failed(self) -> None:
        # The run's error outlives its cancellation (which resets its status).
        if self._sampling_run.error is not None:
            raise NodeDashboardSamplingStopped(
                f"the dashboard stopped sampling: {self._sampling_run.error}\n{self._sampling_run.trace}"
            )
