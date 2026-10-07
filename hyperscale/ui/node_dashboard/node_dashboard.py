import asyncio
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
from hyperscale.ui.components.table import TableConfig
from hyperscale.ui.components.terminal import Terminal
from hyperscale.ui.config.mode import TerminalDisplayMode

from .dashboard_formatting import format_duration, format_reading
from .models import NodeDashboardFrame, NodeDashboardLayout, TableRow
from .node_dashboard_actions import (
    update_node_dashboard_chart,
    update_node_dashboard_cluster,
    update_node_dashboard_detail,
    update_node_dashboard_identity,
    update_node_dashboard_readings,
    update_node_dashboard_status,
    update_node_dashboard_summary,
    update_node_dashboard_table,
)
from .node_dashboard_chart_series import NodeDashboardChartSeries
from .node_dashboard_config import NodeDashboardConfig
from .node_dashboard_reader import NodeDashboardReader
from .node_dashboard_sampling_stopped import NodeDashboardSamplingStopped
from .node_dashboard_summary_lines import NodeDashboardSummaryLines
from .node_dashboard_sections import (
    generate_node_dashboard_sections,
    node_dashboard_table_config,
)

PanelContent = list[str] | list[TableRow] | str
PanelPublisher = Callable[[PanelContent], Awaitable[object]]

# The terminal framework's display mode for each rendering terminal mode:
# "ci" renders without the extended (color) sequences.
DISPLAY_MODES: dict[TerminalMode, TerminalDisplayMode] = {
    "full": "extended",
    "ci": "compatability",
}
# The padding HyperscaleInterface renders `run workflow`'s terminal with.
HORIZONTAL_PADDING = 4
VERTICAL_PADDING = 1


def dashboard_terminal(
    layout: NodeDashboardLayout,
    terminal_mode: TerminalMode,
    table_config: TableConfig,
) -> Terminal | None:
    """The terminal a dashboard renders its frames through, in a mode
    that renders frames; None in any other."""
    if terminal_mode not in DISPLAY_MODES:
        return None

    return Terminal(generate_node_dashboard_sections(layout, table_config, DISPLAY_MODES[terminal_mode]))


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
    each series' newest value, and the role's values in other units, are
    listed beside it.
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
        self._status_line = f"ctrl-c stops the node | logs {log_path}"
        window_seconds = env.SLO_EVALUATION_WINDOW_SECONDS
        table_config = node_dashboard_table_config(
            reader.layout, DISPLAY_MODES.get(terminal_mode, DISPLAY_MODES["ci"])
        )
        self._terminal = dashboard_terminal(reader.layout, terminal_mode, table_config)
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
        readings = self._readings(frame)
        if self._summary_lines is not None:
            await self._write_summary_line(frame, readings)
            return

        # The identity column holds no more lines than the header is tall
        # (IDENTITY_LINE_COUNT): it never pages, so no line moves between
        # frames unless its value changes.
        identity_lines = [
            *frame.identity_lines,
            f"up {format_duration(frame.uptime_seconds)} {frame.lifecycle_state}",
        ]
        panels: tuple[tuple[str, PanelPublisher, PanelContent], ...] = (
            ("identity", update_node_dashboard_identity, identity_lines),
            ("cluster", update_node_dashboard_cluster, frame.cluster_lines),
            ("summary", update_node_dashboard_summary, frame.summary_lines),
            ("detail", update_node_dashboard_detail, frame.detail_lines),
            ("table", update_node_dashboard_table, frame.table_rows),
            ("status", update_node_dashboard_status, self._status_line),
            ("readings", update_node_dashboard_readings, readings),
        )
        # One sample is one frame: no frame shows some panels of a sample
        # beside the table of the sample before.
        async with self._terminal.updating():
            for panel_name, publish, content in panels:
                await self._publish_changed(panel_name, publish, content)

            await self._publish_charts(frame)

    def _readings(self, frame: NodeDashboardFrame) -> list[str]:
        """Each series' newest value in the window, then the role's values
        in other units."""
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

    async def _publish_charts(self, frame: NodeDashboardFrame) -> None:
        # Every sample moves each series' points along the window, so the
        # chart is published every sample -- a series with no point in the
        # window plots none, never a gap as a zero nor stale points.
        await update_node_dashboard_chart(
            {
                chart.title: self._chart_series.points(chart_index)
                for chart_index, chart in enumerate(self._reader.layout.charts)
            }
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
