import asyncio
import pathlib
from collections.abc import Awaitable, Callable

from hyperscale.core.jobs.models import TerminalMode
from hyperscale.distributed.env import Env
from hyperscale.distributed.swim.health_aware_server import HealthAwareServer
from hyperscale.distributed.taskex import TaskRunner
from hyperscale.distributed.taskex.run import Run
from hyperscale.logging import Logger
from hyperscale.logging.hyperscale_logging_models import ServerError
from hyperscale.ui.components.terminal import Terminal
from hyperscale.ui.config.mode import TerminalDisplayMode

from .models import NodeDashboardFrame, TableRow
from .node_dashboard_actions import (
    update_node_dashboard_cluster,
    update_node_dashboard_detail,
    update_node_dashboard_identity,
    update_node_dashboard_status,
    update_node_dashboard_summary,
    update_node_dashboard_table,
)
from .node_dashboard_config import NodeDashboardConfig
from .node_dashboard_reader import NodeDashboardReader
from .node_dashboard_sampling_stopped import NodeDashboardSamplingStopped
from .node_dashboard_sections import generate_node_dashboard_sections

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


class NodeDashboard:
    """A live terminal dashboard for a running node.

    It renders through the hyperscale terminal framework the way `run
    workflow` does (``Terminal`` sections updated by ``@action()``
    publishers) and samples its node on its own interval, through a reader
    that reads the node's state synchronously: the dashboard never awaits
    the node or the network, so it cannot stall the node, and a reader
    that raises is logged and shown on the status line while the sampling
    goes on.

    With ``terminal_mode`` "disabled" it renders nothing and ``start`` and
    ``stop`` do nothing. Its owner calls ``start`` once and ``stop`` on
    every exit path: ``stop`` cancels and awaits the sampling loop, stops
    the terminal (restoring the cursor), releases it, and closes the
    logger the dashboard was given (it owns it).
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
    ) -> None:
        self._reader = reader
        self._node = node
        self._env = env
        self._config = config
        self._logger = logger
        self._status_line = f"ctrl-c stops the node | logs {log_path}"
        self._terminal: Terminal | None = (
            Terminal(generate_node_dashboard_sections(reader.layout, DISPLAY_MODES[terminal_mode]))
            if terminal_mode in DISPLAY_MODES
            else None
        )
        self._task_runner: TaskRunner | None = None
        self._sampling_run: Run | None = None
        self._published: dict[str, PanelContent] = {}

    async def start(self) -> None:
        """Render the dashboard and start sampling the node."""
        if self._terminal is None:
            return

        self._task_runner = TaskRunner(0, self._env)
        await self._terminal.render(
            horizontal_padding=HORIZONTAL_PADDING,
            vertical_padding=VERTICAL_PADDING,
        )
        self._sampling_run = self._task_runner.run(self._sample_until_cancelled)

    async def stop(self) -> None:
        """Stop sampling, stop the terminal and release it. Raises
        ``NodeDashboardSamplingStopped`` if the sampling loop had ended on
        an error of its own (after the terminal is restored)."""
        if self._sampling_run is None:
            return

        await self._task_runner.cancel(self._sampling_run.token)
        await self._task_runner.shutdown()
        await self._terminal.stop()
        await self._terminal.close()
        await self._logger.close()
        self._raise_if_sampling_failed()

    async def _sample_until_cancelled(self) -> None:
        sample_interval_seconds = max(self._config.sample_interval_seconds, self._terminal.refresh_interval)
        while True:
            await self._sample_once()
            await asyncio.sleep(sample_interval_seconds)

    async def _sample_once(self) -> None:
        try:
            frame = self._reader.read()

        except Exception as sample_error:
            await self._report_sample_failure(sample_error)
            return

        await self._publish(frame)

    async def _publish(self, frame: NodeDashboardFrame) -> None:
        panels: tuple[tuple[str, PanelPublisher, PanelContent], ...] = (
            ("identity", update_node_dashboard_identity, frame.identity_lines),
            ("cluster", update_node_dashboard_cluster, frame.cluster_lines),
            ("summary", update_node_dashboard_summary, frame.summary_lines),
            ("detail", update_node_dashboard_detail, frame.detail_lines),
            ("table", update_node_dashboard_table, frame.table_rows),
            ("status", update_node_dashboard_status, self._status_line),
        )
        for panel_name, publish, content in panels:
            await self._publish_changed(panel_name, publish, content)

    async def _publish_changed(self, panel_name: str, publish: PanelPublisher, content: PanelContent) -> None:
        # Only changed panels are published: an unchanged sample costs no
        # render, and no component queues an update it already shows.
        if self._published.get(panel_name) == content:
            return

        self._published[panel_name] = content
        await publish(content)

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
