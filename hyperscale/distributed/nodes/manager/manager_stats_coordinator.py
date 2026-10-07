"""``ManagerStatsCoordinator`` -- pickled under the namespace
``hyperscale.distributed.nodes.manager.stats`` (see that module)."""

from types import MappingProxyType
from typing import TYPE_CHECKING
from collections.abc import Awaitable, Callable
from hyperscale.distributed.reliability import (
    BackpressureLevel as StatsBackpressureLevel,
    BackpressureSignal,
    StatsBuffer,
)
from hyperscale.logging.hyperscale_logging_models import ServerDebug, ServerWarning
from hyperscale.distributed.jobs.windowed_stats_push import WindowedStatsPush
from hyperscale.distributed.runtime import Clock

from .backpressure_level import BackpressureLevel
from .models.manager_stats_metrics import ManagerStatsMetrics
from .progress_state import ProgressState

# AD-23: the stats buffer's backpressure level as the manager reports it;
# any level not listed (NONE) maps to BackpressureLevel.NONE.
_STATS_TO_MANAGER_BACKPRESSURE: MappingProxyType[StatsBackpressureLevel, BackpressureLevel] = MappingProxyType(
    {
        StatsBackpressureLevel.REJECT: BackpressureLevel.REJECT,
        StatsBackpressureLevel.BATCH: BackpressureLevel.BATCH,
        StatsBackpressureLevel.THROTTLE: BackpressureLevel.THROTTLE,
    }
)

if TYPE_CHECKING:
    from hyperscale.distributed.jobs import WindowedStatsCollector
    from hyperscale.distributed.models import WorkflowProgress
    from hyperscale.distributed.nodes.manager.state import ManagerState
    from hyperscale.distributed.nodes.manager.config import ManagerConfig
    from hyperscale.distributed.taskex import TaskRunner
    from hyperscale.logging import Logger

SendFunc = Callable[..., Awaitable[bytes | Exception | None]]


class ManagerStatsCoordinator:
    """
    Coordinates stats aggregation and backpressure.

    Handles:
    - Windowed stats collection from workers
    - Throughput tracking (AD-19)
    - Backpressure signaling (AD-23)
    - Stats buffer management
    """

    def __init__(
        self,
        state: "ManagerState",
        config: "ManagerConfig",
        logger: "Logger",
        node_id: str,
        task_runner: "TaskRunner",
        stats_buffer: StatsBuffer,
        windowed_stats: "WindowedStatsCollector",
        clock: Clock,
        get_healthy_worker_count: Callable[[], int],
        send_to_callback: SendFunc | None = None,
    ) -> None:
        self._state: "ManagerState" = state
        self._config: "ManagerConfig" = config
        self._logger: "Logger" = logger
        self._node_id: str = node_id
        self._task_runner: "TaskRunner" = task_runner
        self._send_to_callback: SendFunc | None = send_to_callback
        self._clock: Clock = clock
        self._get_healthy_worker_count: Callable[[], int] = get_healthy_worker_count

        self._progress_state: ProgressState = ProgressState.NORMAL
        self._progress_state_since: float = clock.monotonic()

        # AD-23: Stats buffer tracking for backpressure
        self._stats_buffer: StatsBuffer = stats_buffer

        self._windowed_stats: "WindowedStatsCollector" = windowed_stats

    async def record_dispatch(self) -> None:
        """Record a workflow dispatch a worker accepted, for throughput
        tracking (AD-19): the manager's advertised dispatch throughput is
        these over the current interval."""
        await self._state.increment_dispatch_throughput_count()

    async def refresh_dispatch_throughput(self) -> float:
        """Refresh throughput counters for the current interval."""
        return await self._state.update_dispatch_throughput(
            self._config.throughput_interval_seconds,
            now=self._clock.monotonic(),
        )

    def get_dispatch_throughput(self) -> float:
        """
        Calculate current dispatch throughput (AD-19).

        Returns:
            Dispatches per second over the current interval
        """
        now = self._clock.monotonic()
        interval_start = self._state._dispatch_throughput_interval_start
        interval_seconds = self._config.throughput_interval_seconds

        elapsed = now - interval_start
        if elapsed <= 0 or interval_start <= 0:
            return self._state._dispatch_throughput_last_value

        if elapsed >= interval_seconds:
            return self._state._dispatch_throughput_last_value

        count = self._state._dispatch_throughput_count
        return count / elapsed

    def get_expected_throughput(self) -> float:
        """Expected dispatch throughput from worker capacity (AD-19): one
        workflow per second per healthy worker -- the baseline the
        manager has always advertised. 0.0 with no healthy workers (idle,
        not stuck)."""
        return float(self._get_healthy_worker_count())

    def get_progress_state_duration(self) -> float:
        """
        Get how long we've been in current progress state.

        Returns:
            Duration in seconds
        """
        return self._clock.monotonic() - self._progress_state_since

    def should_apply_backpressure(self) -> bool:
        """
        Check if backpressure should be applied (AD-23).

        Returns:
            True if system is under load and should shed requests
        """
        return (
            self._stats_buffer.get_backpressure_level()
            >= StatsBackpressureLevel.THROTTLE
        )

    def get_backpressure_level(self) -> BackpressureLevel:
        """
        Get current backpressure level (AD-23).

        Returns:
            Current BackpressureLevel
        """
        level = self._stats_buffer.get_backpressure_level()
        return _STATS_TO_MANAGER_BACKPRESSURE.get(level, BackpressureLevel.NONE)

    def get_backpressure_signal(self) -> BackpressureSignal:
        """Return backpressure signal from the stats buffer."""
        return self._stats_buffer.get_backpressure_signal()

    async def record_progress_update(
        self,
        worker_id: str,
        progress: "WorkflowProgress",
    ) -> None:
        """
        Record a progress update for stats aggregation.

        Args:
            worker_id: Worker identifier
            progress: Workflow progress update
        """
        if not self._state.has_job_leader(progress.job_id):
            cleaned_windows = await self._windowed_stats.cleanup_job_windows(
                progress.job_id
            )
            await self._logger.log(
                ServerWarning(
                    message=(
                        "Skipping windowed stats for missing job "
                        f"{progress.job_id[:8]}... (cleaned {cleaned_windows} windows)"
                    ),
                    node_host=self._config.host,
                    node_port=self._config.tcp_port,
                    node_id=self._node_id,
                )
            )
            return

        self._stats_buffer.record(progress.rate_per_second or 0.0)
        await self._windowed_stats.record(worker_id, progress)
        await self._logger.log(
            ServerDebug(
                message=(
                    "Progress update recorded for workflow "
                    f"{progress.workflow_id[:8]}..."
                ),
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            )
        )

    async def push_batch_stats(self) -> None:
        """
        Push batched stats to gates/clients.

        Called periodically by the stats push loop. Aggregates closed
        windowed stats windows for each job and pushes them to registered
        progress callbacks. Entries are cleared from the windowed collector
        after successful aggregation.
        """
        job_ids = self._callback_routed_jobs_with_pending_stats()
        if not job_ids:
            return

        pushed_count = await self._push_stats_for_jobs(job_ids)

        if pushed_count > 0:
            await self._logger.log(ServerDebug(
                message=f"Pushed {pushed_count} stats windows across {len(job_ids)} jobs",
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            ))

    def _callback_routed_jobs_with_pending_stats(self) -> list[str]:
        """Jobs with pending windows and no origin gate.

        A gate-routed job's windows go to its origin gate (the windowed
        stats flush loop), not to a client callback.
        """
        return [
            job_id
            for job_id in self._windowed_stats.get_jobs_with_pending_stats()
            if self._state.get_job_origin_gate(job_id) is None
        ]

    async def _push_stats_for_jobs(self, job_ids: list[str]) -> int:
        """Push each job's aggregated stats in turn; the total windows pushed."""
        pushed_count = 0
        for job_id in job_ids:
            pushed_count += await self._push_job_stats(job_id)
        return pushed_count

    async def _push_job_stats(self, job_id: str) -> int:
        """
        Push aggregated stats for a single job to its callback.

        Returns:
            Number of stats windows pushed
        """
        aggregated = await self._windowed_stats.get_aggregated_stats(job_id)
        if not aggregated:
            return 0

        if (callback_addr := self._progress_callback_for(job_id)) is None:
            return 0

        return await self._push_stats_windows(job_id, callback_addr, aggregated)

    def _progress_callback_for(self, job_id: str) -> tuple[str, int] | None:
        """The job's progress callback, or None without one or without a send hook."""
        callback_addr = self._state.get_progress_callback(job_id)
        if not callback_addr or not self._send_to_callback:
            return None
        return callback_addr

    async def _push_stats_windows(
        self,
        job_id: str,
        callback_addr: tuple[str, int],
        aggregated: list[WindowedStatsPush],
    ) -> int:
        """Send every aggregated window to the callback; how many were delivered."""
        pushed_windows = 0
        for stats_push in aggregated:
            pushed_windows += await self._push_stats_window(job_id, callback_addr, stats_push)
        return pushed_windows

    async def _push_stats_window(
        self,
        job_id: str,
        callback_addr: tuple[str, int],
        stats_push: WindowedStatsPush,
    ) -> int:
        """Send one window; 1 when delivered, 0 after logging the failure."""
        try:
            reply = await self._send_to_callback(
                callback_addr,
                "windowed_stats_push",
                stats_push.dump(),
            )
            # send_tcp returns transport errors rather than raising.
            if isinstance(reply, Exception):
                raise reply
            return 1
        except Exception as send_error:
            await self._logger.log(ServerWarning(
                message=f"Failed to push stats for job {job_id[:8]}...: {send_error}",
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            ))
            return 0

    def get_stats_metrics(self) -> ManagerStatsMetrics:
        """Get stats-related metrics."""
        # Capture count before get_dispatch_throughput() which may reset it
        throughput_count = self._state._dispatch_throughput_count
        stats_buffer_metrics = self._stats_buffer.get_metrics()
        return {
            "dispatch_throughput": self.get_dispatch_throughput(),
            "expected_throughput": self.get_expected_throughput(),
            "progress_state": self._progress_state.value,
            "progress_state_duration": self.get_progress_state_duration(),
            "backpressure_level": self.get_backpressure_level().value,
            "stats_buffer_count": stats_buffer_metrics["hot_count"],
            "throughput_count": throughput_count,
        }

    def export_stats_checkpoint(self) -> list[tuple[float, float]]:
        """
        Export pending stats as a checkpoint for peer recovery (Task 33).

        Called during state sync to include stats in ManagerStateSnapshot.

        Returns:
            List of (timestamp, value) tuples from the stats buffer
        """
        return self._stats_buffer.export_checkpoint()

    async def import_stats_checkpoint(
        self, checkpoint: list[tuple[float, float]]
    ) -> int:
        """
        Import stats from a checkpoint during recovery (Task 33).

        Called when syncing state from a peer manager.

        Args:
            checkpoint: List of (timestamp, value) tuples

        Returns:
            Number of entries imported
        """
        if not checkpoint:
            return 0

        imported = self._stats_buffer.import_checkpoint(checkpoint)
        if imported > 0:
            await self._logger.log(
                ServerDebug(
                    message=f"Imported {imported} stats entries from peer checkpoint",
                    node_host=self._config.host,
                    node_port=self._config.tcp_port,
                    node_id=self._node_id,
                ),
            )
        return imported
