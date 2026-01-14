"""
Manager stats module.

Handles windowed stats aggregation, backpressure signaling, and
throughput tracking per AD-19 and AD-23 specifications.
"""

import time
from enum import Enum
from typing import TYPE_CHECKING, Any

from hyperscale.distributed.reliability import (
    BackpressureLevel as StatsBackpressureLevel,
    BackpressureSignal,
    StatsBuffer,
)
from hyperscale.logging.hyperscale_logging_models import ServerDebug, ServerWarning

if TYPE_CHECKING:
    from hyperscale.distributed.jobs import WindowedStatsCollector
    from hyperscale.distributed.models import WorkflowProgress
    from hyperscale.distributed.nodes.manager.state import ManagerState
    from hyperscale.distributed.nodes.manager.config import ManagerConfig
    from hyperscale.distributed.taskex import TaskRunner
    from hyperscale.logging import Logger


class ProgressState(Enum):
    """
    Progress state for AD-19 Three-Signal Health Model.

    Tracks dispatch throughput relative to expected capacity.
    """

    NORMAL = "normal"  # >= 80% of expected throughput
    SLOW = "slow"  # 50-80% of expected throughput
    DEGRADED = "degraded"  # 20-50% of expected throughput
    STUCK = "stuck"  # < 20% of expected throughput


class BackpressureLevel(Enum):
    """
    Backpressure levels for AD-23.

    Determines how aggressively to shed load.
    """

    NONE = "none"  # No backpressure
    THROTTLE = "throttle"  # Slow down incoming requests
    BATCH = "batch"  # Batch stats updates
    REJECT = "reject"  # Reject new stats updates


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
    ) -> None:
        self._state: "ManagerState" = state
        self._config: "ManagerConfig" = config
        self._logger: "Logger" = logger
        self._node_id: str = node_id
        self._task_runner: "TaskRunner" = task_runner

        self._progress_state: ProgressState = ProgressState.NORMAL
        self._progress_state_since: float = time.monotonic()

        # AD-23: Stats buffer tracking for backpressure
        self._stats_buffer: StatsBuffer = stats_buffer

        self._windowed_stats: "WindowedStatsCollector" = windowed_stats

    async def record_dispatch(self) -> None:
        """Record a workflow dispatch for throughput tracking."""
        await self._state.increment_dispatch_throughput_count()

    async def refresh_dispatch_throughput(self) -> float:
        """Refresh throughput counters for the current interval."""
        return await self._state.update_dispatch_throughput(
            self._config.throughput_interval_seconds
        )

    def get_dispatch_throughput(self) -> float:
        """
        Calculate current dispatch throughput (AD-19).

        Returns:
            Dispatches per second over the current interval
        """
        now = time.monotonic()
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
        """
        Get expected dispatch throughput based on worker capacity.

        Returns:
            Expected dispatches per second (0.0 if no workers)
        """
        # Simple calculation based on healthy worker count
        # Full implementation would consider actual capacity
        healthy_count = len(self._state._workers) - len(
            self._state._worker_unhealthy_since
        )
        # Return 0.0 if no workers (system is idle, not stuck)
        return float(healthy_count)

    def get_progress_state(self) -> ProgressState:
        """
        Calculate and return current progress state (AD-19).

        Based on ratio of actual throughput to expected throughput:
        - NORMAL: >= 80%
        - SLOW: 50-80%
        - DEGRADED: 20-50%
        - STUCK: < 20%

        Returns:
            Current ProgressState
        """
        actual = self.get_dispatch_throughput()
        expected = self.get_expected_throughput()

        if expected <= 0:
            return ProgressState.NORMAL

        ratio = actual / expected
        now = time.monotonic()

        if ratio >= self._config.progress_normal_ratio:
            new_state = ProgressState.NORMAL
        elif ratio >= self._config.progress_slow_ratio:
            new_state = ProgressState.SLOW
        elif ratio >= self._config.progress_degraded_ratio:
            new_state = ProgressState.DEGRADED
        else:
            new_state = ProgressState.STUCK

        # Track state changes
        if new_state != self._progress_state:
            self._task_runner.run(
                self._logger.log,
                ServerWarning(
                    message=f"Progress state changed: {self._progress_state.value} -> {new_state.value} (ratio={ratio:.2f})",
                    node_host=self._config.host,
                    node_port=self._config.tcp_port,
                    node_id=self._node_id,
                ),
            )
            self._progress_state = new_state
            self._progress_state_since = now

        return self._progress_state

    def get_progress_state_duration(self) -> float:
        """
        Get how long we've been in current progress state.

        Returns:
            Duration in seconds
        """
        return time.monotonic() - self._progress_state_since

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
        if level == StatsBackpressureLevel.REJECT:
            return BackpressureLevel.REJECT
        if level == StatsBackpressureLevel.BATCH:
            return BackpressureLevel.BATCH
        if level == StatsBackpressureLevel.THROTTLE:
            return BackpressureLevel.THROTTLE
        return BackpressureLevel.NONE

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
        if not self._state.has_job_context(progress.job_id):
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

        Called periodically by the stats push loop.
        """
        # In full implementation, this would:
        # 1. Aggregate windowed stats
        # 2. Push to registered callbacks
        # 3. Clear processed entries
        stats_buffer_metrics = self._stats_buffer.get_metrics()
        self._task_runner.run(
            self._logger.log,
            ServerDebug(
                message=(
                    f"Batch stats push (buffer={stats_buffer_metrics['hot_count']})"
                ),
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            ),
        )

    def get_stats_metrics(self) -> dict[str, Any]:
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
            self._task_runner.run(
                self._logger.log,
                ServerDebug(
                    message=f"Imported {imported} stats entries from peer checkpoint",
                    node_host=self._config.host,
                    node_port=self._config.tcp_port,
                    node_id=self._node_id,
                ),
            )
        return imported
