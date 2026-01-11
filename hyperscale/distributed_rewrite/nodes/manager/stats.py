"""
Manager stats module.

Handles windowed stats aggregation, backpressure signaling, and
throughput tracking per AD-19 and AD-23 specifications.
"""

import time
from typing import TYPE_CHECKING

from hyperscale.logging.hyperscale_logging_models import ServerDebug

if TYPE_CHECKING:
    from hyperscale.distributed_rewrite.nodes.manager.state import ManagerState
    from hyperscale.distributed_rewrite.nodes.manager.config import ManagerConfig
    from hyperscale.logging.hyperscale_logger import Logger


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
        task_runner,
    ) -> None:
        self._state = state
        self._config = config
        self._logger = logger
        self._node_id = node_id
        self._task_runner = task_runner

    def record_dispatch(self) -> None:
        """Record a workflow dispatch for throughput tracking."""
        self._state._dispatch_throughput_count += 1

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

        if elapsed >= interval_seconds:
            # Calculate throughput for completed interval
            count = self._state._dispatch_throughput_count
            throughput = count / elapsed if elapsed > 0 else 0.0

            # Reset for next interval
            self._state._dispatch_throughput_count = 0
            self._state._dispatch_throughput_interval_start = now
            self._state._dispatch_throughput_last_value = throughput

            return throughput

        # Return last calculated value during interval
        return self._state._dispatch_throughput_last_value

    def get_expected_throughput(self) -> float:
        """
        Get expected dispatch throughput based on worker capacity.

        Returns:
            Expected dispatches per second
        """
        # Simple calculation based on healthy worker count
        # Full implementation would consider actual capacity
        healthy_count = len(self._state._workers) - len(self._state._worker_unhealthy_since)
        # Assume ~1 dispatch/sec per healthy worker as baseline
        return float(max(healthy_count, 1))

    def should_apply_backpressure(self) -> bool:
        """
        Check if backpressure should be applied (AD-23).

        Returns:
            True if system is under load and should shed requests
        """
        # Check stats buffer thresholds
        # In full implementation, this would check StatsBuffer fill level
        return False

    def get_backpressure_level(self) -> str:
        """
        Get current backpressure level (AD-23).

        Returns:
            "none", "throttle", "batch", or "reject"
        """
        # In full implementation, this checks StatsBuffer thresholds
        return "none"

    def record_progress_update(self, job_id: str, workflow_id: str) -> None:
        """
        Record a progress update for stats aggregation.

        Args:
            job_id: Job ID
            workflow_id: Workflow ID
        """
        # In full implementation, this feeds WindowedStatsCollector
        self._task_runner.run(
            self._logger.log,
            ServerDebug(
                message=f"Progress update recorded for workflow {workflow_id[:8]}...",
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            )
        )

    def get_stats_metrics(self) -> dict:
        """Get stats-related metrics."""
        return {
            "dispatch_throughput": self.get_dispatch_throughput(),
            "expected_throughput": self.get_expected_throughput(),
            "backpressure_level": self.get_backpressure_level(),
            "throughput_count": self._state._dispatch_throughput_count,
        }
