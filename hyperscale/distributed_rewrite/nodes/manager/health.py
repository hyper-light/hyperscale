"""
Manager health module for worker health monitoring.

Handles SWIM callbacks, worker health tracking, and AD-26 deadline extensions.
"""

import time
from typing import TYPE_CHECKING

from hyperscale.distributed_rewrite.models import WorkerHeartbeat
from hyperscale.logging.hyperscale_logging_models import ServerDebug, ServerWarning

if TYPE_CHECKING:
    from hyperscale.distributed_rewrite.nodes.manager.state import ManagerState
    from hyperscale.distributed_rewrite.nodes.manager.config import ManagerConfig
    from hyperscale.distributed_rewrite.nodes.manager.registry import ManagerRegistry
    from hyperscale.logging.hyperscale_logger import Logger


class ManagerHealthMonitor:
    """
    Monitors worker and peer health.

    Handles:
    - SWIM callbacks for node failure/recovery
    - Worker health tracking and deadline extensions (AD-26)
    - Latency sample collection
    - Health signal calculation (AD-19)
    """

    def __init__(
        self,
        state: "ManagerState",
        config: "ManagerConfig",
        registry: "ManagerRegistry",
        logger: "Logger",
        node_id: str,
        task_runner,
    ) -> None:
        self._state = state
        self._config = config
        self._registry = registry
        self._logger = logger
        self._node_id = node_id
        self._task_runner = task_runner
        self._latency_max_age = 60.0
        self._latency_max_count = 30

    def handle_worker_heartbeat(
        self,
        heartbeat: WorkerHeartbeat,
        source_addr: tuple[str, int],
    ) -> None:
        """
        Handle embedded worker heartbeat from SWIM.

        Args:
            heartbeat: Worker heartbeat data
            source_addr: Source UDP address
        """
        worker_id = heartbeat.node_id

        # Clear unhealthy tracking if worker is alive
        self._state._worker_unhealthy_since.pop(worker_id, None)

        # Update deadline if worker provided one
        if hasattr(heartbeat, 'deadline') and heartbeat.deadline:
            self._state._worker_deadlines[worker_id] = heartbeat.deadline

        self._task_runner.run(
            self._logger.log,
            ServerDebug(
                message=f"Worker heartbeat from {worker_id[:8]}... cores={heartbeat.available_cores}/{heartbeat.total_cores}",
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            )
        )

    def handle_worker_failure(self, worker_id: str) -> None:
        """
        Handle worker failure detected by SWIM.

        Args:
            worker_id: Failed worker ID
        """
        if worker_id not in self._state._worker_unhealthy_since:
            self._state._worker_unhealthy_since[worker_id] = time.monotonic()

        self._task_runner.run(
            self._logger.log,
            ServerWarning(
                message=f"Worker {worker_id[:8]}... marked unhealthy",
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            )
        )

    def handle_worker_recovery(self, worker_id: str) -> None:
        """
        Handle worker recovery detected by SWIM.

        Args:
            worker_id: Recovered worker ID
        """
        self._state._worker_unhealthy_since.pop(worker_id, None)

        self._task_runner.run(
            self._logger.log,
            ServerDebug(
                message=f"Worker {worker_id[:8]}... recovered",
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            )
        )

    def record_latency_sample(
        self,
        target_type: str,
        target_id: str,
        latency_ms: float,
    ) -> None:
        """
        Record a latency sample for health tracking.

        Args:
            target_type: Type of target (worker, peer, gate)
            target_id: Target identifier
            latency_ms: Measured latency in milliseconds
        """
        now = time.monotonic()
        sample = (now, latency_ms)

        if target_type == "worker":
            samples = self._state._worker_latency_samples.setdefault(target_id, [])
        elif target_type == "peer":
            samples = self._state._peer_manager_latency_samples.setdefault(target_id, [])
        elif target_type == "gate":
            samples = self._state._gate_latency_samples
        else:
            return

        samples.append(sample)
        self._prune_latency_samples(samples)

    def _prune_latency_samples(self, samples: list[tuple[float, float]]) -> None:
        """Prune old latency samples."""
        now = time.monotonic()
        cutoff = now - self._latency_max_age

        # Remove old samples
        while samples and samples[0][0] < cutoff:
            samples.pop(0)

        # Limit count
        while len(samples) > self._latency_max_count:
            samples.pop(0)

    def get_worker_health_status(self, worker_id: str) -> str:
        """
        Get health status for a worker.

        Args:
            worker_id: Worker ID

        Returns:
            Health status: "healthy", "unhealthy", or "unknown"
        """
        if worker_id in self._state._worker_unhealthy_since:
            return "unhealthy"
        if worker_id in self._state._workers:
            return "healthy"
        return "unknown"

    def get_healthy_worker_count(self) -> int:
        """Get count of healthy workers."""
        return len(self._registry.get_healthy_worker_ids())

    def get_unhealthy_worker_count(self) -> int:
        """Get count of unhealthy workers."""
        return len(self._state._worker_unhealthy_since)

    def is_worker_responsive(self, worker_id: str, job_id: str) -> bool:
        """
        Check if worker is responsive for a job (AD-30).

        Args:
            worker_id: Worker ID
            job_id: Job ID

        Returns:
            True if worker has reported progress recently
        """
        key = (job_id, worker_id)
        last_progress = self._state._worker_job_last_progress.get(key)
        if last_progress is None:
            return True  # No tracking yet, assume responsive

        elapsed = time.monotonic() - last_progress
        return elapsed < self._config.job_responsiveness_threshold_seconds

    def record_job_progress(self, job_id: str, worker_id: str) -> None:
        """
        Record job progress from worker (AD-30).

        Args:
            job_id: Job ID
            worker_id: Worker ID
        """
        key = (job_id, worker_id)
        self._state._worker_job_last_progress[key] = time.monotonic()

    def cleanup_job_progress(self, job_id: str) -> None:
        """
        Cleanup progress tracking for a job.

        Args:
            job_id: Job ID to cleanup
        """
        keys_to_remove = [
            key for key in self._state._worker_job_last_progress
            if key[0] == job_id
        ]
        for key in keys_to_remove:
            self._state._worker_job_last_progress.pop(key, None)

    def get_health_metrics(self) -> dict:
        """Get health-related metrics."""
        return {
            "healthy_workers": self.get_healthy_worker_count(),
            "unhealthy_workers": self.get_unhealthy_worker_count(),
            "total_workers": len(self._state._workers),
            "tracked_latency_targets": (
                len(self._state._worker_latency_samples) +
                len(self._state._peer_manager_latency_samples)
            ),
        }
