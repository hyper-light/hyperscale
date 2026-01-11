"""
Manager health module for worker health monitoring.

Handles SWIM callbacks, worker health tracking, AD-26 deadline extensions,
and AD-30 hierarchical failure detection with job-level suspicion.
"""

import time
from enum import Enum
from typing import TYPE_CHECKING

from hyperscale.distributed_rewrite.models import WorkerHeartbeat
from hyperscale.logging.hyperscale_logging_models import ServerDebug, ServerWarning

if TYPE_CHECKING:
    from hyperscale.distributed_rewrite.nodes.manager.state import ManagerState
    from hyperscale.distributed_rewrite.nodes.manager.config import ManagerConfig
    from hyperscale.distributed_rewrite.nodes.manager.registry import ManagerRegistry
    from hyperscale.logging.hyperscale_logger import Logger


class NodeStatus(Enum):
    """
    Node status for AD-30 hierarchical failure detection.

    Distinguishes between global liveness and job-specific responsiveness.
    """

    ALIVE = "alive"  # Not suspected at any layer
    SUSPECTED_GLOBAL = "suspected_global"  # Machine may be down
    SUSPECTED_JOB = "suspected_job"  # Unresponsive for specific job(s) but not global
    DEAD_GLOBAL = "dead_global"  # Declared dead at global level
    DEAD_JOB = "dead_job"  # Declared dead for specific job only


class JobSuspicion:
    """
    Tracks job-specific suspicion state for AD-30.

    Per (job_id, worker_id) suspicion with confirmation tracking.
    """

    __slots__ = (
        "job_id",
        "worker_id",
        "started_at",
        "confirmation_count",
        "last_confirmation_at",
        "timeout_seconds",
    )

    def __init__(
        self,
        job_id: str,
        worker_id: str,
        timeout_seconds: float = 10.0,
    ) -> None:
        self.job_id = job_id
        self.worker_id = worker_id
        self.started_at = time.monotonic()
        self.confirmation_count = 0
        self.last_confirmation_at = self.started_at
        self.timeout_seconds = timeout_seconds

    def add_confirmation(self) -> None:
        """Add a confirmation (does NOT reschedule timer per AD-30)."""
        self.confirmation_count += 1
        self.last_confirmation_at = time.monotonic()

    def time_remaining(self, cluster_size: int) -> float:
        """
        Calculate time remaining before expiration.

        Per Lifeguard, timeout shrinks with confirmations.

        Args:
            cluster_size: Number of nodes in cluster

        Returns:
            Seconds until expiration
        """
        # Timeout shrinks with confirmations (Lifeguard formula)
        log_n = max(1, cluster_size).bit_length()
        shrink_factor = max(1, log_n - self.confirmation_count)
        effective_timeout = self.timeout_seconds / shrink_factor

        elapsed = time.monotonic() - self.started_at
        return max(0, effective_timeout - elapsed)

    def is_expired(self, cluster_size: int) -> bool:
        """Check if suspicion has expired."""
        return self.time_remaining(cluster_size) <= 0


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

        # AD-30: Job-level suspicion tracking
        # Key: (job_id, worker_id) -> JobSuspicion
        self._job_suspicions: dict[tuple[str, str], JobSuspicion] = {}
        # Workers declared dead for specific jobs
        self._job_dead_workers: dict[str, set[str]] = {}  # job_id -> {worker_ids}
        # Global dead workers (affects all jobs)
        self._global_dead_workers: set[str] = set()

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

    # ========== AD-30: Job Suspicion Management ==========

    def suspect_job(
        self,
        job_id: str,
        worker_id: str,
        timeout_seconds: float | None = None,
    ) -> None:
        """
        Start job-specific suspicion for a worker (AD-30).

        Called when a worker is unresponsive for a specific job.

        Args:
            job_id: Job ID
            worker_id: Worker to suspect
            timeout_seconds: Optional custom timeout
        """
        key = (job_id, worker_id)
        if key in self._job_suspicions:
            return  # Already suspected

        timeout = timeout_seconds or self._config.job_responsiveness_threshold_seconds
        self._job_suspicions[key] = JobSuspicion(
            job_id=job_id,
            worker_id=worker_id,
            timeout_seconds=timeout,
        )

        self._task_runner.run(
            self._logger.log,
            ServerWarning(
                message=f"Job {job_id[:8]}... suspecting worker {worker_id[:8]}...",
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            )
        )

    def confirm_job_suspicion(self, job_id: str, worker_id: str) -> None:
        """
        Add confirmation to job suspicion (does NOT reschedule per AD-30).

        Args:
            job_id: Job ID
            worker_id: Suspected worker
        """
        key = (job_id, worker_id)
        if suspicion := self._job_suspicions.get(key):
            suspicion.add_confirmation()

    def refute_job_suspicion(self, job_id: str, worker_id: str) -> None:
        """
        Refute job suspicion (worker proved responsive).

        Args:
            job_id: Job ID
            worker_id: Worker to clear suspicion for
        """
        key = (job_id, worker_id)
        if key in self._job_suspicions:
            del self._job_suspicions[key]

            self._task_runner.run(
                self._logger.log,
                ServerDebug(
                    message=f"Cleared job {job_id[:8]}... suspicion for worker {worker_id[:8]}...",
                    node_host=self._config.host,
                    node_port=self._config.tcp_port,
                    node_id=self._node_id,
                )
            )

    def check_job_suspicion_expiry(self) -> list[tuple[str, str]]:
        """
        Check for expired job suspicions and declare workers dead.

        Returns:
            List of (job_id, worker_id) pairs declared dead
        """
        cluster_size = len(self._state._workers)
        expired: list[tuple[str, str]] = []

        for key, suspicion in list(self._job_suspicions.items()):
            if suspicion.is_expired(cluster_size):
                job_id, worker_id = key
                expired.append((job_id, worker_id))

                # Mark worker as dead for this job
                if job_id not in self._job_dead_workers:
                    self._job_dead_workers[job_id] = set()
                self._job_dead_workers[job_id].add(worker_id)

                # Remove suspicion
                del self._job_suspicions[key]

                self._task_runner.run(
                    self._logger.log,
                    ServerWarning(
                        message=f"Worker {worker_id[:8]}... declared dead for job {job_id[:8]}... (suspicion expired)",
                        node_host=self._config.host,
                        node_port=self._config.tcp_port,
                        node_id=self._node_id,
                    )
                )

        return expired

    def is_worker_alive_for_job(self, job_id: str, worker_id: str) -> bool:
        """
        Check if worker is alive for a specific job (AD-30).

        Args:
            job_id: Job ID
            worker_id: Worker ID

        Returns:
            True if worker is not dead for this job
        """
        # Check global death first
        if worker_id in self._global_dead_workers:
            return False

        # Check job-specific death
        job_dead = self._job_dead_workers.get(job_id, set())
        return worker_id not in job_dead

    def get_node_status(self, worker_id: str, job_id: str | None = None) -> NodeStatus:
        """
        Get comprehensive node status (AD-30).

        Args:
            worker_id: Worker ID
            job_id: Optional job ID for job-specific check

        Returns:
            Current NodeStatus
        """
        # Check global death
        if worker_id in self._global_dead_workers:
            return NodeStatus.DEAD_GLOBAL

        # Check global suspicion
        if worker_id in self._state._worker_unhealthy_since:
            return NodeStatus.SUSPECTED_GLOBAL

        if job_id:
            # Check job-specific death
            job_dead = self._job_dead_workers.get(job_id, set())
            if worker_id in job_dead:
                return NodeStatus.DEAD_JOB

            # Check job-specific suspicion
            key = (job_id, worker_id)
            if key in self._job_suspicions:
                return NodeStatus.SUSPECTED_JOB

        return NodeStatus.ALIVE

    def on_global_death(self, worker_id: str) -> None:
        """
        Handle global worker death (AD-30).

        Clears all job suspicions for this worker.

        Args:
            worker_id: Dead worker ID
        """
        self._global_dead_workers.add(worker_id)

        # Clear all job suspicions for this worker
        keys_to_remove = [
            key for key in self._job_suspicions
            if key[1] == worker_id
        ]
        for key in keys_to_remove:
            del self._job_suspicions[key]

        # Clear from job-specific dead sets (global death supersedes)
        for job_dead_set in self._job_dead_workers.values():
            job_dead_set.discard(worker_id)

        self._task_runner.run(
            self._logger.log,
            ServerWarning(
                message=f"Worker {worker_id[:8]}... globally dead, cleared job suspicions",
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            )
        )

    def clear_global_death(self, worker_id: str) -> None:
        """
        Clear global death status (worker rejoined).

        Args:
            worker_id: Worker that rejoined
        """
        self._global_dead_workers.discard(worker_id)

    def clear_job_suspicions(self, job_id: str) -> None:
        """
        Clear all suspicions for a completed job.

        Args:
            job_id: Job ID to cleanup
        """
        keys_to_remove = [
            key for key in self._job_suspicions
            if key[0] == job_id
        ]
        for key in keys_to_remove:
            del self._job_suspicions[key]

        self._job_dead_workers.pop(job_id, None)

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
