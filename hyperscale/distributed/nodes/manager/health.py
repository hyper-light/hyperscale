"""
Manager health module for worker health monitoring.

Handles SWIM callbacks, worker health tracking, AD-18 hybrid overload detection,
AD-26 deadline extensions, and AD-30 hierarchical failure detection with job-level suspicion.
"""

import time
from enum import Enum
from typing import TYPE_CHECKING

from hyperscale.distributed.models import WorkerHeartbeat
from hyperscale.distributed.reliability import HybridOverloadDetector
from hyperscale.logging.hyperscale_logging_models import ServerDebug, ServerWarning

if TYPE_CHECKING:
    from hyperscale.distributed.nodes.manager.state import ManagerState
    from hyperscale.distributed.nodes.manager.config import ManagerConfig
    from hyperscale.distributed.nodes.manager.registry import ManagerRegistry
    from hyperscale.distributed.taskex import TaskRunner
    from hyperscale.logging import Logger


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
        # More confirmations = shorter timeout = faster failure declaration
        shrink_factor = max(1, 1 + self.confirmation_count)
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
        task_runner: "TaskRunner",
    ) -> None:
        self._state: "ManagerState" = state
        self._config: "ManagerConfig" = config
        self._registry: "ManagerRegistry" = registry
        self._logger: "Logger" = logger
        self._node_id: str = node_id
        self._task_runner: "TaskRunner" = task_runner
        self._latency_max_age: float = 60.0
        self._latency_max_count: int = 30

        # AD-18: Hybrid overload detector for manager self-health
        self._overload_detector = HybridOverloadDetector()

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
        if hasattr(heartbeat, "deadline") and heartbeat.deadline:
            self._state._worker_deadlines[worker_id] = heartbeat.deadline

        worker_health_state = getattr(heartbeat, "health_overload_state", "healthy")
        previous_state, new_state = self._registry.update_worker_health_state(
            worker_id, worker_health_state
        )

        if previous_state and previous_state != new_state:
            self._log_worker_health_transition(worker_id, previous_state, new_state)
            self._check_aggregate_health_alerts()

        self._task_runner.run(
            self._logger.log,
            ServerDebug(
                message=f"Worker heartbeat from {worker_id[:8]}... cores={heartbeat.available_cores}/{heartbeat.total_cores} state={worker_health_state}",
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            ),
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
            ),
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
            ),
        )

    async def record_latency_sample(
        self,
        target_type: str,
        target_id: str,
        latency_ms: float,
    ) -> None:
        """
        Record a latency sample for health tracking.

        Also feeds the AD-18 hybrid overload detector for self-health monitoring.

        Args:
            target_type: Type of target (worker, peer, gate)
            target_id: Target identifier
            latency_ms: Measured latency in milliseconds
        """
        now = time.monotonic()
        sample = (now, latency_ms)

        if target_type == "worker":
            samples = await self._state.get_worker_latency_samples(target_id)
        elif target_type == "peer":
            samples = await self._state.get_peer_latency_samples(target_id)
        elif target_type == "gate":
            samples = self._state._gate_latency_samples
        else:
            return

        samples.append(sample)

        # AD-18: Feed latency to hybrid overload detector for manager self-health
        self._overload_detector.record_latency(latency_ms)

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

    def get_worker_health_state_counts(self) -> dict[str, int]:
        return self._registry.get_worker_health_state_counts()

    def _log_worker_health_transition(
        self,
        worker_id: str,
        previous_state: str,
        new_state: str,
    ) -> None:
        is_degradation = self._is_health_degradation(previous_state, new_state)

        if is_degradation:
            self._task_runner.run(
                self._logger.log,
                ServerWarning(
                    message=f"Worker {worker_id[:8]}... health degraded: {previous_state} -> {new_state}",
                    node_host=self._config.host,
                    node_port=self._config.tcp_port,
                    node_id=self._node_id,
                ),
            )
        else:
            self._task_runner.run(
                self._logger.log,
                ServerDebug(
                    message=f"Worker {worker_id[:8]}... health improved: {previous_state} -> {new_state}",
                    node_host=self._config.host,
                    node_port=self._config.tcp_port,
                    node_id=self._node_id,
                ),
            )

    def _is_health_degradation(self, previous_state: str, new_state: str) -> bool:
        state_severity = {"healthy": 0, "busy": 1, "stressed": 2, "overloaded": 3}
        previous_severity = state_severity.get(previous_state, 0)
        new_severity = state_severity.get(new_state, 0)
        return new_severity > previous_severity

    def _check_aggregate_health_alerts(self) -> None:
        counts = self._registry.get_worker_health_state_counts()
        total_workers = sum(counts.values())

        if total_workers == 0:
            return

        overloaded_count = counts.get("overloaded", 0)
        stressed_count = counts.get("stressed", 0)
        busy_count = counts.get("busy", 0)
        healthy_count = counts.get("healthy", 0)

        overloaded_ratio = overloaded_count / total_workers
        non_healthy_ratio = (
            overloaded_count + stressed_count + busy_count
        ) / total_workers

        overloaded_threshold = self._config.health_alert_overloaded_ratio
        non_healthy_threshold = self._config.health_alert_non_healthy_ratio

        if healthy_count == 0 and total_workers > 0:
            self._task_runner.run(
                self._logger.log,
                ServerWarning(
                    message=f"ALERT: All {total_workers} workers in non-healthy state (overloaded={overloaded_count}, stressed={stressed_count}, busy={busy_count})",
                    node_host=self._config.host,
                    node_port=self._config.tcp_port,
                    node_id=self._node_id,
                ),
            )
        elif overloaded_ratio >= overloaded_threshold:
            self._task_runner.run(
                self._logger.log,
                ServerWarning(
                    message=f"ALERT: Majority workers overloaded ({overloaded_count}/{total_workers} = {overloaded_ratio:.0%})",
                    node_host=self._config.host,
                    node_port=self._config.tcp_port,
                    node_id=self._node_id,
                ),
            )
        elif non_healthy_ratio >= non_healthy_threshold:
            self._task_runner.run(
                self._logger.log,
                ServerWarning(
                    message=f"ALERT: High worker stress ({non_healthy_ratio:.0%} non-healthy: overloaded={overloaded_count}, stressed={stressed_count}, busy={busy_count})",
                    node_host=self._config.host,
                    node_port=self._config.tcp_port,
                    node_id=self._node_id,
                ),
            )

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
            key for key in self._state._worker_job_last_progress if key[0] == job_id
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
            ),
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
                ),
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
                    ),
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

        keys_to_remove = [key for key in self._job_suspicions if key[1] == worker_id]
        for key in keys_to_remove:
            del self._job_suspicions[key]

        for job_dead_set in self._job_dead_workers.values():
            job_dead_set.discard(worker_id)

        progress_keys_to_remove = [
            key for key in self._state._worker_job_last_progress if key[0] == worker_id
        ]
        for key in progress_keys_to_remove:
            self._state._worker_job_last_progress.pop(key, None)

        self._task_runner.run(
            self._logger.log,
            ServerWarning(
                message=f"Worker {worker_id[:8]}... globally dead, cleared job suspicions",
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            ),
        )

    def clear_global_death(self, worker_id: str) -> None:
        """
        Clear global death status (worker rejoined).

        Args:
            worker_id: Worker that rejoined
        """
        self._global_dead_workers.discard(worker_id)

    def clear_job_suspicions(self, job_id: str) -> None:
        keys_to_remove = [key for key in self._job_suspicions if key[0] == job_id]
        for key in keys_to_remove:
            del self._job_suspicions[key]

        self._job_dead_workers.pop(job_id, None)

    def get_peer_manager_health_counts(self) -> dict[str, int]:
        counts = {"healthy": 0, "busy": 0, "stressed": 0, "overloaded": 0}

        for health_state in self._state._peer_manager_health_states.values():
            if health_state in counts:
                counts[health_state] += 1
            else:
                counts["healthy"] += 1

        return counts

    def check_peer_manager_health_alerts(self) -> None:
        counts = self.get_peer_manager_health_counts()
        total_peers = sum(counts.values())

        if total_peers == 0:
            return

        dc_leader_id = self._state._dc_leader_manager_id
        if dc_leader_id and (
            leader_state := self._state._peer_manager_health_states.get(dc_leader_id)
        ):
            if leader_state == "overloaded":
                self._fire_leader_overload_alert(dc_leader_id)
                return

        overloaded_count = counts.get("overloaded", 0)
        healthy_count = counts.get("healthy", 0)
        non_healthy_count = total_peers - healthy_count

        if healthy_count == 0:
            self._fire_all_managers_unhealthy_alert(counts, total_peers)
        elif overloaded_count / total_peers >= 0.5:
            self._fire_majority_overloaded_alert(overloaded_count, total_peers)
        elif non_healthy_count / total_peers >= 0.8:
            self._fire_high_stress_alert(counts, total_peers)

    def _fire_leader_overload_alert(self, leader_id: str) -> None:
        self._task_runner.run(
            self._logger.log,
            ServerWarning(
                message=f"ALERT: DC leader {leader_id[:8]}... overloaded - control plane saturated",
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            ),
        )

    def _fire_all_managers_unhealthy_alert(
        self,
        counts: dict[str, int],
        total_peers: int,
    ) -> None:
        self._task_runner.run(
            self._logger.log,
            ServerWarning(
                message=f"CRITICAL: All {total_peers} DC managers non-healthy (overloaded={counts['overloaded']}, stressed={counts['stressed']}, busy={counts['busy']})",
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            ),
        )

    def _fire_majority_overloaded_alert(
        self,
        overloaded_count: int,
        total_peers: int,
    ) -> None:
        self._task_runner.run(
            self._logger.log,
            ServerWarning(
                message=f"ALERT: Majority DC managers overloaded ({overloaded_count}/{total_peers})",
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            ),
        )

    def _fire_high_stress_alert(
        self,
        counts: dict[str, int],
        total_peers: int,
    ) -> None:
        non_healthy = total_peers - counts["healthy"]
        ratio = non_healthy / total_peers
        self._task_runner.run(
            self._logger.log,
            ServerWarning(
                message=f"WARNING: DC control plane stressed ({ratio:.0%} non-healthy: overloaded={counts['overloaded']}, stressed={counts['stressed']}, busy={counts['busy']})",
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            ),
        )

    def get_manager_overload_state(
        self,
        cpu_percent: float = 0.0,
        memory_percent: float = 0.0,
    ) -> str:
        """
        Get manager's own overload state (AD-18).

        Args:
            cpu_percent: Current CPU utilization (0-100)
            memory_percent: Current memory utilization (0-100)

        Returns:
            Overload state: "healthy", "busy", "stressed", or "overloaded"
        """
        return self._overload_detector.get_state(cpu_percent, memory_percent).value

    def get_overload_diagnostics(self) -> dict:
        """
        Get hybrid overload detector diagnostics (AD-18).

        Returns:
            Dict with baseline, drift, state, and other diagnostic info
        """
        return self._overload_detector.get_diagnostics()

    def get_health_metrics(self) -> dict:
        """Get health-related metrics."""
        overload_diag = self._overload_detector.get_diagnostics()
        return {
            "healthy_workers": self.get_healthy_worker_count(),
            "unhealthy_workers": self.get_unhealthy_worker_count(),
            "total_workers": len(self._state._workers),
            "tracked_latency_targets": (
                len(self._state._worker_latency_samples)
                + len(self._state._peer_manager_latency_samples)
            ),
            # AD-18 metrics
            "manager_overload_state": overload_diag.get("current_state", "healthy"),
            "manager_baseline_latency": overload_diag.get("baseline", 0.0),
            "manager_baseline_drift": overload_diag.get("baseline_drift", 0.0),
            # AD-30 metrics
            "job_suspicions": len(self._job_suspicions),
            "global_dead_workers": len(self._global_dead_workers),
            "jobs_with_dead_workers": len(self._job_dead_workers),
        }


class ExtensionTracker:
    """
    Tracks healthcheck extensions for a worker (AD-26).

    Implements logarithmic grant reduction to prevent abuse
    while allowing legitimate long-running operations.

    Grant formula: grant = max(min_grant, base_deadline / (2^extension_count))

    Extension denied if:
    - No progress since last extension
    - Total extensions exceed max
    - Node is already marked suspect
    """

    __slots__ = (
        "worker_id",
        "base_deadline",
        "min_grant",
        "max_extensions",
        "extension_count",
        "last_progress",
        "total_extended",
    )

    def __init__(
        self,
        worker_id: str,
        base_deadline: float = 30.0,
        min_grant: float = 1.0,
        max_extensions: int = 5,
    ) -> None:
        """
        Initialize extension tracker.

        Args:
            worker_id: Worker being tracked
            base_deadline: Base deadline in seconds
            min_grant: Minimum grant amount in seconds
            max_extensions: Maximum number of extensions allowed
        """
        self.worker_id = worker_id
        self.base_deadline = base_deadline
        self.min_grant = min_grant
        self.max_extensions = max_extensions
        self.extension_count = 0
        self.last_progress = 0.0
        self.total_extended = 0.0

    def request_extension(
        self,
        reason: str,
        current_progress: float,
    ) -> tuple[bool, float]:
        """
        Request deadline extension.

        Args:
            reason: Reason for extension ("long_workflow", "gc_pause", etc.)
            current_progress: Current progress 0.0-1.0

        Returns:
            (granted, extension_seconds) tuple
        """
        # Deny if too many extensions
        if self.extension_count >= self.max_extensions:
            return False, 0.0

        # Deny if no progress (except first extension)
        if current_progress <= self.last_progress and self.extension_count > 0:
            return False, 0.0

        # Calculate grant with logarithmic reduction
        grant = max(self.min_grant, self.base_deadline / (2**self.extension_count))

        self.extension_count += 1
        self.last_progress = current_progress
        self.total_extended += grant

        return True, grant

    def reset(self) -> None:
        """Reset tracker when worker completes operation or recovers."""
        self.extension_count = 0
        self.last_progress = 0.0
        self.total_extended = 0.0

    def get_remaining_extensions(self) -> int:
        """Get number of remaining extensions available."""
        return max(0, self.max_extensions - self.extension_count)

    def get_denial_reason(self, current_progress: float) -> str:
        """
        Get reason for denial.

        Args:
            current_progress: Current progress value

        Returns:
            Human-readable denial reason
        """
        if self.extension_count >= self.max_extensions:
            return f"Maximum extensions ({self.max_extensions}) exceeded"
        if current_progress <= self.last_progress:
            return f"No progress since last extension (current={current_progress}, last={self.last_progress})"
        return "Extension denied"


class HealthcheckExtensionManager:
    """
    Manages healthcheck extensions for all workers (AD-26).

    Handles extension requests from workers and updates deadlines.
    """

    def __init__(
        self,
        config: "ManagerConfig",
        logger: "Logger",
        node_id: str,
        task_runner,
    ) -> None:
        self._config = config
        self._logger = logger
        self._node_id = node_id
        self._task_runner = task_runner

        # Per-worker extension trackers
        self._extension_trackers: dict[str, ExtensionTracker] = {}
        # Current deadlines per worker
        self._worker_deadlines: dict[str, float] = {}

    def handle_extension_request(
        self,
        worker_id: str,
        reason: str,
        current_progress: float,
        estimated_completion: float,
    ) -> tuple[bool, float, float, int, str | None]:
        """
        Process extension request from worker.

        Args:
            worker_id: Worker requesting extension
            reason: Reason for request
            current_progress: Current progress 0.0-1.0
            estimated_completion: Unix timestamp of estimated completion

        Returns:
            (granted, extension_seconds, new_deadline, remaining_extensions, denial_reason)
        """
        tracker = self._extension_trackers.setdefault(
            worker_id, ExtensionTracker(worker_id=worker_id)
        )

        granted, extension_seconds = tracker.request_extension(
            reason=reason,
            current_progress=current_progress,
        )

        if granted:
            current_deadline = self._worker_deadlines.get(
                worker_id, time.monotonic() + 30.0
            )
            new_deadline = current_deadline + extension_seconds
            self._worker_deadlines[worker_id] = new_deadline

            self._task_runner.run(
                self._logger.log,
                ServerDebug(
                    message=f"Granted {extension_seconds:.1f}s extension to worker {worker_id[:8]}... (progress={current_progress:.2f})",
                    node_host=self._config.host,
                    node_port=self._config.tcp_port,
                    node_id=self._node_id,
                ),
            )

            return (
                True,
                extension_seconds,
                new_deadline,
                tracker.get_remaining_extensions(),
                None,
            )
        else:
            denial_reason = tracker.get_denial_reason(current_progress)

            self._task_runner.run(
                self._logger.log,
                ServerWarning(
                    message=f"Denied extension to worker {worker_id[:8]}...: {denial_reason}",
                    node_host=self._config.host,
                    node_port=self._config.tcp_port,
                    node_id=self._node_id,
                ),
            )

            return (
                False,
                0.0,
                self._worker_deadlines.get(worker_id, 0.0),
                tracker.get_remaining_extensions(),
                denial_reason,
            )

    def on_worker_healthy(self, worker_id: str) -> None:
        """Reset extension tracker when worker completes successfully."""
        if worker_id in self._extension_trackers:
            self._extension_trackers[worker_id].reset()

    def on_worker_removed(self, worker_id: str) -> None:
        """Cleanup when worker is removed."""
        self._extension_trackers.pop(worker_id, None)
        self._worker_deadlines.pop(worker_id, None)

    def get_worker_deadline(self, worker_id: str) -> float | None:
        """Get current deadline for a worker."""
        return self._worker_deadlines.get(worker_id)

    def get_metrics(self) -> dict:
        """Get extension manager metrics."""
        return {
            "tracked_workers": len(self._extension_trackers),
            "total_extensions_granted": sum(
                t.extension_count for t in self._extension_trackers.values()
            ),
        }
