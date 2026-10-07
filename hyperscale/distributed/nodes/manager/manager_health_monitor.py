"""``ManagerHealthMonitor`` -- pickled under the namespace
``hyperscale.distributed.nodes.manager.health`` (see that module)."""

from typing import TYPE_CHECKING
import asyncio
from hyperscale.distributed.models import WorkerHeartbeat
from hyperscale.logging.hyperscale_logging_models import ServerDebug, ServerWarning

from .health_shared import _DEFAULT_CLOCK
from .job_suspicion import JobSuspicion
from .models.manager_health_metrics import ManagerHealthMetrics

if TYPE_CHECKING:
    from hyperscale.distributed.nodes.manager.state import ManagerState
    from hyperscale.distributed.nodes.manager.models.manager_config import ManagerConfig
    from hyperscale.distributed.nodes.manager.registry import ManagerRegistry
    from hyperscale.distributed.taskex import TaskRunner
    from hyperscale.logging import Logger


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

        # Lock for health state mutations to prevent race conditions
        self._health_state_lock: asyncio.Lock = asyncio.Lock()

        # AD-30 job-layer suspicion: (job_id, worker_id) -> JobSuspicion.
        # The global layer is the server's HierarchicalFailureDetector.
        self._job_suspicions: dict[tuple[str, str], JobSuspicion] = {}

    async def handle_worker_heartbeat(
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

        async with self._health_state_lock:
            # Clear unhealthy tracking if worker is alive
            self._state._worker_unhealthy_since.pop(worker_id, None)

            # Update deadline if worker provided one
            if hasattr(heartbeat, "deadline") and heartbeat.deadline:
                self._state._worker_deadlines[worker_id] = heartbeat.deadline

            worker_health_state = getattr(heartbeat, "health_overload_state", "healthy")
            previous_state, new_state = self._registry.update_worker_health_state(
                worker_id, worker_health_state
            )

            # AD-19 addendum (Phase D): record worker-tier LHM. The
            # max across registered workers is what we publish to gates
            # via ManagerHeartbeat.worker_max_lhm_score so cross-DC
            # correlation sees worker-tier stress. Defaulting to 0
            # when the field is absent keeps us compatible with peers
            # that haven't been upgraded yet.
            self._state._worker_lhm_scores[worker_id] = (
                getattr(heartbeat, "lhm_score", 0) or 0
            )

        if previous_state and previous_state != new_state:
            await self._log_worker_health_transition(worker_id, previous_state, new_state)
            await self._check_aggregate_health_alerts()

        await self._logger.log(
            ServerDebug(
                message=f"Worker heartbeat from {worker_id[:8]}... cores={heartbeat.available_cores}/{heartbeat.total_cores} state={worker_health_state}",
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            ),
        )

    async def handle_worker_failure(self, worker_id: str) -> None:
        """
        Handle worker failure detected by SWIM.

        Args:
            worker_id: Failed worker ID
        """
        async with self._health_state_lock:
            if worker_id not in self._state._worker_unhealthy_since:
                self._state._worker_unhealthy_since[worker_id] = _DEFAULT_CLOCK.monotonic()

        await self._logger.log(
            ServerWarning(
                message=f"Worker {worker_id[:8]}... marked unhealthy",
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            ),
        )

    async def handle_worker_recovery(self, worker_id: str) -> None:
        """
        Handle worker recovery detected by SWIM.

        Args:
            worker_id: Recovered worker ID
        """
        async with self._health_state_lock:
            self._state._worker_unhealthy_since.pop(worker_id, None)

        await self._logger.log(
            ServerDebug(
                message=f"Worker {worker_id[:8]}... recovered",
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            ),
        )

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

    async def _log_worker_health_transition(
        self,
        worker_id: str,
        previous_state: str,
        new_state: str,
    ) -> None:
        is_degradation = self._is_health_degradation(previous_state, new_state)

        if is_degradation:
            await self._logger.log(
                ServerWarning(
                    message=f"Worker {worker_id[:8]}... health degraded: {previous_state} -> {new_state}",
                    node_host=self._config.host,
                    node_port=self._config.tcp_port,
                    node_id=self._node_id,
                ),
            )
        else:
            await self._logger.log(
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

    async def _check_aggregate_health_alerts(self) -> None:
        counts = self._registry.get_worker_health_state_counts()
        total_workers = sum(counts.values())

        if total_workers == 0:
            return

        if (message := self._aggregate_health_alert_message(counts, total_workers)) is not None:
            await self._logger.log(
                ServerWarning(
                    message=message,
                    node_host=self._config.host,
                    node_port=self._config.tcp_port,
                    node_id=self._node_id,
                ),
            )

    def _aggregate_health_alert_message(
        self,
        counts: dict[str, int],
        total_workers: int,
    ) -> str | None:
        """The worker-tier alert to raise, if any: all non-healthy first, then the ratio alerts."""
        overloaded_count = counts.get("overloaded", 0)
        stressed_count = counts.get("stressed", 0)
        busy_count = counts.get("busy", 0)
        healthy_count = counts.get("healthy", 0)

        if healthy_count == 0 and total_workers > 0:
            return f"ALERT: All {total_workers} workers in non-healthy state (overloaded={overloaded_count}, stressed={stressed_count}, busy={busy_count})"

        return self._worker_ratio_alert_message(
            overloaded_count, stressed_count, busy_count, total_workers
        )

    def _worker_ratio_alert_message(
        self,
        overloaded_count: int,
        stressed_count: int,
        busy_count: int,
        total_workers: int,
    ) -> str | None:
        """The majority-overloaded or high-stress alert when its configured ratio is reached."""
        overloaded_ratio = overloaded_count / total_workers
        non_healthy_ratio = (
            overloaded_count + stressed_count + busy_count
        ) / total_workers

        overloaded_threshold = self._config.health_alert_overloaded_ratio
        non_healthy_threshold = self._config.health_alert_non_healthy_ratio

        if overloaded_ratio >= overloaded_threshold:
            return f"ALERT: Majority workers overloaded ({overloaded_count}/{total_workers} = {overloaded_ratio:.0%})"
        if non_healthy_ratio >= non_healthy_threshold:
            return f"ALERT: High worker stress ({non_healthy_ratio:.0%} non-healthy: overloaded={overloaded_count}, stressed={stressed_count}, busy={busy_count})"
        return None

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

        elapsed = _DEFAULT_CLOCK.monotonic() - last_progress
        return elapsed < self._config.job_responsiveness_threshold_seconds

    def record_job_progress(self, job_id: str, worker_id: str) -> None:
        """
        Record job progress from worker (AD-30).

        Args:
            job_id: Job ID
            worker_id: Worker ID
        """
        key = (job_id, worker_id)
        self._state._worker_job_last_progress[key] = _DEFAULT_CLOCK.monotonic()

    def cleanup_job_progress(self, job_id: str) -> None:
        """
        Cleanup progress tracking for a job.

        Args:
            job_id: Job ID to cleanup
        """
        keys_to_remove = self._worker_job_progress_keys(job_id)
        for key in keys_to_remove:
            self._state._worker_job_last_progress.pop(key, None)

    def _worker_job_progress_keys(self, job_id: str) -> list[tuple[str, str]]:
        """The (job_id, worker_id) progress keys recorded for ``job_id``."""
        return [
            key for key in self._state._worker_job_last_progress if key[0] == job_id
        ]

    # ========== AD-30: Job Suspicion Management ==========

    async def suspect_job(
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
        async with self._health_state_lock:
            if key in self._job_suspicions:
                return  # Already suspected

            timeout = timeout_seconds or self._config.job_responsiveness_threshold_seconds
            self._job_suspicions[key] = JobSuspicion(
                job_id=job_id,
                worker_id=worker_id,
                timeout_seconds=timeout,
            )

        await self._logger.log(
            ServerWarning(
                message=f"Job {job_id[:8]}... suspecting worker {worker_id[:8]}...",
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            ),
        )

    async def confirm_job_suspicion(self, job_id: str, worker_id: str) -> None:
        """
        Add confirmation to job suspicion (does NOT reschedule per AD-30).

        Args:
            job_id: Job ID
            worker_id: Suspected worker
        """
        key = (job_id, worker_id)
        async with self._health_state_lock:
            if suspicion := self._job_suspicions.get(key):
                suspicion.add_confirmation()

    async def refute_job_suspicion(self, job_id: str, worker_id: str) -> None:
        """
        Refute job suspicion (worker proved responsive).

        Args:
            job_id: Job ID
            worker_id: Worker to clear suspicion for
        """
        key = (job_id, worker_id)
        cleared = False
        async with self._health_state_lock:
            if key in self._job_suspicions:
                del self._job_suspicions[key]
                cleared = True

        if cleared:
            await self._logger.log(
                ServerDebug(
                    message=f"Cleared job {job_id[:8]}... suspicion for worker {worker_id[:8]}...",
                    node_host=self._config.host,
                    node_port=self._config.tcp_port,
                    node_id=self._node_id,
                ),
            )

    async def check_job_suspicion_expiry(self) -> list[tuple[str, str]]:
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

                # Remove suspicion, and the pair's progress: the worker's
                # workflows for the job are reassigned, so it no longer
                # owes the job progress (a report re-creates the entry).
                del self._job_suspicions[key]
                self._state._worker_job_last_progress.pop(key, None)

                await self._logger.log(
                    ServerWarning(
                        message=f"Worker {worker_id[:8]}... declared dead for job {job_id[:8]}... (suspicion expired)",
                        node_host=self._config.host,
                        node_port=self._config.tcp_port,
                        node_id=self._node_id,
                    ),
                )

        return expired

    def find_silent_worker_jobs(
        self,
        threshold_seconds: float,
    ) -> list[tuple[str, str]]:
        """Return ``(job_id, worker_id)`` pairs whose last progress is past threshold.

        AD-30 extension: this surfaces the job-layer's silence signal so the
        responsiveness loop can promote it into a job-suspicion. The current
        progress timestamps (kept by :meth:`record_job_progress`) are the
        canonical "worker is talking about job X" signal; AD-30 designed the
        two-layer detector around exactly this kind of cross-layer evidence
        but the wiring was incomplete — :meth:`suspect_job` had no caller in
        the manager. This method closes that gap by exposing the silent
        pairs to the manager loop in a single lookup.

        Excludes pairs that are already suspected; a pair declared
        job-dead has no progress entry left, so the loop is idempotent
        across ticks.
        """
        now = _DEFAULT_CLOCK.monotonic()
        silent: list[tuple[str, str]] = [
            key
            for key, last_progress in self._state._worker_job_last_progress.items()
            if self._is_silent_worker_job(key, last_progress, now, threshold_seconds)
        ]
        return silent

    def _is_silent_worker_job(
        self,
        key: tuple[str, str],
        last_progress: float,
        now: float,
        threshold_seconds: float,
    ) -> bool:
        """An unsuspected (job, worker) pair whose last progress is at least the threshold old (AD-30)."""
        return key not in self._job_suspicions and now - last_progress >= threshold_seconds

    def clear_job_suspicions(self, job_id: str) -> None:
        keys_to_remove = self._job_suspicion_keys(job_id)
        for key in keys_to_remove:
            del self._job_suspicions[key]

    def _job_suspicion_keys(self, job_id: str) -> list[tuple[str, str]]:
        """The suspected (job_id, worker_id) pairs belonging to ``job_id``."""
        return [key for key in self._job_suspicions if key[0] == job_id]

    def _count_peer_manager_health_states(
        self,
        health_states: dict[str, str],
    ) -> dict[str, int]:
        counts = {"healthy": 0, "busy": 0, "stressed": 0, "overloaded": 0}

        for health_state in health_states.values():
            if health_state in counts:
                counts[health_state] += 1
            else:
                counts["healthy"] += 1

        return counts

    async def get_peer_manager_health_counts(self) -> dict[str, int]:
        health_states = await self._state.get_peer_manager_health_states()
        return self._count_peer_manager_health_states(health_states)

    async def check_peer_manager_health_alerts(self) -> None:
        health_states = await self._state.get_peer_manager_health_states()
        counts = self._count_peer_manager_health_states(health_states)
        total_peers = sum(counts.values())

        if total_peers == 0:
            return

        if await self._alert_if_leader_overloaded(health_states):
            return

        await self._fire_peer_manager_ratio_alert(counts, total_peers)

    async def _alert_if_leader_overloaded(self, health_states: dict[str, str]) -> bool:
        """Fire the DC-leader overload alert when the leader is overloaded; True when fired."""
        dc_leader_id = self._state._dc_leader_manager_id
        leader_state = health_states.get(dc_leader_id) if dc_leader_id else None
        if leader_state == "overloaded":
            await self._fire_leader_overload_alert(dc_leader_id)
            return True
        return False

    async def _fire_peer_manager_ratio_alert(
        self,
        counts: dict[str, int],
        total_peers: int,
    ) -> None:
        """Fire the all-unhealthy alert, else the majority-overloaded or high-stress alert."""
        overloaded_count = counts.get("overloaded", 0)
        healthy_count = counts.get("healthy", 0)
        non_healthy_count = total_peers - healthy_count

        if healthy_count == 0:
            await self._fire_all_managers_unhealthy_alert(counts, total_peers)
            return

        await self._fire_peer_manager_overload_alert(
            counts, overloaded_count, non_healthy_count, total_peers
        )

    async def _fire_peer_manager_overload_alert(
        self,
        counts: dict[str, int],
        overloaded_count: int,
        non_healthy_count: int,
        total_peers: int,
    ) -> None:
        """Majority overloaded (>= 50%) wins over high stress (>= 80% non-healthy)."""
        if overloaded_count / total_peers >= 0.5:
            await self._fire_majority_overloaded_alert(overloaded_count, total_peers)
        elif non_healthy_count / total_peers >= 0.8:
            await self._fire_high_stress_alert(counts, total_peers)

    async def _fire_leader_overload_alert(self, leader_id: str) -> None:
        await self._logger.log(
            ServerWarning(
                message=f"ALERT: DC leader {leader_id[:8]}... overloaded - control plane saturated",
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            ),
        )

    async def _fire_all_managers_unhealthy_alert(
        self,
        counts: dict[str, int],
        total_peers: int,
    ) -> None:
        await self._logger.log(
            ServerWarning(
                message=f"CRITICAL: All {total_peers} DC managers non-healthy (overloaded={counts['overloaded']}, stressed={counts['stressed']}, busy={counts['busy']})",
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            ),
        )

    async def _fire_majority_overloaded_alert(
        self,
        overloaded_count: int,
        total_peers: int,
    ) -> None:
        await self._logger.log(
            ServerWarning(
                message=f"ALERT: Majority DC managers overloaded ({overloaded_count}/{total_peers})",
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            ),
        )

    async def _fire_high_stress_alert(
        self,
        counts: dict[str, int],
        total_peers: int,
    ) -> None:
        non_healthy = total_peers - counts["healthy"]
        ratio = non_healthy / total_peers
        await self._logger.log(
            ServerWarning(
                message=f"WARNING: DC control plane stressed ({ratio:.0%} non-healthy: overloaded={counts['overloaded']}, stressed={counts['stressed']}, busy={counts['busy']})",
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            ),
        )

    def get_health_metrics(self) -> ManagerHealthMetrics:
        """Get health-related metrics."""
        return {
            "healthy_workers": self.get_healthy_worker_count(),
            "unhealthy_workers": self.get_unhealthy_worker_count(),
            "total_workers": len(self._state._workers),
            "tracked_latency_targets": (
                len(self._state._worker_latency_samples)
                + len(self._state._peer_manager_latency_samples)
            ),
            # AD-30 job-layer metrics
            "job_suspicions": len(self._job_suspicions),
        }
