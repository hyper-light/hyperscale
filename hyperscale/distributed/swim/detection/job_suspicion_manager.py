"""
Job-layer suspicion manager with adaptive polling for per-job failure detection.

This implements the fine-grained, per-job layer of hierarchical failure detection.
Unlike the global timing wheel, this uses adaptive polling timers that become
more precise as expiration approaches.

Key features:
- Per-job suspicion tracking (node can be suspected for job A but not job B)
- Adaptive poll intervals based on time remaining
- LHM-aware polling (back off when under load)
- No task creation/cancellation on confirmation (state update only)
"""

import asyncio
import math
import time
from dataclasses import dataclass, field
from typing import Callable

from hyperscale.distributed.swim.core.protocols import LoggerProtocol

from .suspicion_state import SuspicionState


# Type aliases
NodeAddress = tuple[str, int]
JobId = str


@dataclass
class JobSuspicionConfig:
    """Configuration for job-layer suspicion management."""

    # Adaptive polling intervals (ms)
    poll_interval_far_ms: int = 1000  # > 5s remaining
    poll_interval_medium_ms: int = 250  # 1-5s remaining
    poll_interval_near_ms: int = 50  # < 1s remaining

    # Thresholds for interval selection (seconds)
    far_threshold_s: float = 5.0
    near_threshold_s: float = 1.0

    # LHM integration
    max_lhm_backoff_multiplier: float = 3.0  # Max slowdown under load

    # Resource limits
    max_suspicions_per_job: int = 1000
    max_total_suspicions: int = 10000


@dataclass(slots=True)
class JobSuspicion:
    """
    Suspicion state for a specific node within a specific job.

    Tracks the suspicion independently of global node status.
    """

    job_id: JobId
    node: NodeAddress
    incarnation: int
    start_time: float
    min_timeout: float
    max_timeout: float
    confirmers: set[NodeAddress] = field(default_factory=set)
    _logical_confirmation_count: int = 0

    # Timer management
    _poll_task: asyncio.Task | None = field(default=None, repr=False)
    _cancelled: bool = False

    def add_confirmation(self, from_node: NodeAddress) -> bool:
        """Add a confirmation from another node. Returns True if new."""
        if from_node in self.confirmers:
            return False

        self._logical_confirmation_count += 1
        if len(self.confirmers) < 1000:  # Bound memory
            self.confirmers.add(from_node)
        return True

    @property
    def confirmation_count(self) -> int:
        """Number of independent confirmations."""
        return max(len(self.confirmers), self._logical_confirmation_count)

    def calculate_timeout(self, n_members: int) -> float:
        """
        Calculate timeout using Lifeguard formula.

        timeout = max(min, max - (max - min) * log(C+1) / log(N+1))
        """
        c = self.confirmation_count
        n = max(1, n_members)

        if n <= 1:
            return self.max_timeout

        log_factor = math.log(c + 1) / math.log(n + 1)
        timeout = self.max_timeout - (self.max_timeout - self.min_timeout) * log_factor

        return max(self.min_timeout, timeout)

    def time_remaining(self, n_members: int) -> float:
        """Calculate time remaining before expiration."""
        elapsed = time.monotonic() - self.start_time
        timeout = self.calculate_timeout(n_members)
        return max(0, timeout - elapsed)

    def cancel(self) -> None:
        """Cancel this suspicion's timer."""
        self._cancelled = True
        if self._poll_task and not self._poll_task.done():
            self._poll_task.cancel()

    def cleanup(self) -> None:
        """Clean up resources."""
        self.cancel()
        self.confirmers.clear()


class JobSuspicionManager:
    """
    Manages per-job suspicions with adaptive polling timers.

    Unlike global suspicion which asks "is this machine alive?", job suspicion
    asks "is this node participating in this specific job?". A node under heavy
    load for job A might be slow/suspected for that job but fine for job B.

    Architecture:
    - Each (job_id, node) pair has independent suspicion state
    - Single polling task per suspicion (no cancel/reschedule on confirmation)
    - Confirmations update state only; timer naturally picks up changes
    - Poll interval adapts: frequent near expiration, relaxed when far
    - LHM can slow polling when we're under load (reduce self-induced pressure)
    """

    def __init__(
        self,
        config: JobSuspicionConfig | None = None,
        on_expired: Callable[[JobId, NodeAddress, int], None] | None = None,
        on_error: Callable[[str, Exception], None] | None = None,
        get_n_members: Callable[[JobId], int] | None = None,
        get_lhm_multiplier: Callable[[], float] | None = None,
    ) -> None:
        if config is None:
            config = JobSuspicionConfig()

        self._config = config
        self._on_expired = on_expired
        self._on_error = on_error
        self._get_n_members = get_n_members
        self._get_lhm_multiplier = get_lhm_multiplier

        # Suspicions indexed by (job_id, node)
        self._suspicions: dict[tuple[JobId, NodeAddress], JobSuspicion] = {}

        # Per-job suspicion counts for limits
        self._per_job_counts: dict[JobId, int] = {}

        # Lock for structural modifications
        self._lock = asyncio.Lock()

        # Running state
        self._running: bool = True

        # Stats
        self._started_count: int = 0
        self._expired_count: int = 0
        self._refuted_count: int = 0
        self._confirmed_count: int = 0

        # Logging
        self._logger: LoggerProtocol | None = None
        self._node_host: str = ""
        self._node_port: int = 0
        self._node_id: str = ""

    def set_logger(
        self,
        logger: LoggerProtocol,
        node_host: str,
        node_port: int,
        node_id: str,
    ) -> None:
        self._logger = logger
        self._node_host = node_host
        self._node_port = node_port
        self._node_id = node_id

    async def _log_error(self, message: str) -> None:
        if self._logger:
            from hyperscale.logging.hyperscale_logging_models import ServerError

            await self._logger.log(
                ServerError(
                    message=message,
                    node_host=self._node_host,
                    node_port=self._node_port,
                    node_id=self._node_id,
                )
            )

    def _get_n_members_for_job(self, job_id: JobId) -> int:
        if self._get_n_members:
            return self._get_n_members(job_id)
        return 1

    def _get_current_lhm(self) -> float:
        """Get current Local Health Multiplier."""
        if self._get_lhm_multiplier:
            return self._get_lhm_multiplier()
        return 1.0

    def _calculate_poll_interval(self, remaining: float) -> float:
        """
        Calculate adaptive poll interval based on time remaining.

        Returns interval in seconds, adjusted for LHM.
        """
        lhm = min(self._get_current_lhm(), self._config.max_lhm_backoff_multiplier)

        if remaining > self._config.far_threshold_s:
            base_interval = self._config.poll_interval_far_ms / 1000.0
        elif remaining > self._config.near_threshold_s:
            base_interval = self._config.poll_interval_medium_ms / 1000.0
        else:
            base_interval = self._config.poll_interval_near_ms / 1000.0

        # Apply LHM - when under load, poll less frequently
        return base_interval * lhm

    async def start_suspicion(
        self,
        job_id: JobId,
        node: NodeAddress,
        incarnation: int,
        from_node: NodeAddress,
        min_timeout: float = 1.0,
        max_timeout: float = 10.0,
    ) -> JobSuspicion | None:
        """
        Start or update a suspicion for a node in a specific job.

        Returns None if:
        - Max suspicions reached
        - Stale incarnation (older than existing)

        Returns the suspicion state if created or updated.
        """
        async with self._lock:
            key = (job_id, node)
            existing = self._suspicions.get(key)

            if existing:
                if incarnation < existing.incarnation:
                    # Stale suspicion, ignore
                    return existing
                elif incarnation == existing.incarnation:
                    # Same suspicion, add confirmation
                    if existing.add_confirmation(from_node):
                        self._confirmed_count += 1
                    # Timer will pick up new confirmation count
                    return existing
                else:
                    # Higher incarnation, replace
                    existing.cancel()
                    self._per_job_counts[job_id] = (
                        self._per_job_counts.get(job_id, 1) - 1
                    )
            else:
                # Check limits
                job_count = self._per_job_counts.get(job_id, 0)
                if job_count >= self._config.max_suspicions_per_job:
                    return None
                if len(self._suspicions) >= self._config.max_total_suspicions:
                    return None

            # Create new suspicion
            suspicion = JobSuspicion(
                job_id=job_id,
                node=node,
                incarnation=incarnation,
                start_time=time.monotonic(),
                min_timeout=min_timeout,
                max_timeout=max_timeout,
            )
            suspicion.add_confirmation(from_node)

            self._suspicions[key] = suspicion
            self._per_job_counts[job_id] = self._per_job_counts.get(job_id, 0) + 1
            self._started_count += 1

            # Start adaptive polling timer
            suspicion._poll_task = asyncio.create_task(self._poll_suspicion(suspicion))

            return suspicion

    async def _poll_suspicion(self, suspicion: JobSuspicion) -> None:
        """
        Adaptive polling loop for a suspicion.

        Checks time_remaining() and either:
        - Expires the suspicion if time is up
        - Sleeps for an adaptive interval and checks again

        Confirmations update state; this loop naturally picks up changes.
        """
        job_id = suspicion.job_id
        node = suspicion.node

        try:
            while not suspicion._cancelled and self._running:
                n_members = self._get_n_members_for_job(job_id)
                remaining = suspicion.time_remaining(n_members)

                if remaining <= 0:
                    # Expired - handle expiration
                    await self._handle_expiration(suspicion)
                    return

                # Calculate adaptive sleep interval
                poll_interval = self._calculate_poll_interval(remaining)
                # Don't sleep longer than remaining time
                sleep_time = min(poll_interval, remaining)

                await asyncio.sleep(sleep_time)

        except asyncio.CancelledError:
            await self._log_error(
                f"Suspicion timer cancelled for job {suspicion.job_id}, node {suspicion.node}"
            )

    async def _handle_expiration(self, suspicion: JobSuspicion) -> None:
        """Handle suspicion expiration - declare node dead for this job."""
        key = (suspicion.job_id, suspicion.node)

        async with self._lock:
            # Double-check still exists (may have been refuted)
            if key not in self._suspicions:
                return

            current = self._suspicions.get(key)
            if current is not suspicion:
                # Different suspicion now (race)
                return

            # Remove from tracking
            del self._suspicions[key]
            self._per_job_counts[suspicion.job_id] = max(
                0, self._per_job_counts.get(suspicion.job_id, 1) - 1
            )
            self._expired_count += 1

        # Call callback outside lock
        if self._on_expired:
            try:
                self._on_expired(
                    suspicion.job_id, suspicion.node, suspicion.incarnation
                )
            except Exception as callback_error:
                if self._on_error:
                    try:
                        self._on_error(
                            f"on_expired callback failed for job {suspicion.job_id}, node {suspicion.node}",
                            callback_error,
                        )
                    except Exception as error_callback_error:
                        await self._log_error(
                            f"on_error callback failed: {error_callback_error}, original: {callback_error}"
                        )
                else:
                    await self._log_error(
                        f"on_expired callback failed for job {suspicion.job_id}, node {suspicion.node}: {callback_error}"
                    )

    async def confirm_suspicion(
        self,
        job_id: JobId,
        node: NodeAddress,
        incarnation: int,
        from_node: NodeAddress,
    ) -> bool:
        """
        Add confirmation to existing suspicion.

        Returns True if confirmation was added.
        No timer rescheduling - poll loop picks up new state.
        """
        async with self._lock:
            key = (job_id, node)
            suspicion = self._suspicions.get(key)

            if suspicion and suspicion.incarnation == incarnation:
                if suspicion.add_confirmation(from_node):
                    self._confirmed_count += 1
                    return True
            return False

    async def refute_suspicion(
        self,
        job_id: JobId,
        node: NodeAddress,
        incarnation: int,
    ) -> bool:
        """
        Refute a suspicion (node proved alive with higher incarnation).

        Returns True if suspicion was cleared.
        """
        async with self._lock:
            key = (job_id, node)
            suspicion = self._suspicions.get(key)

            if suspicion and incarnation > suspicion.incarnation:
                suspicion.cancel()
                del self._suspicions[key]
                self._per_job_counts[job_id] = max(
                    0, self._per_job_counts.get(job_id, 1) - 1
                )
                self._refuted_count += 1
                return True
            return False

    async def clear_job(self, job_id: JobId) -> int:
        """
        Clear all suspicions for a job (e.g., job completed).

        Returns number of suspicions cleared.
        """
        async with self._lock:
            to_remove: list[tuple[JobId, NodeAddress]] = []

            for key, suspicion in self._suspicions.items():
                if key[0] == job_id:
                    suspicion.cancel()
                    to_remove.append(key)

            for key in to_remove:
                del self._suspicions[key]

            self._per_job_counts[job_id] = 0
            return len(to_remove)

    async def clear_all(self) -> None:
        """Clear all suspicions (e.g., shutdown)."""
        async with self._lock:
            for suspicion in self._suspicions.values():
                suspicion.cancel()
            self._suspicions.clear()
            self._per_job_counts.clear()

    def is_suspected(self, job_id: JobId, node: NodeAddress) -> bool:
        """Check if a node is suspected for a specific job."""
        return (job_id, node) in self._suspicions

    def get_suspicion(
        self,
        job_id: JobId,
        node: NodeAddress,
    ) -> JobSuspicion | None:
        """Get suspicion state for a node in a job."""
        return self._suspicions.get((job_id, node))

    def get_suspected_nodes(self, job_id: JobId) -> list[NodeAddress]:
        """Get all suspected nodes for a job."""
        return [key[1] for key in self._suspicions.keys() if key[0] == job_id]

    def get_jobs_suspecting(self, node: NodeAddress) -> list[JobId]:
        """Get all jobs that have this node suspected."""
        return [key[0] for key in self._suspicions.keys() if key[1] == node]

    async def shutdown(self) -> None:
        """Shutdown the manager and cancel all timers."""
        self._running = False
        await self.clear_all()

    def get_stats(self) -> dict[str, int]:
        """Get manager statistics."""
        return {
            "active_suspicions": len(self._suspicions),
            "jobs_with_suspicions": len(
                [c for c in self._per_job_counts.values() if c > 0]
            ),
            "started_count": self._started_count,
            "expired_count": self._expired_count,
            "refuted_count": self._refuted_count,
            "confirmed_count": self._confirmed_count,
        }

    def get_job_stats(self, job_id: JobId) -> dict[str, int]:
        """Get statistics for a specific job."""
        count = self._per_job_counts.get(job_id, 0)
        suspected = self.get_suspected_nodes(job_id)
        return {
            "suspicion_count": count,
            "suspected_nodes": len(suspected),
        }
