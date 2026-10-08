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

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

import asyncio
import math
from dataclasses import dataclass, field
from itertools import compress, repeat
from operator import eq, itemgetter
from typing import Callable
from hyperscale.distributed.protocol.time_quantum import TIME_REMAINDER_EPSILON_SECONDS
from hyperscale.distributed.runtime import Clock, RealClock
from hyperscale.distributed.swim.core.protocols import LoggerProtocol
from hyperscale.logging.hyperscale_logging_models import ServerError

from .suspicion_state import SuspicionState
from .job_suspicion_manager_shared import _DEFAULT_CLOCK
from .job_suspicion_manager_shared import NodeAddress
from .job_suspicion_manager_shared import JobId
from .job_suspicion import JobSuspicion
from .job_suspicion_config import JobSuspicionConfig


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

            preempted, preempting_result = self._preempt_new_suspicion(
                job_id, existing, incarnation, from_node
            )
            if preempted:
                return preempting_result

            # Create new suspicion
            suspicion = JobSuspicion(
                job_id=job_id,
                node=node,
                incarnation=incarnation,
                start_time=_DEFAULT_CLOCK.monotonic(),
                min_timeout=min_timeout,
                max_timeout=max_timeout,
                originator=from_node,
            )
            # Originator's vote is implicit (see the field docstring).
            suspicion.add_confirmation(from_node)

            self._suspicions[key] = suspicion
            self._per_job_counts[job_id] = self._per_job_counts.get(job_id, 0) + 1
            self._started_count += 1

            # Start adaptive polling timer.
            # Phase 6b: explicit ``loop.create_task`` so the task binds
            # to the loop the suspicion was started on rather than
            # implicitly going through ``get_running_loop`` at task-
            # creation time.
            suspicion._poll_task = asyncio.get_running_loop().create_task(
                self._poll_suspicion(suspicion)
            )

            return suspicion

    def _preempt_new_suspicion(
        self,
        job_id: JobId,
        existing: JobSuspicion | None,
        incarnation: int,
        from_node: NodeAddress,
    ) -> tuple[bool, JobSuspicion | None]:
        """Decide whether start_suspicion returns early, and with what; the caller holds the lock."""
        if existing:
            return self._merge_existing_suspicion(job_id, existing, incarnation, from_node)
        # Check limits
        return self._at_suspicion_limit(job_id), None

    def _merge_existing_suspicion(
        self,
        job_id: JobId,
        existing: JobSuspicion,
        incarnation: int,
        from_node: NodeAddress,
    ) -> tuple[bool, JobSuspicion | None]:
        """Fold a suspicion into the existing one: keep it unless the incarnation is higher (then replace)."""
        if incarnation < existing.incarnation:
            # Stale suspicion, ignore
            return True, existing
        elif incarnation == existing.incarnation:
            # Same suspicion, add confirmation
            self._add_job_confirmation(existing, from_node)
            # Timer will pick up new confirmation count
            return True, existing
        # Higher incarnation, replace
        existing.cancel()
        self._per_job_counts[job_id] = (
            self._per_job_counts.get(job_id, 1) - 1
        )
        return False, None

    def _at_suspicion_limit(self, job_id: JobId) -> bool:
        """Whether the per-job or total suspicion limit refuses a new suspicion."""
        job_count = self._per_job_counts.get(job_id, 0)
        return (
            job_count >= self._config.max_suspicions_per_job
            or len(self._suspicions) >= self._config.max_total_suspicions
        )

    def _add_job_confirmation(self, suspicion: JobSuspicion, from_node: NodeAddress) -> bool:
        """Add ``from_node``'s confirmation, counting it; True when it was new."""
        if suspicion.add_confirmation(from_node):
            self._confirmed_count += 1
            return True
        return False

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
            await self._run_poll_loop(suspicion, job_id)

        except asyncio.CancelledError:
            await self._log_error(
                f"Suspicion timer cancelled for job {suspicion.job_id}, node {suspicion.node}"
            )

    async def _run_poll_loop(self, suspicion: JobSuspicion, job_id: JobId) -> None:
        """Poll ``suspicion`` at the adaptive interval until it expires, is cancelled, or the manager stops."""
        while self._is_polling(suspicion):
            n_members = self._get_n_members_for_job(job_id)
            remaining = suspicion.time_remaining(n_members)

            if remaining <= 0:
                # Expired - handle expiration
                await self._handle_expiration(suspicion)
                return

            # Calculate adaptive sleep interval
            poll_interval = self._calculate_poll_interval(remaining)
            # Don't sleep longer than remaining time — floored so
            # the clock always moves (defense in depth for the
            # frozen-instant class; the epsilon contract in
            # time_remaining is the primary guard).
            sleep_time = max(min(poll_interval, remaining), 0.001)

            await _DEFAULT_CLOCK.sleep(sleep_time)

    def _is_polling(self, suspicion: JobSuspicion) -> bool:
        """Whether the poll loop continues: the suspicion is live and the manager running."""
        return not suspicion._cancelled and self._running

    async def _handle_expiration(self, suspicion: JobSuspicion) -> None:
        """Handle suspicion expiration - declare node dead for this job."""
        key = (suspicion.job_id, suspicion.node)

        async with self._lock:
            # Double-check still exists (may have been refuted), and is
            # not a different suspicion now (race): a missing key reads
            # None, which is never ``suspicion``.
            if self._suspicions.get(key) is not suspicion:
                return

            # Remove from tracking
            del self._suspicions[key]
            self._per_job_counts[suspicion.job_id] = max(
                0, self._per_job_counts.get(suspicion.job_id, 1) - 1
            )
            self._expired_count += 1

        # Call callback outside lock
        await self._notify_expired(suspicion)

    async def _notify_expired(self, suspicion: JobSuspicion) -> None:
        """Invoke on_expired for ``suspicion``, reporting a callback failure."""
        if self._on_expired:
            try:
                self._on_expired(
                    suspicion.job_id, suspicion.node, suspicion.incarnation
                )
            except Exception as callback_error:
                await self._report_expired_callback_failure(suspicion, callback_error)

    async def _report_expired_callback_failure(
        self,
        suspicion: JobSuspicion,
        callback_error: Exception,
    ) -> None:
        """Route an on_expired failure to on_error, else the log; log an on_error failure too."""
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
                return self._add_job_confirmation(suspicion, from_node)
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
            # Keys and values iterate in the same order, so compress selects the job's suspicions.
            job_matches = list(map(eq, map(itemgetter(0), self._suspicions.keys()), repeat(job_id)))
            to_remove: list[tuple[JobId, NodeAddress]] = list(compress(self._suspicions.keys(), job_matches))

            for suspicion in compress(self._suspicions.values(), job_matches):
                suspicion.cancel()

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

_REHOMED = (
    JobSuspicionConfig,
    JobSuspicion,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
