"""
Gate orphan job coordinator for handling job takeover when gate peers fail.

This module implements the detection and takeover of orphaned jobs when a gate
peer becomes unavailable. The SWIM cluster leader is the only failover
authority; fencing tokens prevent stale leaders from reappearing.

Key responsibilities:
- Detect jobs orphaned by gate peer failures
- Execute SWIM-leader takeover with proper fencing token increment
- Broadcast leadership changes to peer gates and managers
- Prevent thundering herd via jitter and grace periods
"""

import asyncio
from typing import TYPE_CHECKING, Callable, Awaitable

from hyperscale.distributed.health.extension_tracker import ExtensionTracker
from hyperscale.distributed.models import (
    GlobalJobStatus,
    JobLeadershipAnnouncement,
    JobStatus,
    JobStatusPush,
)
from hyperscale.logging import Logger
from hyperscale.logging.hyperscale_logging_models import (
    ServerDebug,
    ServerInfo,
    ServerWarning,
)

from .models.orphan_job_stats import OrphanJobStats
from .state import GateRuntimeState

from hyperscale.distributed.runtime import Clock



if TYPE_CHECKING:
    from hyperscale.distributed.swim.core import NodeId
    from hyperscale.distributed.jobs.gates.consistent_hash_ring import (
        ConsistentHashRing,
    )
    from hyperscale.distributed.jobs import JobLeadershipTracker
    from hyperscale.distributed.jobs.gates import GateJobManager
    from hyperscale.distributed.leases import JobLease
    from hyperscale.distributed.taskex import TaskRunner


class GateOrphanJobCoordinator:
    """
    Coordinates detection and takeover of orphaned jobs when gate peers fail.

    When a gate peer becomes unavailable (detected via SWIM), this coordinator:
    1. Identifies all jobs that were led by the failed gate
    2. Marks those jobs as orphaned with timestamps
    3. Periodically scans orphaned jobs after a grace period
    4. Lets only the SWIM cluster leader commit takeover
    5. Broadcasts leadership changes to maintain cluster consistency

    The grace period prevents premature takeover during transient network issues
    and allows membership to stabilize after node removal.

    Asyncio Safety:
    - Uses internal lock for orphan state modifications
    - Coordinates with JobLeadershipTracker's async methods
    - Background loop runs via TaskRunner for proper lifecycle management
    """

    CALLBACK_PUSH_MAX_RETRIES: int = 3
    CALLBACK_PUSH_BASE_DELAY_SECONDS: float = 0.5
    CALLBACK_PUSH_MAX_DELAY_SECONDS: float = 2.0

    __slots__ = (
        "_clock",
        "_state",
        "_finalize_failed_job",
        "_logger",
        "_task_runner",
        "_job_hash_ring",
        "_job_leadership_tracker",
        "_job_manager",
        "_get_node_id",
        "_get_node_addr",
        "_send_tcp",
        "_get_active_peers",
        "_forward_status_push_to_peers",
        "_state_repair_callback",
        "_commit_takeover_callback",
        "_is_cluster_leader",
        "_orphan_check_interval_seconds",
        "_orphan_grace_period_seconds",
        "_orphan_extension_min_grant_seconds",
        "_orphan_extension_max_extensions",
        "_orphan_extensions",
        "_orphan_due_at",
        "_orphan_due_heartbeats",
        "_takeover_extensions",
        "_takeover_jitter_min_seconds",
        "_takeover_jitter_max_seconds",
        "_confirmed_orphaned_jobs",
        "_running",
        "_check_loop_task",
        "_lock",
        "_terminal_statuses",
    )

    def __init__(
        self,
        state: GateRuntimeState,
        logger: Logger,
        task_runner: "TaskRunner",
        job_hash_ring: "ConsistentHashRing",
        job_leadership_tracker: "JobLeadershipTracker",
        job_manager: "GateJobManager",
        get_node_id: Callable[[], "NodeId"],
        get_node_addr: Callable[[], tuple[str, int]],
        send_tcp: Callable[[tuple[str, int], str, bytes, float], Awaitable[bytes]],
        get_active_peers: Callable[[], set[tuple[str, int]]],
        clock: Clock,
        forward_status_push_to_peers: Callable[[str, bytes], Awaitable[bool]]
        | None = None,
        state_repair_callback: Callable[[str], Awaitable[bool]] | None = None,
        commit_takeover_callback: Callable[[str], Awaitable[int | None]] | None = None,
        is_cluster_leader: Callable[[], bool] | None = None,
        orphan_check_interval_seconds: float = 15.0,
        orphan_grace_period_seconds: float = 30.0,
        orphan_extension_min_grant_seconds: float = 1.0,
        orphan_extension_max_extensions: int = 5,
        takeover_jitter_min_seconds: float = 0.5,
        takeover_jitter_max_seconds: float = 2.0,
        *,
        finalize_failed_job: Callable[[str, tuple[str, ...], str], Awaitable[None]],
    ) -> None:
        """
        Initialize the orphan job coordinator.

        Args:
            state: Runtime state container with orphan tracking
            logger: Async logger instance
            task_runner: Background task executor
            job_hash_ring: Existing job hash ring retained for coordinator compatibility
            job_leadership_tracker: Tracks per-job leadership with fencing tokens
            job_manager: Manages job state and target datacenters
            get_node_id: Callback to get this gate's node ID
            get_node_addr: Callback to get this gate's TCP address
            send_tcp: Callback to send TCP messages to peers
            get_active_peers: Callback to get active peer gate addresses
            forward_status_push_to_peers: Callback to forward status pushes to peer gates
            state_repair_callback: Callback to hydrate missing committed job state
            commit_takeover_callback: Callback that quorum-commits a takeover
            is_cluster_leader: Callback returning whether this gate is SWIM leader
            orphan_check_interval_seconds: How often to scan for orphaned jobs
            orphan_grace_period_seconds: Time to wait before attempting
                takeover (derived: gate/config.py
                derive_gate_orphan_grace_seconds); raised by the longest
                rescue seen, and extended per AD-26 while the job's leader
                is still heard from
            orphan_extension_min_grant_seconds: Smallest AD-26 extension
            orphan_extension_max_extensions: Most AD-26 extensions per orphan
                A due orphan the tier has not taken over gets one more such
                window for the tier to produce a leader that can -- raised by
                the longest takeover seen after a job came due, and extended
                per AD-26 while peer gates are still heard from -- before it
                fails
            takeover_jitter_min_seconds: Minimum random jitter before takeover
            takeover_jitter_max_seconds: Maximum random jitter before takeover
        """
        self._clock: Clock = clock
        self._state = state
        self._finalize_failed_job = finalize_failed_job
        self._logger = logger
        self._task_runner = task_runner
        self._job_hash_ring = job_hash_ring
        self._job_leadership_tracker = job_leadership_tracker
        self._job_manager = job_manager
        self._get_node_id = get_node_id
        self._get_node_addr = get_node_addr
        self._send_tcp = send_tcp
        self._get_active_peers = get_active_peers
        self._forward_status_push_to_peers = forward_status_push_to_peers
        self._state_repair_callback = state_repair_callback
        self._commit_takeover_callback = commit_takeover_callback
        self._is_cluster_leader = is_cluster_leader
        self._orphan_check_interval_seconds = orphan_check_interval_seconds
        self._orphan_grace_period_seconds = orphan_grace_period_seconds
        self._orphan_extension_min_grant_seconds = orphan_extension_min_grant_seconds
        self._orphan_extension_max_extensions = orphan_extension_max_extensions
        # The AD-26 extensions each orphaned job was granted.
        self._orphan_extensions: dict[str, ExtensionTracker] = {}
        # When each orphan came due for takeover, and the tier's heartbeats
        # by then; the AD-26 extensions of its takeover window.
        self._orphan_due_at: dict[str, float] = {}
        self._orphan_due_heartbeats: dict[str, int] = {}
        self._takeover_extensions: dict[str, ExtensionTracker] = {}
        self._takeover_jitter_min_seconds = takeover_jitter_min_seconds
        self._takeover_jitter_max_seconds = takeover_jitter_max_seconds
        self._confirmed_orphaned_jobs: set[str] = set()
        self._running = False
        self._check_loop_task: asyncio.Task | None = None
        self._lock = asyncio.Lock()
        self._terminal_statuses = {
            JobStatus.COMPLETED.value,
            JobStatus.FAILED.value,
            JobStatus.CANCELLED.value,
            JobStatus.TIMEOUT.value,
        }

    async def start(self) -> None:
        """Start the orphan job check loop."""
        if self._running:
            return

        self._running = True
        # Phase 6b: explicit ``loop.create_task`` so the task binds to the
        # loop ``start`` was called from rather than implicitly going
        # through ``get_running_loop`` at task-creation time.
        self._check_loop_task = asyncio.get_running_loop().create_task(
            self._orphan_check_loop()
        )

        await self._logger.log(
            ServerInfo(
                message=f"Orphan job coordinator started (check_interval={self._orphan_check_interval_seconds}s, "
                f"grace_period={self._orphan_grace_period_seconds}s)",
                node_host=self._get_node_addr()[0],
                node_port=self._get_node_addr()[1],
                node_id=self._get_node_id().short,
            )
        )

    async def stop(self) -> None:
        """Stop the orphan job check loop."""
        self._running = False

        if self._check_loop_task and not self._check_loop_task.done():
            await self._cancel_check_loop()

        self._check_loop_task = None

    async def _cancel_check_loop(self) -> None:
        """Cancel the check loop and wait for it to end, passing on a cancel
        aimed at this task while it waited."""
        self._check_loop_task.cancel()
        cancels_requested_before_wait = asyncio.current_task().cancelling()
        try:
            await self._check_loop_task
        except asyncio.CancelledError:
            # The task we cancelled ended; a cancel aimed at this task
            # while it waited goes on.
            if asyncio.current_task().cancelling() > cancels_requested_before_wait:
                raise

    def mark_jobs_orphaned_by_gate(
        self,
        failed_gate_addr: tuple[str, int],
    ) -> list[str]:
        """
        Mark all jobs led by a failed gate as orphaned.

        Called when a gate peer failure is detected via SWIM. This method
        identifies all jobs that were led by the failed gate and marks them
        as orphaned with the current timestamp.

        Args:
            failed_gate_addr: TCP address of the failed gate peer

        Returns:
            List of job IDs that were marked as orphaned
        """
        orphaned_job_ids = self._job_leadership_tracker.get_jobs_led_by_addr(
            failed_gate_addr
        )

        now = self._clock.monotonic()
        for job_id in orphaned_job_ids:
            self._state.mark_job_orphaned(job_id, now, failed_gate_addr)

        self._state.mark_leader_dead(failed_gate_addr)

        return orphaned_job_ids

    def mark_jobs_confirmed_orphaned_by_gate(
        self,
        failed_gate_addr: tuple[str, int],
    ) -> list[str]:
        """Mark jobs orphaned by a SWIM-confirmed dead gate.

        Confirmed SWIM death has already paid the failure-detection
        bracket. These jobs are eligible for immediate evaluation by the
        current SWIM leader; the periodic loop remains a reconciliation
        fallback for non-leaders and leadership changes.
        """
        orphaned_job_ids = self.mark_jobs_orphaned_by_gate(failed_gate_addr)
        self._confirmed_orphaned_jobs.update(orphaned_job_ids)

        if orphaned_job_ids and self._is_current_cluster_leader():
            self._task_runner.run(
                self.evaluate_confirmed_orphans,
                orphaned_job_ids,
            )

        return orphaned_job_ids

    async def evaluate_confirmed_orphans(
        self,
        job_ids: list[str] | None = None,
    ) -> None:
        """Immediately evaluate SWIM-confirmed orphaned jobs."""
        if not self._is_current_cluster_leader():
            return

        orphaned_jobs = self._state.get_orphaned_jobs()
        for job_id in self._confirmed_orphan_candidates(job_ids):
            await self._evaluate_confirmed_orphan(job_id, orphaned_jobs)

    def _confirmed_orphan_candidates(self, job_ids: list[str] | None) -> list[str]:
        """The jobs to evaluate: those given, else every confirmed orphan."""
        return (
            list(job_ids)
            if job_ids is not None
            else list(self._confirmed_orphaned_jobs)
        )

    async def _evaluate_confirmed_orphan(self, job_id: str, orphaned_jobs: dict[str, float]) -> None:
        """Evaluate a confirmed orphan still orphaned; forget one that is not."""
        orphaned_at = orphaned_jobs.get(job_id)
        if orphaned_at is None:
            self._confirmed_orphaned_jobs.discard(job_id)
            return
        await self._evaluate_orphan_takeover(job_id, orphaned_at)

    def clear_orphaned_job(self, job_id: str) -> None:
        """A peer resolved an orphan's leadership: a rescue, which the
        orphan grace learns from."""
        now = self._clock.monotonic()
        if (due_at := self._orphan_due_at.get(job_id)) is not None:
            self._state.record_orphan_takeover_wait(now - due_at)
        self._state.rescue_orphaned_job(job_id, now)
        self._confirmed_orphaned_jobs.discard(job_id)
        self._orphan_extensions.pop(job_id, None)
        self._orphan_due_at.pop(job_id, None)
        self._orphan_due_heartbeats.pop(job_id, None)
        self._takeover_extensions.pop(job_id, None)

    async def on_lease_expired(self, lease: "JobLease") -> None:
        """
        Handle expired job lease callback from LeaseManager.

        When a job lease expires without renewal, it indicates the owning
        gate may have failed. This marks the job as potentially orphaned
        for evaluation during the next check cycle.

        Args:
            lease: The expired job lease
        """
        job_id = lease.job_id
        owner_node = lease.owner_node

        if owner_node == self._get_node_id().full:
            return

        now = self._clock.monotonic()
        if not self._state.is_job_orphaned(job_id):
            self._state.mark_job_orphaned(
                job_id,
                now,
                self._known_gate_addr(owner_node),
            )

            await self._logger.log(
                ServerDebug(
                    message=f"Job {job_id[:8]}... lease expired (owner={owner_node[:8]}...), marked for orphan check",
                    node_host=self._get_node_addr()[0],
                    node_port=self._get_node_addr()[1],
                    node_id=self._get_node_id().short,
                ),
            )

    def _known_gate_addr(self, node_id: str) -> tuple[str, int] | None:
        """The TCP address of a gate this gate knows, else None."""
        owner_gate = self._state.get_known_gate(node_id)
        return (owner_gate.tcp_host, owner_gate.tcp_port) if owner_gate is not None else None

    async def _send_job_status_push_with_retry(
        self,
        job_id: str,
        callback: tuple[str, int],
        push_data: bytes,
        allow_peer_forwarding: bool = True,
    ) -> None:
        delivered, last_error = await self._push_to_callback(callback, push_data)
        if delivered:
            return

        delivered, last_error = await self._forward_push_to_peers(
            job_id, push_data, allow_peer_forwarding, last_error
        )
        if delivered:
            return

        await self._logger.log(
            ServerWarning(
                message=(
                    f"Failed to deliver orphan timeout status for job {job_id[:8]}... "
                    f"after {self.CALLBACK_PUSH_MAX_RETRIES} retries: {last_error}"
                ),
                node_host=self._get_node_addr()[0],
                node_port=self._get_node_addr()[1],
                node_id=self._get_node_id().short,
            )
        )

    async def _push_to_callback(
        self,
        callback: tuple[str, int],
        push_data: bytes,
    ) -> tuple[bool, Exception | None]:
        """Push to the client's callback, retrying with capped exponential
        backoff; whether it was delivered, and the last error."""
        last_error: Exception | None = None

        for attempt in range(self.CALLBACK_PUSH_MAX_RETRIES):
            if (send_error := await self._push_once(callback, push_data, attempt)) is None:
                return True, last_error
            last_error = send_error

        return False, last_error

    async def _push_once(
        self,
        callback: tuple[str, int],
        push_data: bytes,
        attempt: int,
    ) -> Exception | None:
        """One push attempt; the error it failed with (after backing off
        when attempts remain), or None once delivered."""
        try:
            response, _ = await self._send_tcp(
                callback,
                "job_status_push",
                push_data,
                5.0,
            )
            # send_tcp returns transport errors rather than raising.
            if isinstance(response, Exception):
                raise response
            return None
        except Exception as send_error:
            await self._back_off_push(attempt)
            return send_error

    async def _back_off_push(self, attempt: int) -> None:
        """Sleep before the next push attempt, when one remains."""
        if attempt < self.CALLBACK_PUSH_MAX_RETRIES - 1:
            delay = min(
                self.CALLBACK_PUSH_BASE_DELAY_SECONDS * (2**attempt),
                self.CALLBACK_PUSH_MAX_DELAY_SECONDS,
            )
            await self._clock.sleep(delay)

    async def _forward_push_to_peers(
        self,
        job_id: str,
        push_data: bytes,
        allow_peer_forwarding: bool,
        last_error: Exception | None,
    ) -> tuple[bool, Exception | None]:
        """Forward the push to peer gates, when allowed and possible;
        whether a peer delivered it, and the last error."""
        if not self._may_forward_push(allow_peer_forwarding):
            return False, last_error
        return await self._forward_push(job_id, push_data, last_error)

    def _may_forward_push(self, allow_peer_forwarding: bool) -> bool:
        """Whether the push may go through peer gates."""
        return allow_peer_forwarding and bool(self._forward_status_push_to_peers)

    async def _forward_push(
        self,
        job_id: str,
        push_data: bytes,
        last_error: Exception | None,
    ) -> tuple[bool, Exception | None]:
        """Ask peer gates to deliver the push; whether one did, and the last
        error."""
        try:
            forwarded = await self._forward_status_push_to_peers(job_id, push_data)
        except Exception as forward_error:
            return False, forward_error
        return bool(forwarded), last_error

    async def _orphan_check_loop(self) -> None:
        """
        Periodically check for orphaned jobs and attempt takeover.

        This loop runs at a configurable interval and:
        1. Gets all jobs marked as orphaned
        2. Filters to those past the grace period
        3. Checks if this gate should own each job (via hash ring)
        4. Executes takeover for jobs we should own
        """
        while self._running:
            if not await self._run_orphan_check_reporting_errors():
                break

    async def _run_orphan_check_reporting_errors(self) -> bool:
        """One check; an error it raised is logged and the loop goes on.
        False when the check was cancelled: the loop ends."""
        try:
            await self._run_orphan_check()

        except asyncio.CancelledError:
            return False
        except Exception as error:
            await self._logger.log(
                ServerWarning(
                    message=f"Orphan check loop error: {error}",
                    node_host=self._get_node_addr()[0],
                    node_port=self._get_node_addr()[1],
                    node_id=self._get_node_id().short,
                ),
            )
        return True

    async def _run_orphan_check(self) -> None:
        """One check, an interval after the last: evaluate each orphan past
        its grace (and AD-26 extensions) for takeover. Stopped meanwhile,
        it does nothing and the loop ends."""
        await self._clock.sleep(self._orphan_check_interval_seconds)

        if not self._running:
            return

        orphaned_jobs = self._state.get_orphaned_jobs()
        if not orphaned_jobs:
            return

        # Tracking of jobs no longer orphaned goes with them.
        self._forget_settled_orphans(orphaned_jobs)

        # The grace: the gate tier's verdict on a lapsed leader
        # (derived), or the longest rescue seen here if longer.
        grace = max(self._orphan_grace_period_seconds, self._state.longest_orphan_rescue_seconds)
        now = self._clock.monotonic()
        jobs_to_evaluate = self._orphans_due_for_evaluation(orphaned_jobs, grace, now)

        await self._evaluate_due_orphans(jobs_to_evaluate)

    def _forget_settled_orphans(self, orphaned_jobs: dict[str, float]) -> None:
        """Drop the tracking of jobs no longer orphaned."""
        for settled_job_id in self._settled_orphan_job_ids(orphaned_jobs):
            self._orphan_extensions.pop(settled_job_id, None)
            self._orphan_due_at.pop(settled_job_id, None)
            self._orphan_due_heartbeats.pop(settled_job_id, None)
            self._takeover_extensions.pop(settled_job_id, None)

    def _settled_orphan_job_ids(self, orphaned_jobs: dict[str, float]) -> list[str]:
        """The tracked jobs that are no longer orphaned."""
        return [
            job_id
            for job_id in self._orphan_extensions.keys() | self._orphan_due_at.keys()
            if job_id not in orphaned_jobs
        ]

    def _orphans_due_for_evaluation(
        self,
        orphaned_jobs: dict[str, float],
        grace: float,
        now: float,
    ) -> list[tuple[str, float]]:
        """The orphans to evaluate for takeover now, with when each was
        orphaned."""
        jobs_to_evaluate: list[tuple[str, float]] = []
        for job_id, orphaned_at in orphaned_jobs.items():
            if self._is_orphan_due(job_id, orphaned_at, grace, now):
                jobs_to_evaluate.append((job_id, orphaned_at))
        return jobs_to_evaluate

    def _is_orphan_due(self, job_id: str, orphaned_at: float, grace: float, now: float) -> bool:
        """Whether an orphan is due for evaluation: SWIM confirmed its
        leader dead, or its grace and extensions ran out unextended."""
        if job_id in self._confirmed_orphaned_jobs:
            return True
        tracker = self._orphan_extensions.get(job_id)
        if now - orphaned_at < grace + self._extended_seconds(tracker):
            return False
        return not self._extend_orphan_grace(job_id, tracker, grace)

    def _extend_orphan_grace(
        self,
        job_id: str,
        tracker: ExtensionTracker | None,
        grace: float,
    ) -> bool:
        """Whether the orphan's grace was extended again: its leader is
        still heard from since the last grant (AD-26)."""
        # AD-26: a leader still heard from since the last grant
        # (or the orphaning) may yet renew its lease -- extend,
        # decaying. A silent one has nothing to wait for.
        if (leader_heartbeats := self._state.orphan_leader_heartbeats(job_id)) is None:
            return False
        heartbeats, baseline = leader_heartbeats
        baseline = self._extension_baseline(tracker, baseline)
        if not heartbeats > baseline:
            return False
        return self._grant_extension(
            job_id,
            tracker,
            grace,
            heartbeats,
            self._orphan_extensions,
            "orphaned: its leader gate is still heard from",
        )

    async def _evaluate_due_orphans(self, jobs_to_evaluate: list[tuple[str, float]]) -> None:
        """Evaluate each due orphan for takeover."""
        if not jobs_to_evaluate:
            return

        await self._logger.log(
            ServerDebug(
                message=f"Evaluating {len(jobs_to_evaluate)} orphaned jobs for takeover",
                node_host=self._get_node_addr()[0],
                node_port=self._get_node_addr()[1],
                node_id=self._get_node_id().short,
            )
        )

        for job_id, orphaned_at in jobs_to_evaluate:
            await self._evaluate_orphan_takeover(
                job_id,
                orphaned_at,
            )

    async def _evaluate_orphan_takeover(
        self,
        job_id: str,
        orphaned_at: float,
    ) -> None:
        """
        Evaluate whether to take over an orphaned job.

        Checks if this gate is the SWIM cluster leader, and if so, executes
        the takeover with proper fencing.

        Args:
            job_id: The orphaned job ID
            orphaned_at: Timestamp when job was marked orphaned
        """
        # Due now (or earlier): the tier leader takes it over. A tier that
        # has not by the end of its takeover window -- one more failover
        # (derived), or the longest takeover seen after coming due if
        # longer -- has no leader that can; it gets AD-26 extensions while
        # peer gates are still heard from (they may yet elect one).
        now = self._clock.monotonic()
        due_at = self._orphan_due_at.setdefault(job_id, now)
        tier_heartbeats = self._state.gate_peer_heartbeats_total
        due_heartbeats = self._orphan_due_heartbeats.setdefault(job_id, tier_heartbeats)
        takeover_window = max(self._orphan_grace_period_seconds, self._state.longest_orphan_takeover_wait_seconds)
        takeover_window_spent = self._is_takeover_window_spent(
            job_id, now, due_at, takeover_window, tier_heartbeats, due_heartbeats
        )

        job = await self._local_or_repaired_job(job_id)
        if not job:
            self._forget_unrecoverable_orphan(job_id, takeover_window_spent)
            return

        await self._decide_orphan(job_id, job, now - orphaned_at, takeover_window_spent)

    def _is_takeover_window_spent(
        self,
        job_id: str,
        now: float,
        due_at: float,
        takeover_window: float,
        tier_heartbeats: int,
        due_heartbeats: int,
    ) -> bool:
        """Whether the orphan's takeover window, with its AD-26 extensions,
        is spent -- extended once more while the tier is still heard from."""
        tracker = self._takeover_extensions.get(job_id)
        if not now - due_at >= takeover_window + self._extended_seconds(tracker):
            return False

        last_heartbeats = self._extension_baseline(tracker, due_heartbeats)
        if not tier_heartbeats > last_heartbeats:
            return True

        return not self._grant_extension(
            job_id,
            tracker,
            takeover_window,
            tier_heartbeats,
            self._takeover_extensions,
            "orphan due: its tier is still heard from",
        )

    @staticmethod
    def _extended_seconds(tracker: ExtensionTracker | None) -> float:
        """The seconds an AD-26 extension tracker granted so far."""
        return tracker.total_extended if tracker is not None else 0.0

    @staticmethod
    def _extension_baseline(tracker: ExtensionTracker | None, baseline: int) -> int:
        """The heartbeat count of the last grant, else the given baseline."""
        return (
            tracker.last_completed_items
            if tracker is not None and tracker.last_completed_items is not None
            else baseline
        )

    def _grant_extension(
        self,
        job_id: str,
        tracker: ExtensionTracker | None,
        base_deadline: float,
        heartbeats: int,
        trackers: dict[str, ExtensionTracker],
        reason: str,
    ) -> bool:
        """Request an AD-26 extension, decaying, for a job still heard from,
        tracking it from its first; whether it was granted."""
        if tracker is None:
            tracker = ExtensionTracker(
                worker_id=job_id,
                base_deadline=base_deadline,
                min_grant=self._orphan_extension_min_grant_seconds,
                max_extensions=self._orphan_extension_max_extensions,
            )
            trackers[job_id] = tracker
        granted, _grant, _denial, _warning = tracker.request_extension(
            reason,
            current_progress=float(heartbeats),
            completed_items=heartbeats,
        )
        return granted

    async def _local_or_repaired_job(self, job_id: str) -> GlobalJobStatus | None:
        """The job held here, else repaired from peer gates' replicas."""
        job = self._job_manager.get_job(job_id)
        if not job:
            job = await self._get_or_repair_job(job_id)
        return job

    def _forget_unrecoverable_orphan(self, job_id: str, takeover_window_spent: bool) -> None:
        """An orphan held nowhere is forgotten once its takeover window is spent."""
        if takeover_window_spent:
            self._clear_orphaned_job(job_id)

    async def _decide_orphan(
        self,
        job_id: str,
        job: GlobalJobStatus,
        time_orphaned: float,
        takeover_window_spent: bool,
    ) -> None:
        """Clear a terminal orphan, fail one whose takeover window is spent,
        else take it over when this gate leads the cluster."""
        if job.status in self._terminal_statuses:
            self._clear_orphaned_job(job_id)
            return

        if takeover_window_spent:
            await self._fail_orphaned_job(job_id, job, time_orphaned)
            return

        await self._take_over_if_cluster_leader(job_id)

    async def _take_over_if_cluster_leader(self, job_id: str) -> None:
        """Take the orphan over when this gate is the SWIM cluster leader."""
        if not self._is_current_cluster_leader():
            await self._logger.log(
                ServerDebug(
                    message=(
                        f"Job {job_id[:8]}... orphan takeover deferred to the "
                        "SWIM cluster leader"
                    ),
                    node_host=self._get_node_addr()[0],
                    node_port=self._get_node_addr()[1],
                    node_id=self._get_node_id().short,
                ),
            )
            return

        await self._execute_takeover(job_id)

    def _is_current_cluster_leader(self) -> bool:
        """Return whether this gate is currently the SWIM cluster leader."""
        if self._is_cluster_leader is None:
            return False
        return self._is_cluster_leader()

    def _clear_orphaned_job(self, job_id: str) -> None:
        """Clear all orphan evidence for ``job_id``."""
        self._state.clear_orphaned_job(job_id)
        self._confirmed_orphaned_jobs.discard(job_id)
        self._orphan_extensions.pop(job_id, None)
        self._orphan_due_at.pop(job_id, None)
        self._orphan_due_heartbeats.pop(job_id, None)
        self._takeover_extensions.pop(job_id, None)

    async def _fail_orphaned_job(
        self,
        job_id: str,
        job: GlobalJobStatus,
        time_orphaned: float,
    ) -> None:
        """Fail an orphaned job that exceeded the takeover timeout."""
        job.status = JobStatus.FAILED.value
        if job.timestamp > 0:
            job.elapsed_seconds = self._clock.monotonic() - job.timestamp
        self._job_manager.set_job(job_id, job)
        self._clear_orphaned_job(job_id)
        await self._finalize_failed_job(
            job_id,
            (),
            f"orphaned for {time_orphaned:.1f}s without takeover",
        )

        await self._logger.log(
            ServerWarning(
                message=f"Orphaned job {job_id[:8]}... failed after {time_orphaned:.1f}s without takeover",
                node_host=self._get_node_addr()[0],
                node_port=self._get_node_addr()[1],
                node_id=self._get_node_id().short,
            )
        )

        callback = self._job_manager.get_callback(job_id)
        if callback is None:
            return

        push = JobStatusPush(
            job_id=job_id,
            status=job.status,
            message=f"Job {job_id} failed (orphan timeout)",
            total_completed=job.total_completed,
            total_failed=job.total_failed,
            overall_rate=job.overall_rate,
            elapsed_seconds=job.elapsed_seconds,
            is_final=True,
            callback_addr=callback,
        )
        await self._send_job_status_push_with_retry(
            job_id,
            callback,
            push.dump(),
            allow_peer_forwarding=True,
        )

    async def _get_or_repair_job(self, job_id: str) -> GlobalJobStatus | None:
        """Return local job state, fetching committed replica state if needed."""
        job = self._job_manager.get_job(job_id)
        if job is not None:
            return job

        if self._state_repair_callback is None:
            return None

        return await self._repair_job(job_id)

    async def _repair_job(self, job_id: str) -> GlobalJobStatus | None:
        """Fetch the job's committed replica state from peer gates; None
        (a failure logged) when none could be applied."""
        try:
            repaired = await self._state_repair_callback(job_id)
        except Exception as repair_error:
            await self._logger.log(
                ServerDebug(
                    message=(
                        f"State repair for orphaned job {job_id[:8]}... "
                        f"failed: {repair_error}"
                    ),
                    node_host=self._get_node_addr()[0],
                    node_port=self._get_node_addr()[1],
                    node_id=self._get_node_id().short,
                ),
            )
            return None

        return self._job_manager.get_job(job_id) if repaired else None

    async def _execute_takeover(self, job_id: str) -> None:
        """
        Execute takeover of an orphaned job.

        The SWIM cluster leader quorum-commits leadership with an incremented
        fencing token, then broadcasts the committed value. This path
        intentionally does not sleep after leader selection; delaying lets
        unrelated state cleanup clear the orphan before the leader can fence
        the old owner.

        Args:
            job_id: The job ID to take over
        """
        async with self._lock:
            target_dc_count = await self._takeover_target_dc_count_locked(job_id)

        if target_dc_count is None:
            return

        if (new_token := await self._commit_and_settle_takeover(job_id)) is None:
            return

        await self._logger.log(
            ServerInfo(
                message=(
                    f"Took over orphaned job {job_id[:8]}... "
                    f"(fence_token={new_token}, target_dcs={target_dc_count})"
                ),
                node_host=self._get_node_addr()[0],
                node_port=self._get_node_addr()[1],
                node_id=self._get_node_id().short,
            )
        )

        await self._broadcast_leadership_takeover(job_id, new_token, target_dc_count)

    async def _takeover_target_dc_count_locked(self, job_id: str) -> int | None:
        """Under the lock: the job's target datacenter count when this gate,
        the SWIM cluster leader, may take the still-orphaned job over; None
        when it may not."""
        if not self._state.is_job_orphaned(job_id):
            return None

        job = await self._get_or_repair_job(job_id)
        if job is None:
            return None

        return self._leader_target_dc_count(job_id, job)

    def _leader_target_dc_count(self, job_id: str, job: GlobalJobStatus) -> int | None:
        """The job's target datacenter count while it runs and this gate is
        the SWIM cluster leader; a terminal job's orphan evidence is cleared."""
        if job.status in self._terminal_statuses:
            self._clear_orphaned_job(job_id)
            return None

        if not self._is_current_cluster_leader():
            return None

        return len(self._job_manager.get_target_dcs(job_id))

    async def _commit_and_settle_takeover(self, job_id: str) -> int | None:
        """Quorum-commit the takeover, then clear the job's orphan state;
        the new fence token, or None when the takeover did not commit or
        the job stopped being orphaned meanwhile."""
        new_token = await self._commit_takeover(job_id)
        if new_token is None:
            return None

        async with self._lock:
            if not self._settle_takeover_locked(job_id):
                return None

        return new_token

    async def _commit_takeover(self, job_id: str) -> int | None:
        """Quorum-commit this gate's leadership of the job with a raised
        fence token; None (logged) when it could not."""
        if self._commit_takeover_callback is None:
            await self._logger.log(
                ServerWarning(
                    message=f"No commit callback available for orphaned job {job_id[:8]}... takeover",
                    node_host=self._get_node_addr()[0],
                    node_port=self._get_node_addr()[1],
                    node_id=self._get_node_id().short,
                )
            )
            return None

        new_token = await self._commit_takeover_callback(job_id)
        if new_token is None:
            await self._logger.log(
                ServerWarning(
                    message=f"Quorum commit failed for orphaned job {job_id[:8]}... takeover",
                    node_host=self._get_node_addr()[0],
                    node_port=self._get_node_addr()[1],
                    node_id=self._get_node_id().short,
                )
            )
        return new_token

    def _settle_takeover_locked(self, job_id: str) -> bool:
        """Under the lock: record the takeover's wait and clear the job's
        orphan evidence; False when it is no longer orphaned."""
        if not self._state.is_job_orphaned(job_id):
            return False
        if (due_at := self._orphan_due_at.get(job_id)) is not None:
            self._state.record_orphan_takeover_wait(self._clock.monotonic() - due_at)
        self._clear_orphaned_job(job_id)
        return True

    async def _broadcast_leadership_takeover(
        self,
        job_id: str,
        fence_token: int,
        target_dc_count: int,
    ) -> None:
        """
        Broadcast leadership takeover to peer gates.

        Sends JobLeadershipAnnouncement to all active peer gates so they
        update their tracking of who leads this job.

        Args:
            job_id: The job ID we took over
            fence_token: Our new fencing token
            target_dc_count: Number of target datacenters for the job
        """
        node_id = self._get_node_id()
        node_addr = self._get_node_addr()

        announcement = JobLeadershipAnnouncement(
            job_id=job_id,
            leader_id=node_id.full,
            leader_addr=node_addr,
            fence_token=fence_token,
            target_dc_count=target_dc_count,
        )

        announcement_data = announcement.dump()
        active_peers = self._get_active_peers()

        for peer_addr in sorted(active_peers):
            self._task_runner.run(
                self._send_leadership_announcement,
                peer_addr,
                announcement_data,
                job_id,
            )

    async def _send_leadership_announcement(
        self,
        peer_addr: tuple[str, int],
        announcement_data: bytes,
        job_id: str,
    ) -> None:
        """
        Send leadership announcement to a single peer gate.

        Best-effort delivery - failures are logged but don't block takeover.

        Args:
            peer_addr: TCP address of the peer gate
            announcement_data: Serialized JobLeadershipAnnouncement
            job_id: Job ID for logging
        """
        try:
            response, _ = await self._send_tcp(
                peer_addr,
                "job_leadership_announcement",
                announcement_data,
                5.0,
            )
            # send_tcp returns transport errors rather than raising.
            if isinstance(response, Exception):
                raise response
        except Exception as error:
            await self._logger.log(
                ServerDebug(
                    message=f"Failed to send leadership announcement for {job_id[:8]}... to {peer_addr}: {error}",
                    node_host=self._get_node_addr()[0],
                    node_port=self._get_node_addr()[1],
                    node_id=self._get_node_id().short,
                ),
            )

    def get_orphan_stats(self) -> OrphanJobStats:
        """
        Get statistics about orphaned job tracking.

        Returns:
            Dict with orphan counts and timing information
        """
        orphaned_jobs = self._state.get_orphaned_jobs()
        now = self._clock.monotonic()

        past_grace_period = sum(
            1
            for orphaned_at in orphaned_jobs.values()
            if (now - orphaned_at) >= self._orphan_grace_period_seconds
        )

        return {
            "total_orphaned": len(orphaned_jobs),
            "confirmed_orphaned": len(self._confirmed_orphaned_jobs),
            "past_grace_period": past_grace_period,
            "grace_period_seconds": self._orphan_grace_period_seconds,
            "check_interval_seconds": self._orphan_check_interval_seconds,
            "running": self._running,
        }
