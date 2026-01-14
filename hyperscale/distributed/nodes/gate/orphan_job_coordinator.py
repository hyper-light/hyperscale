"""
Gate orphan job coordinator for handling job takeover when gate peers fail.

This module implements the detection and takeover of orphaned jobs when a gate
peer becomes unavailable. It uses the consistent hash ring to determine new
ownership and fencing tokens to prevent split-brain scenarios.

Key responsibilities:
- Detect jobs orphaned by gate peer failures
- Determine new job ownership via consistent hash ring
- Execute takeover with proper fencing token increment
- Broadcast leadership changes to peer gates and managers
- Prevent thundering herd via jitter and grace periods
"""

import asyncio
import random
import time
from typing import TYPE_CHECKING, Any, Callable, Awaitable

from hyperscale.distributed.models import (
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

from .state import GateRuntimeState

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
    4. Takes over jobs where this gate is the new owner (via hash ring)
    5. Broadcasts leadership changes to maintain cluster consistency

    The grace period prevents premature takeover during transient network issues
    and allows the consistent hash ring to stabilize after node removal.

    Asyncio Safety:
    - Uses internal lock for orphan state modifications
    - Coordinates with JobLeadershipTracker's async methods
    - Background loop runs via TaskRunner for proper lifecycle management
    """

    __slots__ = (
        "_state",
        "_logger",
        "_task_runner",
        "_job_hash_ring",
        "_job_leadership_tracker",
        "_job_manager",
        "_get_node_id",
        "_get_node_addr",
        "_send_tcp",
        "_get_active_peers",
        "_orphan_check_interval_seconds",
        "_orphan_grace_period_seconds",
        "_orphan_timeout_seconds",
        "_takeover_jitter_min_seconds",
        "_takeover_jitter_max_seconds",
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
        orphan_check_interval_seconds: float = 15.0,
        orphan_grace_period_seconds: float = 30.0,
        orphan_timeout_seconds: float = 300.0,
        takeover_jitter_min_seconds: float = 0.5,
        takeover_jitter_max_seconds: float = 2.0,
    ) -> None:
        """
        Initialize the orphan job coordinator.

        Args:
            state: Runtime state container with orphan tracking
            logger: Async logger instance
            task_runner: Background task executor
            job_hash_ring: Consistent hash ring for determining job ownership
            job_leadership_tracker: Tracks per-job leadership with fencing tokens
            job_manager: Manages job state and target datacenters
            get_node_id: Callback to get this gate's node ID
            get_node_addr: Callback to get this gate's TCP address
            send_tcp: Callback to send TCP messages to peers
            get_active_peers: Callback to get active peer gate addresses
            orphan_check_interval_seconds: How often to scan for orphaned jobs
            orphan_grace_period_seconds: Time to wait before attempting takeover
            orphan_timeout_seconds: Max time before orphaned jobs fail
            takeover_jitter_min_seconds: Minimum random jitter before takeover
            takeover_jitter_max_seconds: Maximum random jitter before takeover
        """
        self._state = state
        self._logger = logger
        self._task_runner = task_runner
        self._job_hash_ring = job_hash_ring
        self._job_leadership_tracker = job_leadership_tracker
        self._job_manager = job_manager
        self._get_node_id = get_node_id
        self._get_node_addr = get_node_addr
        self._send_tcp = send_tcp
        self._get_active_peers = get_active_peers
        self._orphan_check_interval_seconds = orphan_check_interval_seconds
        self._orphan_grace_period_seconds = orphan_grace_period_seconds
        self._orphan_timeout_seconds = orphan_timeout_seconds
        self._takeover_jitter_min_seconds = takeover_jitter_min_seconds
        self._takeover_jitter_max_seconds = takeover_jitter_max_seconds
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
        self._check_loop_task = asyncio.create_task(self._orphan_check_loop())

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
            self._check_loop_task.cancel()
            try:
                await self._check_loop_task
            except asyncio.CancelledError:
                pass

        self._check_loop_task = None

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

        now = time.monotonic()
        for job_id in orphaned_job_ids:
            self._state.mark_job_orphaned(job_id, now)

        self._state.mark_leader_dead(failed_gate_addr)

        return orphaned_job_ids

    def on_lease_expired(self, lease: "JobLease") -> None:
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

        now = time.monotonic()
        if not self._state.is_job_orphaned(job_id):
            self._state.mark_job_orphaned(job_id, now)

            self._task_runner.run(
                self._logger.log,
                ServerDebug(
                    message=f"Job {job_id[:8]}... lease expired (owner={owner_node[:8]}...), marked for orphan check",
                    node_host=self._get_node_addr()[0],
                    node_port=self._get_node_addr()[1],
                    node_id=self._get_node_id().short,
                ),
            )

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
            try:
                await asyncio.sleep(self._orphan_check_interval_seconds)

                if not self._running:
                    break

                orphaned_jobs = self._state.get_orphaned_jobs()
                if not orphaned_jobs:
                    continue

                now = time.monotonic()
                jobs_to_evaluate: list[tuple[str, float]] = []

                for job_id, orphaned_at in orphaned_jobs.items():
                    time_orphaned = now - orphaned_at
                    if time_orphaned >= self._orphan_grace_period_seconds:
                        jobs_to_evaluate.append((job_id, orphaned_at))

                if not jobs_to_evaluate:
                    continue

                await self._logger.log(
                    ServerDebug(
                        message=f"Evaluating {len(jobs_to_evaluate)} orphaned jobs for takeover",
                        node_host=self._get_node_addr()[0],
                        node_port=self._get_node_addr()[1],
                        node_id=self._get_node_id().short,
                    )
                )

                for job_id, orphaned_at in jobs_to_evaluate:
                    await self._evaluate_orphan_takeover(job_id, orphaned_at)

            except asyncio.CancelledError:
                break
            except Exception as error:
                self._task_runner.run(
                    self._logger.log,
                    ServerWarning(
                        message=f"Orphan check loop error: {error}",
                        node_host=self._get_node_addr()[0],
                        node_port=self._get_node_addr()[1],
                        node_id=self._get_node_id().short,
                    ),
                )

    async def _evaluate_orphan_takeover(
        self,
        job_id: str,
        orphaned_at: float,
    ) -> None:
        """
        Evaluate whether to take over an orphaned job.

        Checks if this gate is the new owner via consistent hash ring,
        and if so, executes the takeover with proper fencing.

        Args:
            job_id: The orphaned job ID
            orphaned_at: Timestamp when job was marked orphaned
        """
        job = self._job_manager.get_job(job_id)
        if not job:
            self._state.clear_orphaned_job(job_id)
            return

        if job.status in self._terminal_statuses:
            self._state.clear_orphaned_job(job_id)
            return

        new_owner = await self._job_hash_ring.get_node(job_id)
        if not new_owner:
            self._task_runner.run(
                self._logger.log,
                ServerWarning(
                    message=f"No owner found in hash ring for orphaned job {job_id[:8]}...",
                    node_host=self._get_node_addr()[0],
                    node_port=self._get_node_addr()[1],
                    node_id=self._get_node_id().short,
                ),
            )
            return

        my_node_id = self._get_node_id().full

        if new_owner.node_id != my_node_id:
            self._task_runner.run(
                self._logger.log,
                ServerDebug(
                    message=f"Job {job_id[:8]}... should be owned by {new_owner.node_id[:8]}..., not us",
                    node_host=self._get_node_addr()[0],
                    node_port=self._get_node_addr()[1],
                    node_id=self._get_node_id().short,
                ),
            )
            return

        await self._execute_takeover(job_id)

    async def _execute_takeover(self, job_id: str) -> None:
        """
        Execute takeover of an orphaned job.

        Applies jitter to prevent thundering herd, takes over leadership
        with an incremented fencing token, and broadcasts the change.

        Args:
            job_id: The job ID to take over
        """
        if self._takeover_jitter_max_seconds > 0:
            jitter = random.uniform(
                self._takeover_jitter_min_seconds,
                self._takeover_jitter_max_seconds,
            )
            await asyncio.sleep(jitter)

        if not self._state.is_job_orphaned(job_id):
            return

        job = self._job_manager.get_job(job_id)
        if not job or job.status in self._terminal_statuses:
            self._state.clear_orphaned_job(job_id)
            return

        target_dc_count = len(self._job_manager.get_target_dcs(job_id))

        new_token = await self._job_leadership_tracker.takeover_leadership_async(
            job_id,
            metadata=target_dc_count,
        )

        self._state.clear_orphaned_job(job_id)

        await self._logger.log(
            ServerInfo(
                message=f"Took over orphaned job {job_id[:8]}... (fence_token={new_token}, target_dcs={target_dc_count})",
                node_host=self._get_node_addr()[0],
                node_port=self._get_node_addr()[1],
                node_id=self._get_node_id().short,
            )
        )

        await self._broadcast_leadership_takeover(job_id, new_token, target_dc_count)

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

        for peer_addr in active_peers:
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
            await self._send_tcp(
                peer_addr,
                "job_leadership_announcement",
                announcement_data,
                5.0,
            )
        except Exception as error:
            self._task_runner.run(
                self._logger.log,
                ServerDebug(
                    message=f"Failed to send leadership announcement for {job_id[:8]}... to {peer_addr}: {error}",
                    node_host=self._get_node_addr()[0],
                    node_port=self._get_node_addr()[1],
                    node_id=self._get_node_id().short,
                ),
            )

    def get_orphan_stats(self) -> dict[str, Any]:
        """
        Get statistics about orphaned job tracking.

        Returns:
            Dict with orphan counts and timing information
        """
        orphaned_jobs = self._state.get_orphaned_jobs()
        now = time.monotonic()

        past_grace_period = sum(
            1
            for orphaned_at in orphaned_jobs.values()
            if (now - orphaned_at) >= self._orphan_grace_period_seconds
        )

        return {
            "total_orphaned": len(orphaned_jobs),
            "past_grace_period": past_grace_period,
            "grace_period_seconds": self._orphan_grace_period_seconds,
            "check_interval_seconds": self._orphan_check_interval_seconds,
            "running": self._running,
        }
