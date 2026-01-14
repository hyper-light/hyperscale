"""
Gate-side job timeout tracking for multi-DC coordination (AD-34).

The GateJobTimeoutTracker aggregates timeout state from all DCs:
- Receives JobProgressReport from managers (periodic, best-effort)
- Receives JobTimeoutReport from managers (persistent until ACK'd)
- Declares global timeout when appropriate (all DCs timed out, stuck, etc.)
- Broadcasts JobGlobalTimeout to all DC managers

This is the gate-side counterpart to GateCoordinatedTimeout in manager.
"""

import asyncio
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from hyperscale.logging.hyperscale_logging_models import (
    ServerDebug,
    ServerInfo,
    ServerWarning,
)
from hyperscale.distributed.models.distributed import (
    JobProgressReport,
    JobTimeoutReport,
    JobGlobalTimeout,
    JobLeaderTransfer,
    JobFinalStatus,
)

if TYPE_CHECKING:
    from hyperscale.distributed.nodes.gate import GateServer


@dataclass(slots=True)
class GateJobTrackingInfo:
    """
    Gate's view of a job across all DCs (AD-34 Part 5).

    Tracks per-DC progress, timeouts, and extension data to enable
    global timeout decisions.
    """

    job_id: str
    """Job identifier."""

    submitted_at: float
    """Global start time (monotonic)."""

    timeout_seconds: float
    """Job timeout in seconds."""

    target_datacenters: list[str]
    """DCs where this job is running."""

    dc_status: dict[str, str] = field(default_factory=dict)
    """DC -> "running" | "completed" | "failed" | "timed_out" | "cancelled"."""

    dc_last_progress: dict[str, float] = field(default_factory=dict)
    """DC -> last progress timestamp (monotonic)."""

    dc_manager_addrs: dict[str, tuple[str, int]] = field(default_factory=dict)
    """DC -> current manager (host, port) for sending timeout decisions."""

    dc_fence_tokens: dict[str, int] = field(default_factory=dict)
    """DC -> manager's fence token (for stale rejection)."""

    # Extension tracking (AD-26 integration)
    dc_total_extensions: dict[str, float] = field(default_factory=dict)
    """DC -> total extension seconds granted."""

    dc_max_extension: dict[str, float] = field(default_factory=dict)
    """DC -> largest single extension granted."""

    dc_workers_with_extensions: dict[str, int] = field(default_factory=dict)
    """DC -> count of workers with active extensions."""

    # Global timeout state
    globally_timed_out: bool = False
    """Whether gate has declared global timeout."""

    timeout_reason: str = ""
    """Reason for global timeout."""

    timeout_fence_token: int = 0
    """Gate's fence token for this timeout decision."""


class GateJobTimeoutTracker:
    """
    Track jobs across all DCs for global timeout coordination (AD-34).

    Gate-side timeout coordination:
    1. Managers send JobProgressReport every ~10s (best-effort)
    2. Managers send JobTimeoutReport when DC-local timeout detected
    3. Gate aggregates and decides when to declare global timeout
    4. Gate broadcasts JobGlobalTimeout to all DCs

    Global timeout triggers:
    - Overall timeout exceeded (based on job's timeout_seconds)
    - All DCs stuck (no progress for stuck_threshold)
    - Majority of DCs timed out locally
    """

    def __init__(
        self,
        gate: "GateServer",
        check_interval: float = 15.0,
        stuck_threshold: float = 180.0,
    ):
        """
        Initialize timeout tracker.

        Args:
            gate: Parent GateServer
            check_interval: Seconds between timeout checks
            stuck_threshold: Seconds of no progress before "stuck" declaration
        """
        self._gate = gate
        self._tracked_jobs: dict[str, GateJobTrackingInfo] = {}
        self._lock = asyncio.Lock()
        self._check_interval = check_interval
        self._stuck_threshold = stuck_threshold
        self._running = False
        self._check_task: asyncio.Task | None = None

    async def start(self) -> None:
        """Start the timeout checking loop."""
        if self._running:
            return
        self._running = True
        self._check_task = asyncio.create_task(self._timeout_check_loop())

    async def stop(self) -> None:
        """Stop the timeout checking loop."""
        self._running = False
        if self._check_task:
            self._check_task.cancel()
            try:
                await self._check_task
            except asyncio.CancelledError:
                pass
            self._check_task = None
        async with self._lock:
            self._tracked_jobs.clear()

    async def start_tracking_job(
        self,
        job_id: str,
        timeout_seconds: float,
        target_dcs: list[str],
    ) -> None:
        """
        Start tracking when job is submitted to DCs.

        Called by gate when dispatching job to datacenters.
        """
        async with self._lock:
            now = time.monotonic()
            self._tracked_jobs[job_id] = GateJobTrackingInfo(
                job_id=job_id,
                submitted_at=now,
                timeout_seconds=timeout_seconds,
                target_datacenters=list(target_dcs),
                dc_status={dc: "running" for dc in target_dcs},
                dc_last_progress={dc: now for dc in target_dcs},
                dc_manager_addrs={},
                dc_fence_tokens={},
                dc_total_extensions={dc: 0.0 for dc in target_dcs},
                dc_max_extension={dc: 0.0 for dc in target_dcs},
                dc_workers_with_extensions={dc: 0 for dc in target_dcs},
                timeout_fence_token=0,
            )

    async def record_progress(self, report: JobProgressReport) -> None:
        """
        Record progress from a DC (AD-34 Part 5).

        Updates tracking state with progress info from manager.
        Best-effort - lost reports are tolerated.
        """
        async with self._lock:
            info = self._tracked_jobs.get(report.job_id)
            if not info:
                return

            # Update DC progress
            info.dc_last_progress[report.datacenter] = report.timestamp
            info.dc_manager_addrs[report.datacenter] = (
                report.manager_host,
                report.manager_port,
            )
            info.dc_fence_tokens[report.datacenter] = report.fence_token

            # Update extension tracking (AD-26 integration)
            info.dc_total_extensions[report.datacenter] = (
                report.total_extensions_granted
            )
            info.dc_max_extension[report.datacenter] = report.max_worker_extension
            info.dc_workers_with_extensions[report.datacenter] = (
                report.workers_with_extensions
            )

            # Check if DC completed
            if report.workflows_completed == report.workflows_total:
                info.dc_status[report.datacenter] = "completed"

    async def record_timeout(self, report: JobTimeoutReport) -> None:
        """
        Record DC-local timeout from a manager (AD-34 Part 5).

        Manager detected timeout but waits for gate's global decision.
        """
        async with self._lock:
            info = self._tracked_jobs.get(report.job_id)
            if not info:
                return

            info.dc_status[report.datacenter] = "timed_out"
            info.dc_manager_addrs[report.datacenter] = (
                report.manager_host,
                report.manager_port,
            )
            info.dc_fence_tokens[report.datacenter] = report.fence_token

            await self._gate._udp_logger.log(
                ServerInfo(
                    message=f"DC {report.datacenter} reported timeout for job {report.job_id[:8]}...: {report.reason}",
                    node_host=self._gate._host,
                    node_port=self._gate._tcp_port,
                    node_id=self._gate._node_id.short,
                )
            )

    async def record_leader_transfer(self, report: JobLeaderTransfer) -> None:
        """
        Record manager leader change in a DC (AD-34 Part 7).

        Updates tracking to route future timeout decisions to new leader.
        """
        async with self._lock:
            info = self._tracked_jobs.get(report.job_id)
            if not info:
                return

            info.dc_manager_addrs[report.datacenter] = (
                report.new_leader_host,
                report.new_leader_port,
            )
            info.dc_fence_tokens[report.datacenter] = report.fence_token

            await self._gate._udp_logger.log(
                ServerDebug(
                    message=f"DC {report.datacenter} leader transfer for job {report.job_id[:8]}... "
                    f"to {report.new_leader_id} (fence={report.fence_token})",
                    node_host=self._gate._host,
                    node_port=self._gate._tcp_port,
                    node_id=self._gate._node_id.short,
                )
            )

    async def handle_final_status(self, report: JobFinalStatus) -> None:
        """
        Handle final status from a DC (AD-34 lifecycle cleanup).

        When all DCs report terminal status, remove job from tracking.
        """
        async with self._lock:
            info = self._tracked_jobs.get(report.job_id)
            if not info:
                return

            # Update DC status
            info.dc_status[report.datacenter] = report.status

            # Check if all DCs have terminal status
            terminal_statuses = {
                "completed",
                "failed",
                "cancelled",
                "timed_out",
                "timeout",
            }
            all_terminal = all(
                info.dc_status.get(dc) in terminal_statuses
                for dc in info.target_datacenters
            )

            if all_terminal:
                # All DCs done - cleanup tracking
                del self._tracked_jobs[report.job_id]
                await self._gate._udp_logger.log(
                    ServerDebug(
                        message=f"All DCs terminal for job {report.job_id[:8]}... - removed from timeout tracking",
                        node_host=self._gate._host,
                        node_port=self._gate._tcp_port,
                        node_id=self._gate._node_id.short,
                    )
                )

    async def get_job_info(self, job_id: str) -> GateJobTrackingInfo | None:
        """Get tracking info for a job."""
        async with self._lock:
            return self._tracked_jobs.get(job_id)

    async def _timeout_check_loop(self) -> None:
        """
        Periodically check for global timeouts (AD-34 Part 5).

        Runs every check_interval and evaluates all tracked jobs.
        """
        while self._running:
            try:
                await asyncio.sleep(self._check_interval)

                # Check all tracked jobs
                async with self._lock:
                    jobs_to_check = list(self._tracked_jobs.items())

                for job_id, info in jobs_to_check:
                    if info.globally_timed_out:
                        continue

                    should_timeout, reason = await self._check_global_timeout(info)
                    if should_timeout:
                        await self._declare_global_timeout(job_id, reason)

            except asyncio.CancelledError:
                break
            except Exception as error:
                await self._gate.handle_exception(error, "_timeout_check_loop")

    async def _check_global_timeout(
        self, info: GateJobTrackingInfo
    ) -> tuple[bool, str]:
        """
        Check if job should be globally timed out.

        Returns (should_timeout, reason).
        """
        now = time.monotonic()

        # Skip if already terminal
        terminal_statuses = {"completed", "failed", "cancelled", "timed_out", "timeout"}
        running_dcs = [
            dc
            for dc in info.target_datacenters
            if info.dc_status.get(dc) not in terminal_statuses
        ]

        if not running_dcs:
            return False, ""

        # Calculate effective timeout with extensions
        # Use max extensions across all DCs (most conservative)
        max_extensions = max(
            info.dc_total_extensions.get(dc, 0.0) for dc in info.target_datacenters
        )
        effective_timeout = info.timeout_seconds + max_extensions

        # Check overall timeout
        elapsed = now - info.submitted_at
        if elapsed > effective_timeout:
            return True, (
                f"Global timeout exceeded ({elapsed:.1f}s > {effective_timeout:.1f}s, "
                f"base={info.timeout_seconds:.1f}s + extensions={max_extensions:.1f}s)"
            )

        # Check if all running DCs are stuck (no progress)
        all_stuck = True
        for dc in running_dcs:
            last_progress = info.dc_last_progress.get(dc, info.submitted_at)
            if now - last_progress < self._stuck_threshold:
                all_stuck = False
                break

        if all_stuck and running_dcs:
            oldest_progress = min(
                info.dc_last_progress.get(dc, info.submitted_at) for dc in running_dcs
            )
            stuck_duration = now - oldest_progress
            return True, (
                f"All DCs stuck (no progress for {stuck_duration:.1f}s across {len(running_dcs)} DCs)"
            )

        # Check if majority of DCs report local timeout
        local_timeout_dcs = [
            dc
            for dc in info.target_datacenters
            if info.dc_status.get(dc) == "timed_out"
        ]
        if len(local_timeout_dcs) > len(info.target_datacenters) / 2:
            return True, (
                f"Majority DCs timed out ({len(local_timeout_dcs)}/{len(info.target_datacenters)})"
            )

        return False, ""

    async def _declare_global_timeout(self, job_id: str, reason: str) -> None:
        """
        Declare global timeout and broadcast to all DCs (AD-34 Part 5).

        Sends JobGlobalTimeout to all target DCs.
        """
        async with self._lock:
            info = self._tracked_jobs.get(job_id)
            if not info or info.globally_timed_out:
                return

            # Mark as globally timed out
            info.globally_timed_out = True
            info.timeout_reason = reason
            info.timeout_fence_token += 1

        await self._gate._udp_logger.log(
            ServerWarning(
                message=f"Declaring global timeout for job {job_id[:8]}...: {reason}",
                node_host=self._gate._host,
                node_port=self._gate._tcp_port,
                node_id=self._gate._node_id.short,
            )
        )

        # Broadcast to all DCs with managers
        timeout_msg = JobGlobalTimeout(
            job_id=job_id,
            reason=reason,
            timed_out_at=time.monotonic(),
            fence_token=info.timeout_fence_token,
        )

        for dc, manager_addr in info.dc_manager_addrs.items():
            if info.dc_status.get(dc) in {"completed", "failed", "cancelled"}:
                continue  # Skip terminal DCs

            try:
                await self._gate.send_tcp(
                    manager_addr,
                    "receive_job_global_timeout",
                    timeout_msg.dump(),
                    timeout=5.0,
                )
            except Exception as error:
                await self._gate._udp_logger.log(
                    ServerWarning(
                        message=f"Failed to send global timeout to DC {dc} for job {job_id[:8]}...: {error}",
                        node_host=self._gate._host,
                        node_port=self._gate._tcp_port,
                        node_id=self._gate._node_id.short,
                    )
                )

        try:
            await self._gate.handle_global_timeout(
                job_id,
                reason,
                list(info.target_datacenters),
                dict(info.dc_manager_addrs),
            )
        except Exception as error:
            await self._gate.handle_exception(error, "handle_global_timeout")

    async def stop_tracking(self, job_id: str) -> None:
        """
        Stop tracking a job (called on cleanup).

        Removes job from tracker without declaring timeout.
        """
        async with self._lock:
            self._tracked_jobs.pop(job_id, None)
