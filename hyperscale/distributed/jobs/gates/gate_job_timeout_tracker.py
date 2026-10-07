"""
Gate-side job timeout tracking for multi-DC coordination (AD-34).

The GateJobTimeoutTracker aggregates timeout state from all DCs:
- Receives JobProgressReport from managers (periodic, best-effort)
- Receives JobTimeoutReport from managers (persistent until ACK'd)
- Declares global timeout when appropriate (all DCs timed out, stuck, etc.)
- Broadcasts JobGlobalTimeout to all DC managers

This is the gate-side counterpart to GateCoordinatedTimeout in manager.

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING
from hyperscale.logging.hyperscale_logging_models import ServerDebug, ServerInfo, ServerWarning
from hyperscale.distributed.models.distributed import (
    JobProgressReport,
    JobTimeoutReport,
    JobGlobalTimeout,
    JobLeaderTransfer,
    JobFinalStatus,
)
from hyperscale.distributed.runtime import Clock, RealClock

from .gate_job_tracking_info import GateJobTrackingInfo

if TYPE_CHECKING:
    from hyperscale.distributed.nodes.gate import GateServer

_DEFAULT_CLOCK: Clock = RealClock()


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
        # Phase 6b: explicit ``loop.create_task`` keeps the task on the
        # loop this tracker was started from rather than relying on
        # ``get_running_loop`` resolution at task-creation time.
        self._check_task = asyncio.get_running_loop().create_task(
            self._timeout_check_loop()
        )

    async def stop(self) -> None:
        """Stop the timeout checking loop."""
        self._running = False
        if self._check_task:
            await self._cancel_check_task()
        async with self._lock:
            self._tracked_jobs.clear()

    async def _cancel_check_task(self) -> None:
        """Cancel the check loop and wait for it, re-raising only a cancel aimed at the caller."""
        self._check_task.cancel()
        cancels_requested_before_wait = asyncio.current_task().cancelling()
        try:
            await self._check_task
        except asyncio.CancelledError:
            # The task we cancelled ended; a cancel aimed at this task
            # while it waited goes on.
            if asyncio.current_task().cancelling() > cancels_requested_before_wait:
                raise
        self._check_task = None

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
            now = _DEFAULT_CLOCK.monotonic()
            self._tracked_jobs[job_id] = GateJobTrackingInfo(
                job_id=job_id,
                submitted_at=now,
                timeout_seconds=timeout_seconds,
                target_datacenters=list(target_dcs),
                dc_status=dict.fromkeys(target_dcs, "running"),
                dc_last_progress=dict.fromkeys(target_dcs, now),
                dc_manager_addrs={},
                dc_fence_tokens={},
                dc_total_extensions=dict.fromkeys(target_dcs, 0.0),
                dc_max_extension=dict.fromkeys(target_dcs, 0.0),
                dc_workers_with_extensions=dict.fromkeys(target_dcs, 0),
                timeout_fence_token=0,
            )

    async def _reject_superseded_report(
        self,
        info: GateJobTrackingInfo,
        job_id: str,
        datacenter: str,
        fence_token: int,
    ) -> bool:
        """Drop a report carrying a fence token this gate has already
        moved past for that datacenter.

        AD-34's global timeout decision is driven by
        ``dc_last_progress``, and the tracker stored ``fence_token``
        without ever comparing it -- so a straggling report from a
        manager that has since lost leadership refreshed the progress
        clock and pushed the timeout out on evidence from a node no
        longer running the work. After a leadership transfer the old
        leader's in-flight reports are exactly that.

        Returns True when the report was rejected (and logged).
        """
        known_token = info.dc_fence_tokens.get(datacenter)
        if known_token is None or fence_token >= known_token:
            return False

        await self._gate._udp_logger.log(
            ServerWarning(
                message=(
                    f"Ignored superseded report for job {job_id[:8]}... from "
                    f"DC {datacenter}: fence {fence_token} is behind the "
                    f"known fence {known_token}"
                ),
                node_host=self._gate._host,
                node_port=self._gate._tcp_port,
                node_id=self._gate._node_id.short,
            )
        )
        return True

    def _tracked_target_info(self, job_id: str, datacenter: str) -> GateJobTrackingInfo | None:
        """The job's tracking info while ``datacenter`` is still one of its targets, else None."""
        info = self._tracked_jobs.get(job_id)
        if not info or datacenter not in info.target_datacenters:
            return None
        return info

    async def _admitted_report_info(
        self,
        job_id: str,
        datacenter: str,
        fence_token: int,
    ) -> GateJobTrackingInfo | None:
        """The job's tracking info for a report from a current target that is not superseded, else None."""
        # A datacenter the job moved off reports nothing the job's
        # timeout depends on.
        info = self._tracked_target_info(job_id, datacenter)
        if info is None or await self._reject_superseded_report(
            info, job_id, datacenter, fence_token
        ):
            return None
        return info

    async def record_progress(self, report: JobProgressReport) -> None:
        """
        Record progress from a DC (AD-34 Part 5).

        Updates tracking state with progress info from manager.
        Best-effort - lost reports are tolerated.
        """
        async with self._lock:
            info = await self._admitted_report_info(
                report.job_id, report.datacenter, report.fence_token
            )
            if info is None:
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
            info = await self._admitted_report_info(
                report.job_id, report.datacenter, report.fence_token
            )
            if info is None:
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
            info = await self._admitted_report_info(
                report.job_id, report.datacenter, report.fence_token
            )
            if info is None:
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
            info = self._tracked_target_info(report.job_id, report.datacenter)
            if info is None:
                return

            # Update DC status
            info.dc_status[report.datacenter] = report.status

            # Check if all DCs have terminal status
            if self._all_datacenters_terminal(info):
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

    @staticmethod
    def _all_datacenters_terminal(info: GateJobTrackingInfo) -> bool:
        """Whether every target datacenter has reported a terminal status (AD-34 lifecycle cleanup)."""
        terminal_statuses = {
            "completed",
            "failed",
            "cancelled",
            "timed_out",
            "timeout",
        }
        return all(
            info.dc_status.get(dc) in terminal_statuses
            for dc in info.target_datacenters
        )

    async def replace_target_datacenter(
        self,
        job_id: str,
        lost_datacenter: str,
        replacement_datacenter: str,
    ) -> None:
        """The job moved off ``lost_datacenter`` to ``replacement_datacenter``
        (AD-36 mid-flight failover): the replacement is tracked from now --
        just started, so not stuck -- and the lost one no longer counts
        toward the job's timeout decisions. "" for a replacement: nothing
        re-runs, the lost datacenter is only dropped."""
        async with self._lock:
            info = self._tracked_jobs.get(job_id)
            if not info:
                return
            now = _DEFAULT_CLOCK.monotonic()
            self._drop_target_datacenter(info, lost_datacenter)
            if not replacement_datacenter:
                return
            info.target_datacenters.append(replacement_datacenter)
            info.dc_status[replacement_datacenter] = "running"
            info.dc_last_progress[replacement_datacenter] = now
            info.dc_total_extensions[replacement_datacenter] = 0.0
            info.dc_max_extension[replacement_datacenter] = 0.0
            info.dc_workers_with_extensions[replacement_datacenter] = 0

    @staticmethod
    def _drop_target_datacenter(info: GateJobTrackingInfo, lost_datacenter: str) -> None:
        """Stop counting ``lost_datacenter`` toward the job's timeout: untarget it and forget its state."""
        info.target_datacenters = [
            datacenter
            for datacenter in info.target_datacenters
            if datacenter != lost_datacenter
        ]
        GateJobTimeoutTracker._forget_datacenter_state(info, lost_datacenter)

    @staticmethod
    def _forget_datacenter_state(info: GateJobTrackingInfo, lost_datacenter: str) -> None:
        """Drop every per-datacenter record the job keeps for ``lost_datacenter``."""
        for per_datacenter in (
            info.dc_status,
            info.dc_last_progress,
            info.dc_manager_addrs,
            info.dc_fence_tokens,
            info.dc_total_extensions,
            info.dc_max_extension,
            info.dc_workers_with_extensions,
        ):
            per_datacenter.pop(lost_datacenter, None)

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
            if not await self._run_timeout_check_pass():
                break

    async def _run_timeout_check_pass(self) -> bool:
        """One sleep-then-check pass of the loop; False once the loop is cancelled (AD-34 Part 5)."""
        try:
            await _DEFAULT_CLOCK.sleep(self._check_interval)

            # Check all tracked jobs
            await self._check_tracked_jobs()

        except asyncio.CancelledError:
            return False
        except Exception as error:
            await self._gate.handle_exception(error, "_timeout_check_loop")
        return True

    async def _check_tracked_jobs(self) -> None:
        """Snapshot the tracked jobs under the lock, then check each for a global timeout."""
        async with self._lock:
            jobs_to_check = list(self._tracked_jobs.items())

        for job_id, info in jobs_to_check:
            await self._check_job_timeout(job_id, info)

    async def _check_job_timeout(self, job_id: str, info: GateJobTrackingInfo) -> None:
        """Declare a global timeout for a job not already timed out when its check says so."""
        if info.globally_timed_out:
            return

        should_timeout, reason = await self._check_global_timeout(info)
        if should_timeout:
            await self._declare_global_timeout(job_id, reason)

    async def _check_global_timeout(
        self, info: GateJobTrackingInfo
    ) -> tuple[bool, str]:
        """
        Check if job should be globally timed out.

        Returns (should_timeout, reason).
        """
        now = _DEFAULT_CLOCK.monotonic()

        # Skip if already terminal
        running_dcs = self._running_datacenters(info)

        if not running_dcs:
            return False, ""

        # Calculate effective timeout with extensions
        # Use max extensions across all DCs (most conservative)
        max_extensions = self._max_extension_seconds(info)
        effective_timeout = info.timeout_seconds + max_extensions

        # Check overall timeout
        elapsed = now - info.submitted_at
        if elapsed > effective_timeout:
            return True, (
                f"Global timeout exceeded ({elapsed:.1f}s > {effective_timeout:.1f}s, "
                f"base={info.timeout_seconds:.1f}s + extensions={max_extensions:.1f}s)"
            )

        return self._stuck_or_majority_verdict(info, running_dcs, now)

    @staticmethod
    def _running_datacenters(info: GateJobTrackingInfo) -> list[str]:
        """Target datacenters that have not reported a terminal status."""
        terminal_statuses = {"completed", "failed", "cancelled", "timed_out", "timeout"}
        return [
            dc
            for dc in info.target_datacenters
            if info.dc_status.get(dc) not in terminal_statuses
        ]

    @staticmethod
    def _max_extension_seconds(info: GateJobTrackingInfo) -> float:
        """The most extension time granted in any target datacenter (AD-26 integration)."""
        return max(
            info.dc_total_extensions.get(dc, 0.0) for dc in info.target_datacenters
        )

    def _stuck_or_majority_verdict(
        self,
        info: GateJobTrackingInfo,
        running_dcs: list[str],
        now: float,
    ) -> tuple[bool, str]:
        """Time out when every running DC is stuck, else when a majority timed out locally."""
        # Check if all running DCs are stuck (no progress)
        if self._all_running_stuck(info, running_dcs, now):
            return True, self._stuck_reason(info, running_dcs, now)

        return self._majority_timeout_verdict(info)

    def _all_running_stuck(
        self,
        info: GateJobTrackingInfo,
        running_dcs: list[str],
        now: float,
    ) -> bool:
        """Whether no running DC has made progress within the stuck threshold."""
        return all(
            not (now - info.dc_last_progress.get(dc, info.submitted_at) < self._stuck_threshold)
            for dc in running_dcs
        )

    @staticmethod
    def _stuck_reason(
        info: GateJobTrackingInfo,
        running_dcs: list[str],
        now: float,
    ) -> str:
        """Why the job is stuck: how long since the oldest running DC's last progress."""
        oldest_progress = min(
            info.dc_last_progress.get(dc, info.submitted_at) for dc in running_dcs
        )
        stuck_duration = now - oldest_progress
        return (
            f"All DCs stuck (no progress for {stuck_duration:.1f}s across {len(running_dcs)} DCs)"
        )

    @staticmethod
    def _majority_timeout_verdict(info: GateJobTrackingInfo) -> tuple[bool, str]:
        """Time out when a majority of target DCs report a local timeout."""
        # Check if majority of DCs report local timeout
        local_timeout_count = sum(
            info.dc_status.get(dc) == "timed_out" for dc in info.target_datacenters
        )
        if local_timeout_count > len(info.target_datacenters) / 2:
            return True, (
                f"Majority DCs timed out ({local_timeout_count}/{len(info.target_datacenters)})"
            )

        return False, ""

    async def _declare_global_timeout(self, job_id: str, reason: str) -> None:
        """
        Declare global timeout and broadcast to all DCs (AD-34 Part 5).

        Sends JobGlobalTimeout to all target DCs.
        """
        async with self._lock:
            info = self._claim_global_timeout(job_id, reason)
        if info is None:
            return

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
            timed_out_at=_DEFAULT_CLOCK.monotonic(),
            fence_token=info.timeout_fence_token,
        )

        await self._broadcast_global_timeout(job_id, info, timeout_msg)

        try:
            await self._gate.handle_global_timeout(
                job_id,
                reason,
                list(info.target_datacenters),
                dict(info.dc_manager_addrs),
            )
        except Exception as error:
            await self._gate.handle_exception(error, "handle_global_timeout")

    def _claim_global_timeout(self, job_id: str, reason: str) -> GateJobTrackingInfo | None:
        """Mark a tracked job globally timed out (once) and advance its fence; None if not ours to declare."""
        info = self._tracked_jobs.get(job_id)
        if not info or info.globally_timed_out:
            return None

        # Mark as globally timed out
        info.globally_timed_out = True
        info.timeout_reason = reason
        info.timeout_fence_token += 1
        return info

    async def _broadcast_global_timeout(
        self,
        job_id: str,
        info: GateJobTrackingInfo,
        timeout_msg: JobGlobalTimeout,
    ) -> None:
        """Send the global timeout to every non-terminal DC's manager."""
        for dc, manager_addr in info.dc_manager_addrs.items():
            if info.dc_status.get(dc) in {"completed", "failed", "cancelled"}:
                continue  # Skip terminal DCs

            await self._send_global_timeout(job_id, dc, manager_addr, timeout_msg)

    async def _send_global_timeout(
        self,
        job_id: str,
        dc: str,
        manager_addr: tuple[str, int],
        timeout_msg: JobGlobalTimeout,
    ) -> None:
        """Deliver the global timeout to one DC manager, warning when it is not acknowledged."""
        # send_tcp returns a failure rather than raising; anything but
        # the manager's b"ok" means the decision was not processed.
        response, _ = await self._gate.send_tcp(
            manager_addr,
            "job_global_timeout",
            timeout_msg.dump(),
            timeout=5.0,
        )
        if response != b"ok":
            await self._gate._udp_logger.log(
                ServerWarning(
                    message=f"Global timeout for job {job_id[:8]}... not delivered to DC {dc}: {response!r}",
                    node_host=self._gate._host,
                    node_port=self._gate._tcp_port,
                    node_id=self._gate._node_id.short,
                )
            )

    async def stop_tracking(self, job_id: str) -> None:
        """
        Stop tracking a job (called on cleanup).

        Removes job from tracker without declaring timeout.
        """
        async with self._lock:
            self._tracked_jobs.pop(job_id, None)

_REHOMED = (
    GateJobTrackingInfo,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
