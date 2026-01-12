"""
Job leadership transfer TCP handler for worker.

Handles job leadership transfer notifications from managers (AD-31, Section 8).
"""

import time
from typing import TYPE_CHECKING

from hyperscale.distributed.models import (
    JobLeaderWorkerTransfer,
    JobLeaderWorkerTransferAck,
    PendingTransfer,
)
from hyperscale.logging.hyperscale_logging_models import (
    ServerDebug,
    ServerInfo,
    ServerWarning,
)

if TYPE_CHECKING:
    from ..server import WorkerServer


class JobLeaderTransferHandler:
    """
    Handler for job leadership transfer notifications from managers.

    Updates workflow job leader mappings when manager leadership changes.
    Preserves AD-31 and Section 8 robustness requirements.
    """

    def __init__(self, server: "WorkerServer") -> None:
        """
        Initialize handler with server reference.

        Args:
            server: WorkerServer instance for state access
        """
        self._server = server

    async def handle(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """
        Handle job leadership transfer notification from manager.

        Updates _workflow_job_leader mapping to route progress to new manager.

        Section 8 robustness:
        - 8.1: Uses per-job lock to prevent race conditions
        - 8.2: Validates fence token and manager legitimacy
        - 8.3: Stores pending transfers for late-arriving workflows
        - 8.4: Returns detailed ack with workflow states
        - 8.6: Updates transfer metrics
        - 8.7: Detailed logging

        Orphan handling (Section 2.7):
        - Clears workflows from _orphaned_workflows when transfer arrives

        Args:
            addr: Source address (manager TCP address)
            data: Serialized JobLeaderWorkerTransfer
            clock_time: Logical clock time

        Returns:
            Serialized JobLeaderWorkerTransferAck
        """
        self._server._transfer_metrics_received += 1
        transfer_start_time = time.monotonic()

        try:
            transfer = JobLeaderWorkerTransfer.load(data)
            job_id = transfer.job_id

            await self._log_transfer_start(transfer, job_id)

            # 8.1: Acquire per-job lock
            job_lock = await self._server._get_job_transfer_lock(job_id)
            async with job_lock:
                # 8.2: Validate transfer
                rejection = await self._validate_and_reject_transfer(transfer, job_id)
                if rejection is not None:
                    return rejection

                # Update fence token
                self._server._job_fence_tokens[job_id] = transfer.fence_token

                # Process workflow routing updates
                (
                    workflows_updated,
                    workflows_rescued,
                    workflows_not_found,
                    workflow_states,
                ) = self._apply_workflow_routing_updates(transfer)

                # 8.3: Store pending transfer for late-arriving workflows
                if workflows_not_found:
                    self._server._pending_transfers[job_id] = PendingTransfer(
                        job_id=job_id,
                        workflow_ids=workflows_not_found,
                        new_manager_id=transfer.new_manager_id,
                        new_manager_addr=transfer.new_manager_addr,
                        fence_token=transfer.fence_token,
                        old_manager_id=transfer.old_manager_id,
                        received_at=time.monotonic(),
                    )

                # 8.6: Update metrics
                self._server._transfer_metrics_accepted += 1

                # 8.7: Detailed logging
                await self._log_transfer_result(
                    transfer,
                    job_id,
                    workflows_updated,
                    workflows_rescued,
                    workflows_not_found,
                    transfer_start_time,
                )

                # 8.4: Return detailed ack with workflow states
                return JobLeaderWorkerTransferAck(
                    job_id=job_id,
                    worker_id=self._server._node_id.full,
                    workflows_updated=workflows_updated,
                    accepted=True,
                    fence_token_received=transfer.fence_token,
                    workflow_states=workflow_states,
                ).dump()

        except Exception as error:
            self._server._transfer_metrics_rejected_other += 1
            return JobLeaderWorkerTransferAck(
                job_id="unknown",
                worker_id=self._server._node_id.full,
                workflows_updated=0,
                accepted=False,
                rejection_reason=str(error),
                fence_token_received=0,
            ).dump()

    async def _log_transfer_start(
        self, transfer: JobLeaderWorkerTransfer, job_id: str
    ) -> None:
        """Log the start of transfer processing."""
        old_manager_str = (
            transfer.old_manager_id[:8] if transfer.old_manager_id else "unknown"
        )
        await self._server._udp_logger.log(
            ServerDebug(
                message=(
                    f"Processing job leadership transfer: job={job_id[:8]}..., "
                    f"new_manager={transfer.new_manager_id[:8]}..., "
                    f"old_manager={old_manager_str}..., "
                    f"fence_token={transfer.fence_token}, "
                    f"workflows={len(transfer.workflow_ids)}"
                ),
                node_host=self._server._host,
                node_port=self._server._tcp_port,
                node_id=self._server._node_id.short,
            )
        )

    async def _validate_and_reject_transfer(
        self, transfer: JobLeaderWorkerTransfer, job_id: str
    ) -> bytes | None:
        """Validate transfer and return rejection response if invalid."""
        # Validate fence token
        fence_valid, fence_reason = self._server._validate_transfer_fence_token(
            job_id, transfer.fence_token
        )
        if not fence_valid:
            self._server._transfer_metrics_rejected_stale_token += 1
            await self._server._udp_logger.log(
                ServerWarning(
                    message=f"Rejected job leadership transfer for job {job_id[:8]}...: {fence_reason}",
                    node_host=self._server._host,
                    node_port=self._server._tcp_port,
                    node_id=self._server._node_id.short,
                )
            )
            return JobLeaderWorkerTransferAck(
                job_id=job_id,
                worker_id=self._server._node_id.full,
                workflows_updated=0,
                accepted=False,
                rejection_reason=fence_reason,
                fence_token_received=transfer.fence_token,
            ).dump()

        # Validate new manager is known
        manager_valid, manager_reason = self._server._validate_transfer_manager(
            transfer.new_manager_id
        )
        if not manager_valid:
            self._server._transfer_metrics_rejected_unknown_manager += 1
            await self._server._udp_logger.log(
                ServerWarning(
                    message=f"Rejected job leadership transfer for job {job_id[:8]}...: {manager_reason}",
                    node_host=self._server._host,
                    node_port=self._server._tcp_port,
                    node_id=self._server._node_id.short,
                )
            )
            return JobLeaderWorkerTransferAck(
                job_id=job_id,
                worker_id=self._server._node_id.full,
                workflows_updated=0,
                accepted=False,
                rejection_reason=manager_reason,
                fence_token_received=transfer.fence_token,
            ).dump()

        return None

    def _apply_workflow_routing_updates(
        self, transfer: JobLeaderWorkerTransfer
    ) -> tuple[int, int, list[str], dict[str, str]]:
        """Apply routing updates for workflows in the transfer."""
        active = self._server._active_workflows
        orphaned = self._server._orphaned_workflows
        job_leader = self._server._workflow_job_leader

        # Partition workflows into found vs not found (comprehension)
        workflows_not_found = [
            wf_id for wf_id in transfer.workflow_ids if wf_id not in active
        ]
        found_workflows = [wf_id for wf_id in transfer.workflow_ids if wf_id in active]

        # Update job leader and collect states (comprehension with side effects via walrus)
        workflow_states = {}
        workflows_rescued = 0
        for workflow_id in found_workflows:
            job_leader[workflow_id] = transfer.new_manager_addr
            workflow_states[workflow_id] = active[workflow_id].status
            # Clear orphan status if present (Section 2.7)
            if workflow_id in orphaned:
                del orphaned[workflow_id]
                workflows_rescued += 1

        return (
            len(found_workflows),
            workflows_rescued,
            workflows_not_found,
            workflow_states,
        )

    async def _log_transfer_result(
        self,
        transfer: JobLeaderWorkerTransfer,
        job_id: str,
        workflows_updated: int,
        workflows_rescued: int,
        workflows_not_found: list[str],
        start_time: float,
    ) -> None:
        """Log transfer result details."""
        transfer_duration_ms = (time.monotonic() - start_time) * 1000

        if workflows_updated > 0 or workflows_not_found:
            rescue_msg = ""
            if workflows_rescued > 0:
                rescue_msg = f" ({workflows_rescued} rescued from orphan state)"

            pending_msg = ""
            if workflows_not_found:
                pending_msg = f" ({len(workflows_not_found)} stored as pending)"

            await self._server._udp_logger.log(
                ServerInfo(
                    message=(
                        f"Job {job_id[:8]}... leadership transfer: "
                        f"updated {workflows_updated} workflow(s) to route to {transfer.new_manager_addr}"
                        f"{rescue_msg}{pending_msg} "
                        f"[latency={transfer_duration_ms:.1f}ms]"
                    ),
                    node_host=self._server._host,
                    node_port=self._server._tcp_port,
                    node_id=self._server._node_id.short,
                )
            )
