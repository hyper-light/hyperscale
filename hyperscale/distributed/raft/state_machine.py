"""
Raft state machine for manager JobManager mutations.

Applies committed Raft log entries to the JobManager by
deserializing commands and dispatching to the correct method.
Each command type maps to exactly one handler.
"""

from typing import TYPE_CHECKING

import cloudpickle

from .logging_models import RaftError, RaftWarning
from .models import RaftCommandType, RaftLogEntry
from .models.commands import RaftCommand

if TYPE_CHECKING:
    from hyperscale.distributed.jobs.job_leadership_tracker import JobLeadershipTracker
    from hyperscale.distributed.jobs.job_manager import JobManager
    from hyperscale.logging import Logger


class RaftStateMachine:
    """
    Deterministic state machine that applies committed Raft entries
    to a JobManager instance.

    Each command type is dispatched via a lookup dict to a focused
    handler method. Unknown types are logged and skipped.
    """

    __slots__ = (
        "_job_manager",
        "_leadership_tracker",
        "_logger",
        "_node_id",
        "_handlers",
    )

    def __init__(
        self,
        job_manager: "JobManager",
        leadership_tracker: "JobLeadershipTracker",
        logger: "Logger",
        node_id: str,
    ) -> None:
        self._job_manager = job_manager
        self._leadership_tracker = leadership_tracker
        self._logger = logger
        self._node_id = node_id
        self._handlers: dict[str, object] = {
            RaftCommandType.CREATE_JOB: self._apply_create_job,
            RaftCommandType.TRACK_REMOTE_JOB: self._apply_track_remote_job,
            RaftCommandType.COMPLETE_JOB: self._apply_complete_job,
            RaftCommandType.REGISTER_WORKFLOW: self._apply_register_workflow,
            RaftCommandType.REGISTER_SUB_WORKFLOW: self._apply_register_sub_workflow,
            RaftCommandType.UPDATE_WORKFLOW_PROGRESS: self._apply_update_workflow_progress,
            RaftCommandType.RECORD_SUB_WORKFLOW_RESULT: self._apply_record_sub_workflow_result,
            RaftCommandType.MARK_WORKFLOW_COMPLETED: self._apply_mark_workflow_completed,
            RaftCommandType.MARK_WORKFLOW_FAILED: self._apply_mark_workflow_failed,
            RaftCommandType.MARK_AGGREGATION_FAILED: self._apply_mark_aggregation_failed,
            RaftCommandType.UPDATE_WORKFLOW_STATUS: self._apply_update_workflow_status,
            RaftCommandType.UPDATE_JOB_STATUS: self._apply_update_job_status,
            RaftCommandType.UPDATE_CONTEXT: self._apply_update_context,
            RaftCommandType.ASSUME_JOB_LEADERSHIP: self._apply_assume_leadership,
            RaftCommandType.TAKEOVER_JOB_LEADERSHIP: self._apply_takeover_leadership,
            RaftCommandType.RELEASE_JOB_LEADERSHIP: self._apply_release_leadership,
            RaftCommandType.INITIATE_CANCELLATION: self._apply_initiate_cancellation,
            RaftCommandType.COMPLETE_CANCELLATION: self._apply_complete_cancellation,
            RaftCommandType.PROVISION_CONFIRMED: self._apply_provision_confirmed,
            RaftCommandType.FLUSH_STATS_WINDOW: self._apply_flush_stats_window,
            RaftCommandType.NODE_MEMBERSHIP_EVENT: self._apply_node_membership_event,
            RaftCommandType.NO_OP: self._apply_no_op,
        }

    async def apply(self, entry: RaftLogEntry) -> None:
        """
        Apply a single committed log entry to the state machine.

        Deserializes the command and dispatches to the appropriate handler.
        """
        handler = self._handlers.get(entry.command_type)
        if handler is None:
            await self._logger.log(RaftWarning(
                message=f"Unknown command type: {entry.command_type}",
                node_id=self._node_id,
                job_id=entry.job_id,
            ))
            return

        command = self._deserialize(entry)
        if command is None:
            return

        await handler(command)

    def _deserialize(self, entry: RaftLogEntry) -> RaftCommand | None:
        """Deserialize command bytes. Returns None on failure."""
        if not entry.command:
            return None
        try:
            return cloudpickle.loads(entry.command)
        except Exception as error:
            # Log but do not re-raise -- Raft log is immutable
            self._logger.log(RaftError(
                message=f"Failed to deserialize command: {error}",
                node_id=self._node_id,
                job_id=entry.job_id,
            ))
            return None

    # =========================================================================
    # Job Lifecycle Handlers
    # =========================================================================

    async def _apply_create_job(self, command: RaftCommand) -> None:
        """Apply CREATE_JOB: create a new job."""
        await self._job_manager.create_job(
            submission=command.submission,
            callback_addr=command.callback_addr,
        )

    async def _apply_track_remote_job(self, command: RaftCommand) -> None:
        """Apply TRACK_REMOTE_JOB: track a job from another manager."""
        await self._job_manager.track_remote_job(
            job_id=command.job_id,
            leader_node_id=command.leader_node_id,
            leader_addr=command.leader_addr,
        )

    async def _apply_complete_job(self, command: RaftCommand) -> None:
        """Apply COMPLETE_JOB: mark job as completed."""
        await self._job_manager.complete_job(job_id=command.job_id)

    # =========================================================================
    # Workflow Registration Handlers
    # =========================================================================

    async def _apply_register_workflow(self, command: RaftCommand) -> None:
        """Apply REGISTER_WORKFLOW: register a workflow for a job."""
        await self._job_manager.register_workflow(
            job_id=command.job_id,
            workflow_id=command.workflow_id,
            name=command.workflow_name,
            workflow=command.workflow,
        )

    async def _apply_register_sub_workflow(self, command: RaftCommand) -> None:
        """Apply REGISTER_SUB_WORKFLOW: register sub-workflow dispatch."""
        await self._job_manager.register_sub_workflow(
            job_id=command.job_id,
            workflow_id=command.workflow_id,
            worker_id=command.worker_id,
            cores_allocated=command.cores_allocated,
        )

    # =========================================================================
    # Progress and Results Handlers
    # =========================================================================

    async def _apply_update_workflow_progress(self, command: RaftCommand) -> None:
        """Apply UPDATE_WORKFLOW_PROGRESS: update sub-workflow progress."""
        await self._job_manager.update_workflow_progress(
            sub_workflow_token=command.sub_workflow_token,
            progress=command.progress,
        )

    async def _apply_record_sub_workflow_result(self, command: RaftCommand) -> None:
        """Apply RECORD_SUB_WORKFLOW_RESULT: record final result."""
        await self._job_manager.record_sub_workflow_result(
            sub_workflow_token=command.sub_workflow_token,
            result=command.result,
        )

    # =========================================================================
    # Workflow Completion Handlers
    # =========================================================================

    async def _apply_mark_workflow_completed(self, command: RaftCommand) -> None:
        """Apply MARK_WORKFLOW_COMPLETED: mark workflow completed."""
        await self._job_manager.mark_workflow_completed(
            workflow_token=command.workflow_token,
            from_worker=command.from_worker,
        )

    async def _apply_mark_workflow_failed(self, command: RaftCommand) -> None:
        """Apply MARK_WORKFLOW_FAILED: mark workflow failed."""
        await self._job_manager.mark_workflow_failed(
            workflow_token=command.workflow_token,
            error=command.error,
        )

    async def _apply_mark_aggregation_failed(self, command: RaftCommand) -> None:
        """Apply MARK_AGGREGATION_FAILED: mark aggregation failed."""
        await self._job_manager.mark_aggregation_failed(
            workflow_token=command.workflow_token,
            error=command.error,
        )

    async def _apply_update_workflow_status(self, command: RaftCommand) -> None:
        """Apply UPDATE_WORKFLOW_STATUS: update workflow status."""
        await self._job_manager.update_workflow_status(
            job_id=command.job_id,
            workflow_token=command.workflow_token,
            new_status=command.new_status,
            error=command.error,
        )

    # =========================================================================
    # State Management Handlers
    # =========================================================================

    async def _apply_update_job_status(self, command: RaftCommand) -> None:
        """Apply UPDATE_JOB_STATUS: update job status string."""
        await self._job_manager.update_job_status(
            job_token=command.job_token,
            status=command.status,
        )

    async def _apply_update_context(self, command: RaftCommand) -> None:
        """Apply UPDATE_CONTEXT: merge context updates into job."""
        await self._job_manager.update_context(
            job_token=command.job_token,
            updates=command.context_updates,
        )

    # =========================================================================
    # Job Leadership Handlers
    # =========================================================================

    async def _apply_assume_leadership(self, command: RaftCommand) -> None:
        """Apply ASSUME_JOB_LEADERSHIP: this node assumes leadership."""
        self._leadership_tracker.assume_leadership(
            job_id=command.job_id,
            metadata=command.metadata,
            initial_token=command.initial_token,
        )

    async def _apply_takeover_leadership(self, command: RaftCommand) -> None:
        """Apply TAKEOVER_JOB_LEADERSHIP: this node takes over leadership."""
        self._leadership_tracker.takeover_leadership(
            job_id=command.job_id,
            metadata=command.metadata,
        )

    async def _apply_release_leadership(self, command: RaftCommand) -> None:
        """Apply RELEASE_JOB_LEADERSHIP: release leadership of a job."""
        self._leadership_tracker.release_leadership(job_id=command.job_id)

    # =========================================================================
    # Cancellation Handlers
    # =========================================================================

    async def _apply_initiate_cancellation(self, command: RaftCommand) -> None:
        """Apply INITIATE_CANCELLATION: begin cancellation of a job."""
        pass

    async def _apply_complete_cancellation(self, command: RaftCommand) -> None:
        """Apply COMPLETE_CANCELLATION: finalize cancellation of a job."""
        pass

    # =========================================================================
    # Provisioning Handlers
    # =========================================================================

    async def _apply_provision_confirmed(self, command: RaftCommand) -> None:
        """Apply PROVISION_CONFIRMED: record a provision confirmation."""
        pass

    # =========================================================================
    # Stats Handlers
    # =========================================================================

    async def _apply_flush_stats_window(self, command: RaftCommand) -> None:
        """Apply FLUSH_STATS_WINDOW: apply aggregated stats window data."""
        pass

    # =========================================================================
    # Membership Handlers
    # =========================================================================

    async def _apply_node_membership_event(self, command: RaftCommand) -> None:
        """Apply NODE_MEMBERSHIP_EVENT: record a cluster membership change."""
        pass

    # =========================================================================
    # Raft Control
    # =========================================================================

    async def _apply_no_op(self, command: RaftCommand) -> None:
        """Apply NO_OP: no state change. Used for leadership confirmation."""
        pass
