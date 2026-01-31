"""
Raft-backed job manager wrapper for manager nodes.

Routes all 13 mutating JobManager operations through Raft consensus.
Read operations bypass Raft for eventual consistency reads.
"""

from typing import TYPE_CHECKING, Any

from .logging_models import RaftWarning
from .models import RaftCommandType
from .models.commands import RaftCommand

if TYPE_CHECKING:
    from hyperscale.core.graph.workflow import Workflow
    from hyperscale.distributed.models import (
        JobSubmission,
        WorkflowFinalResult,
        WorkflowProgress,
        WorkflowStatus,
    )
    from hyperscale.logging import Logger

    from .raft_consensus import RaftConsensus


class RaftJobManager:
    """
    Wraps JobManager mutations through Raft consensus.

    Every mutating method serializes a RaftCommand and proposes it
    through the RaftConsensus coordinator. The state machine applies
    the command to the underlying JobManager once committed.

    Read methods delegate directly to the underlying JobManager.
    """

    __slots__ = (
        "_consensus",
        "_logger",
        "_node_id",
    )

    def __init__(
        self,
        consensus: "RaftConsensus",
        logger: "Logger",
        node_id: str,
    ) -> None:
        self._consensus = consensus
        self._logger = logger
        self._node_id = node_id

    # =========================================================================
    # Internal Helper
    # =========================================================================

    async def _propose(
        self,
        job_id: str,
        command: RaftCommand,
    ) -> bool:
        """
        Propose a command through Raft. Returns True on success.

        Logs a warning on failure (not leader, at capacity, etc.).
        """
        success, _index = await self._consensus.propose_command(job_id, command)
        if not success:
            await self._logger.log(RaftWarning(
                message=f"Raft proposal rejected for {command.command_type.value} on job {job_id}",
                node_id=self._node_id,
                job_id=job_id,
            ))
        return success

    # =========================================================================
    # Job Lifecycle
    # =========================================================================

    async def create_job(
        self,
        job_id: str,
        submission: "JobSubmission",
        callback_addr: tuple[str, int] | None = None,
    ) -> bool:
        """Propose CREATE_JOB through Raft."""
        return await self._propose(job_id, RaftCommand(
            command_type=RaftCommandType.CREATE_JOB,
            submission=submission,
            callback_addr=callback_addr,
            job_id=job_id,
        ))

    async def track_remote_job(
        self,
        job_id: str,
        leader_node_id: str,
        leader_addr: tuple[str, int],
    ) -> bool:
        """Propose TRACK_REMOTE_JOB through Raft."""
        return await self._propose(job_id, RaftCommand(
            command_type=RaftCommandType.TRACK_REMOTE_JOB,
            job_id=job_id,
            leader_node_id=leader_node_id,
            leader_addr=leader_addr,
        ))

    async def complete_job(self, job_id: str) -> bool:
        """Propose COMPLETE_JOB through Raft."""
        return await self._propose(job_id, RaftCommand(
            command_type=RaftCommandType.COMPLETE_JOB,
            job_id=job_id,
        ))

    # =========================================================================
    # Workflow Registration
    # =========================================================================

    async def register_workflow(
        self,
        job_id: str,
        workflow_id: str,
        name: str,
        workflow: "Workflow | None" = None,
    ) -> bool:
        """Propose REGISTER_WORKFLOW through Raft."""
        return await self._propose(job_id, RaftCommand(
            command_type=RaftCommandType.REGISTER_WORKFLOW,
            job_id=job_id,
            workflow_id=workflow_id,
            workflow_name=name,
            workflow=workflow,
        ))

    async def register_sub_workflow(
        self,
        job_id: str,
        workflow_id: str,
        worker_id: str,
        cores_allocated: int,
    ) -> bool:
        """Propose REGISTER_SUB_WORKFLOW through Raft."""
        return await self._propose(job_id, RaftCommand(
            command_type=RaftCommandType.REGISTER_SUB_WORKFLOW,
            job_id=job_id,
            workflow_id=workflow_id,
            worker_id=worker_id,
            cores_allocated=cores_allocated,
        ))

    # =========================================================================
    # Progress and Results
    # =========================================================================

    async def update_workflow_progress(
        self,
        job_id: str,
        sub_workflow_token: str,
        progress: "WorkflowProgress",
    ) -> bool:
        """Propose UPDATE_WORKFLOW_PROGRESS through Raft."""
        return await self._propose(job_id, RaftCommand(
            command_type=RaftCommandType.UPDATE_WORKFLOW_PROGRESS,
            job_id=job_id,
            sub_workflow_token=sub_workflow_token,
            progress=progress,
        ))

    async def record_sub_workflow_result(
        self,
        job_id: str,
        sub_workflow_token: str,
        result: "WorkflowFinalResult",
    ) -> bool:
        """Propose RECORD_SUB_WORKFLOW_RESULT through Raft."""
        return await self._propose(job_id, RaftCommand(
            command_type=RaftCommandType.RECORD_SUB_WORKFLOW_RESULT,
            job_id=job_id,
            sub_workflow_token=sub_workflow_token,
            result=result,
        ))

    # =========================================================================
    # Workflow Completion
    # =========================================================================

    async def mark_workflow_completed(
        self,
        job_id: str,
        workflow_token: str,
        from_worker: bool = True,
    ) -> bool:
        """Propose MARK_WORKFLOW_COMPLETED through Raft."""
        return await self._propose(job_id, RaftCommand(
            command_type=RaftCommandType.MARK_WORKFLOW_COMPLETED,
            job_id=job_id,
            workflow_token=workflow_token,
            from_worker=from_worker,
        ))

    async def mark_workflow_failed(
        self,
        job_id: str,
        workflow_token: str,
        error: str,
    ) -> bool:
        """Propose MARK_WORKFLOW_FAILED through Raft."""
        return await self._propose(job_id, RaftCommand(
            command_type=RaftCommandType.MARK_WORKFLOW_FAILED,
            job_id=job_id,
            workflow_token=workflow_token,
            error=error,
        ))

    async def mark_aggregation_failed(
        self,
        job_id: str,
        workflow_token: str,
        error: str,
    ) -> bool:
        """Propose MARK_AGGREGATION_FAILED through Raft."""
        return await self._propose(job_id, RaftCommand(
            command_type=RaftCommandType.MARK_AGGREGATION_FAILED,
            job_id=job_id,
            workflow_token=workflow_token,
            error=error,
        ))

    async def update_workflow_status(
        self,
        job_id: str,
        workflow_token: str,
        new_status: "WorkflowStatus",
        error: str | None = None,
    ) -> bool:
        """Propose UPDATE_WORKFLOW_STATUS through Raft."""
        return await self._propose(job_id, RaftCommand(
            command_type=RaftCommandType.UPDATE_WORKFLOW_STATUS,
            job_id=job_id,
            workflow_token=workflow_token,
            new_status=new_status,
            error=error,
        ))

    # =========================================================================
    # State Management
    # =========================================================================

    async def update_job_status(
        self,
        job_id: str,
        job_token: str,
        status: str,
    ) -> bool:
        """Propose UPDATE_JOB_STATUS through Raft."""
        return await self._propose(job_id, RaftCommand(
            command_type=RaftCommandType.UPDATE_JOB_STATUS,
            job_id=job_id,
            job_token=job_token,
            status=status,
        ))

    async def update_context(
        self,
        job_id: str,
        job_token: str,
        updates: dict[str, Any],
    ) -> bool:
        """Propose UPDATE_CONTEXT through Raft."""
        return await self._propose(job_id, RaftCommand(
            command_type=RaftCommandType.UPDATE_CONTEXT,
            job_id=job_id,
            job_token=job_token,
            context_updates=updates,
        ))

    # =========================================================================
    # Job Leadership
    # =========================================================================

    async def assume_job_leadership(
        self,
        job_id: str,
        metadata: Any = None,
        initial_token: int = 1,
    ) -> bool:
        """Propose ASSUME_JOB_LEADERSHIP through Raft."""
        return await self._propose(job_id, RaftCommand(
            command_type=RaftCommandType.ASSUME_JOB_LEADERSHIP,
            job_id=job_id,
            metadata=metadata,
            initial_token=initial_token,
        ))

    async def takeover_job_leadership(
        self,
        job_id: str,
        metadata: Any = None,
    ) -> bool:
        """Propose TAKEOVER_JOB_LEADERSHIP through Raft."""
        return await self._propose(job_id, RaftCommand(
            command_type=RaftCommandType.TAKEOVER_JOB_LEADERSHIP,
            job_id=job_id,
            metadata=metadata,
        ))

    async def release_job_leadership(self, job_id: str) -> bool:
        """Propose RELEASE_JOB_LEADERSHIP through Raft."""
        return await self._propose(job_id, RaftCommand(
            command_type=RaftCommandType.RELEASE_JOB_LEADERSHIP,
            job_id=job_id,
        ))

    # =========================================================================
    # Cancellation
    # =========================================================================

    async def initiate_cancellation(
        self,
        job_id: str,
        pending_workflows: set[str] | None = None,
    ) -> bool:
        """Propose INITIATE_CANCELLATION through Raft."""
        return await self._propose(job_id, RaftCommand(
            command_type=RaftCommandType.INITIATE_CANCELLATION,
            job_id=job_id,
            pending_workflows=pending_workflows,
        ))

    async def complete_cancellation(self, job_id: str) -> bool:
        """Propose COMPLETE_CANCELLATION through Raft."""
        return await self._propose(job_id, RaftCommand(
            command_type=RaftCommandType.COMPLETE_CANCELLATION,
            job_id=job_id,
        ))

    # =========================================================================
    # Provisioning
    # =========================================================================

    async def provision_confirmed(
        self,
        job_id: str,
        confirming_node_id: str,
    ) -> bool:
        """Propose PROVISION_CONFIRMED through Raft."""
        return await self._propose(job_id, RaftCommand(
            command_type=RaftCommandType.PROVISION_CONFIRMED,
            job_id=job_id,
            confirming_node_id=confirming_node_id,
        ))

    # =========================================================================
    # Stats
    # =========================================================================

    async def flush_stats_window(
        self,
        job_id: str,
        stats_data: bytes,
    ) -> bool:
        """Propose FLUSH_STATS_WINDOW through Raft."""
        return await self._propose(job_id, RaftCommand(
            command_type=RaftCommandType.FLUSH_STATS_WINDOW,
            job_id=job_id,
            stats_data=stats_data,
        ))

    # =========================================================================
    # Membership
    # =========================================================================

    async def node_membership_event(
        self,
        job_id: str,
        event_type: str,
        node_id: str,
        node_addr: tuple[str, int] | None = None,
    ) -> bool:
        """Propose NODE_MEMBERSHIP_EVENT through Raft."""
        return await self._propose(job_id, RaftCommand(
            command_type=RaftCommandType.NODE_MEMBERSHIP_EVENT,
            job_id=job_id,
            event_type=event_type,
            node_id=node_id,
            node_addr=node_addr,
        ))
