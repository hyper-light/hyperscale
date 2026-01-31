"""
Raft-backed job manager wrapper for gate nodes.

Routes all 9 mutating GateJobManager operations through Raft consensus.
Read operations bypass Raft for eventual consistency reads.
"""

from typing import TYPE_CHECKING

from .logging_models import RaftWarning
from .models import GateRaftCommandType
from .models.gate_commands import GateRaftCommand

if TYPE_CHECKING:
    from typing import Any

    from hyperscale.distributed.models import (
        GlobalJobStatus,
        JobFinalResult,
        JobSubmission,
        WorkflowResultPush,
    )
    from hyperscale.logging import Logger

    from .gate_raft_consensus import GateRaftConsensus


class GateRaftJobManager:
    """
    Wraps GateJobManager mutations through Raft consensus.

    Every mutating method serializes a GateRaftCommand and proposes it
    through the GateRaftConsensus coordinator. The state machine applies
    the command to the underlying GateJobManager once committed.

    Read methods delegate directly to the underlying GateJobManager.
    """

    __slots__ = (
        "_consensus",
        "_logger",
        "_node_id",
    )

    def __init__(
        self,
        consensus: "GateRaftConsensus",
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
        command: GateRaftCommand,
    ) -> bool:
        """
        Propose a command through Raft. Returns True on success.

        Logs a warning on failure (not leader, at capacity, etc.).
        """
        success, _index = await self._consensus.propose_command(job_id, command)
        if not success:
            await self._logger.log(RaftWarning(
                message=f"Gate Raft proposal rejected for {command.command_type.value} on job {job_id}",
                node_id=self._node_id,
                job_id=job_id,
            ))
        return success

    # =========================================================================
    # Job CRUD
    # =========================================================================

    async def set_job(
        self,
        job_id: str,
        job: "GlobalJobStatus",
    ) -> bool:
        """Propose SET_JOB through Raft."""
        return await self._propose(job_id, GateRaftCommand(
            command_type=GateRaftCommandType.SET_JOB,
            job_id=job_id,
            job=job,
        ))

    async def delete_job(self, job_id: str) -> bool:
        """Propose DELETE_JOB through Raft."""
        return await self._propose(job_id, GateRaftCommand(
            command_type=GateRaftCommandType.DELETE_JOB,
            job_id=job_id,
        ))

    # =========================================================================
    # Target Datacenter Management
    # =========================================================================

    async def set_target_dcs(
        self,
        job_id: str,
        dcs: set[str],
    ) -> bool:
        """Propose SET_TARGET_DCS through Raft."""
        return await self._propose(job_id, GateRaftCommand(
            command_type=GateRaftCommandType.SET_TARGET_DCS,
            job_id=job_id,
            target_dcs=dcs,
        ))

    async def add_target_dc(
        self,
        job_id: str,
        dc_id: str,
    ) -> bool:
        """Propose ADD_TARGET_DC through Raft."""
        return await self._propose(job_id, GateRaftCommand(
            command_type=GateRaftCommandType.ADD_TARGET_DC,
            job_id=job_id,
            dc_id=dc_id,
        ))

    # =========================================================================
    # DC Results
    # =========================================================================

    async def set_dc_result(
        self,
        job_id: str,
        dc_id: str,
        result: "JobFinalResult",
    ) -> bool:
        """Propose SET_DC_RESULT through Raft."""
        return await self._propose(job_id, GateRaftCommand(
            command_type=GateRaftCommandType.SET_DC_RESULT,
            job_id=job_id,
            dc_id=dc_id,
            dc_result=result,
        ))

    # =========================================================================
    # Callback Management
    # =========================================================================

    async def set_callback(
        self,
        job_id: str,
        addr: tuple[str, int],
    ) -> bool:
        """Propose SET_CALLBACK through Raft."""
        return await self._propose(job_id, GateRaftCommand(
            command_type=GateRaftCommandType.SET_CALLBACK,
            job_id=job_id,
            callback_addr=addr,
        ))

    async def remove_callback(self, job_id: str) -> bool:
        """Propose REMOVE_CALLBACK through Raft."""
        return await self._propose(job_id, GateRaftCommand(
            command_type=GateRaftCommandType.REMOVE_CALLBACK,
            job_id=job_id,
        ))

    # =========================================================================
    # Fence Token Management
    # =========================================================================

    async def set_fence_token(
        self,
        job_id: str,
        token: int,
    ) -> bool:
        """Propose SET_FENCE_TOKEN through Raft."""
        return await self._propose(job_id, GateRaftCommand(
            command_type=GateRaftCommandType.SET_FENCE_TOKEN,
            job_id=job_id,
            fence_token=token,
        ))

    # =========================================================================
    # Cleanup
    # =========================================================================

    async def cleanup_old_jobs(
        self,
        job_id: str,
        max_age_seconds: float,
    ) -> bool:
        """Propose CLEANUP_OLD_JOBS through Raft."""
        return await self._propose(job_id, GateRaftCommand(
            command_type=GateRaftCommandType.CLEANUP_OLD_JOBS,
            job_id=job_id,
            max_age_seconds=max_age_seconds,
        ))

    # =========================================================================
    # Gate Leadership
    # =========================================================================

    async def assume_gate_leadership(
        self,
        job_id: str,
        metadata: "Any" = None,
        initial_token: int = 1,
    ) -> bool:
        """Propose ASSUME_GATE_LEADERSHIP through Raft."""
        return await self._propose(job_id, GateRaftCommand(
            command_type=GateRaftCommandType.ASSUME_GATE_LEADERSHIP,
            job_id=job_id,
            metadata=metadata,
            initial_token=initial_token,
        ))

    async def takeover_gate_leadership(
        self,
        job_id: str,
        metadata: "Any" = None,
    ) -> bool:
        """Propose TAKEOVER_GATE_LEADERSHIP through Raft."""
        return await self._propose(job_id, GateRaftCommand(
            command_type=GateRaftCommandType.TAKEOVER_GATE_LEADERSHIP,
            job_id=job_id,
            metadata=metadata,
        ))

    async def release_gate_leadership(self, job_id: str) -> bool:
        """Propose RELEASE_GATE_LEADERSHIP through Raft."""
        return await self._propose(job_id, GateRaftCommand(
            command_type=GateRaftCommandType.RELEASE_GATE_LEADERSHIP,
            job_id=job_id,
        ))

    async def process_leadership_claim(
        self,
        job_id: str,
        claimer_id: str,
        claimer_addr: tuple[str, int],
        fencing_token: int,
        metadata: "Any" = None,
    ) -> bool:
        """Propose PROCESS_LEADERSHIP_CLAIM through Raft."""
        return await self._propose(job_id, GateRaftCommand(
            command_type=GateRaftCommandType.PROCESS_LEADERSHIP_CLAIM,
            job_id=job_id,
            claimer_id=claimer_id,
            claimer_addr=claimer_addr,
            fencing_token=fencing_token,
            metadata=metadata,
        ))

    # =========================================================================
    # DC Manager Tracking
    # =========================================================================

    async def update_dc_manager(
        self,
        job_id: str,
        dc_id: str,
        manager_id: str,
        manager_addr: tuple[str, int],
        fencing_token: int,
    ) -> bool:
        """Propose UPDATE_DC_MANAGER through Raft."""
        return await self._propose(job_id, GateRaftCommand(
            command_type=GateRaftCommandType.UPDATE_DC_MANAGER,
            job_id=job_id,
            dc_id=dc_id,
            manager_id=manager_id,
            manager_addr=manager_addr,
            fencing_token=fencing_token,
        ))

    async def release_dc_managers(self, job_id: str) -> bool:
        """Propose RELEASE_DC_MANAGERS through Raft."""
        return await self._propose(job_id, GateRaftCommand(
            command_type=GateRaftCommandType.RELEASE_DC_MANAGERS,
            job_id=job_id,
        ))

    # =========================================================================
    # Lease Management
    # =========================================================================

    async def create_lease(
        self,
        job_id: str,
        datacenter: str,
        lease_holder: str,
        fence_token: int,
        expires_at: float,
        lease_version: int = 0,
    ) -> bool:
        """Propose CREATE_LEASE through Raft."""
        return await self._propose(job_id, GateRaftCommand(
            command_type=GateRaftCommandType.CREATE_LEASE,
            job_id=job_id,
            datacenter=datacenter,
            lease_holder=lease_holder,
            fence_token=fence_token,
            expires_at=expires_at,
            lease_version=lease_version,
        ))

    async def release_lease(self, job_id: str) -> bool:
        """Propose RELEASE_LEASE through Raft."""
        return await self._propose(job_id, GateRaftCommand(
            command_type=GateRaftCommandType.RELEASE_LEASE,
            job_id=job_id,
        ))

    # =========================================================================
    # Job Submission State
    # =========================================================================

    async def set_job_submission(
        self,
        job_id: str,
        submission: "JobSubmission",
    ) -> bool:
        """Propose SET_JOB_SUBMISSION through Raft."""
        return await self._propose(job_id, GateRaftCommand(
            command_type=GateRaftCommandType.SET_JOB_SUBMISSION,
            job_id=job_id,
            submission=submission,
        ))

    # =========================================================================
    # Workflow DC Results
    # =========================================================================

    async def set_workflow_dc_result(
        self,
        job_id: str,
        dc_id: str,
        workflow_id: str,
        workflow_result: "WorkflowResultPush",
    ) -> bool:
        """Propose SET_WORKFLOW_DC_RESULT through Raft."""
        return await self._propose(job_id, GateRaftCommand(
            command_type=GateRaftCommandType.SET_WORKFLOW_DC_RESULT,
            job_id=job_id,
            dc_id=dc_id,
            workflow_id=workflow_id,
            workflow_result=workflow_result,
        ))

    # =========================================================================
    # Membership
    # =========================================================================

    async def gate_membership_event(
        self,
        job_id: str,
        event_type: str,
        node_id: str,
        node_addr: tuple[str, int] | None = None,
    ) -> bool:
        """Propose GATE_MEMBERSHIP_EVENT through Raft."""
        return await self._propose(job_id, GateRaftCommand(
            command_type=GateRaftCommandType.GATE_MEMBERSHIP_EVENT,
            job_id=job_id,
            event_type=event_type,
            node_id=node_id,
            node_addr=node_addr,
        ))
