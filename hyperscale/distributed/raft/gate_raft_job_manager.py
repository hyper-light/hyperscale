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
    from hyperscale.distributed.models import GlobalJobStatus, JobFinalResult
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
