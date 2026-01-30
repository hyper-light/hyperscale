"""
Raft state machine for gate GateJobManager mutations.

Applies committed Raft log entries to the GateJobManager by
deserializing commands and dispatching to the correct method.
Each command type maps to exactly one handler.
"""

from typing import TYPE_CHECKING

import cloudpickle

from .logging_models import RaftError, RaftWarning
from .models import GateRaftCommandType, RaftLogEntry
from .models.gate_commands import GateRaftCommand

if TYPE_CHECKING:
    from hyperscale.distributed.jobs.gates.gate_job_manager import GateJobManager
    from hyperscale.logging import Logger


class GateStateMachine:
    """
    Deterministic state machine that applies committed Raft entries
    to a GateJobManager instance.

    Each command type is dispatched via a lookup dict to a focused
    handler method. Unknown types are logged and skipped.
    """

    __slots__ = (
        "_job_manager",
        "_logger",
        "_node_id",
        "_handlers",
    )

    def __init__(
        self,
        job_manager: "GateJobManager",
        logger: "Logger",
        node_id: str,
    ) -> None:
        self._job_manager = job_manager
        self._logger = logger
        self._node_id = node_id
        self._handlers: dict[str, object] = {
            GateRaftCommandType.SET_JOB: self._apply_set_job,
            GateRaftCommandType.DELETE_JOB: self._apply_delete_job,
            GateRaftCommandType.SET_TARGET_DCS: self._apply_set_target_dcs,
            GateRaftCommandType.ADD_TARGET_DC: self._apply_add_target_dc,
            GateRaftCommandType.SET_DC_RESULT: self._apply_set_dc_result,
            GateRaftCommandType.SET_CALLBACK: self._apply_set_callback,
            GateRaftCommandType.REMOVE_CALLBACK: self._apply_remove_callback,
            GateRaftCommandType.SET_FENCE_TOKEN: self._apply_set_fence_token,
            GateRaftCommandType.CLEANUP_OLD_JOBS: self._apply_cleanup_old_jobs,
            GateRaftCommandType.NO_OP: self._apply_no_op,
        }

    async def apply(self, entry: RaftLogEntry) -> None:
        """
        Apply a single committed log entry to the state machine.

        Deserializes the command and dispatches to the appropriate handler.
        """
        handler = self._handlers.get(entry.command_type)
        if handler is None:
            await self._logger.log(RaftWarning(
                message=f"Unknown gate command type: {entry.command_type}",
                node_id=self._node_id,
                job_id=entry.job_id,
            ))
            return

        command = self._deserialize(entry)
        if command is None:
            return

        await handler(command)

    def _deserialize(self, entry: RaftLogEntry) -> GateRaftCommand | None:
        """Deserialize command bytes. Returns None on failure."""
        if not entry.command:
            return None
        try:
            return cloudpickle.loads(entry.command)
        except Exception as error:
            self._logger.log(RaftError(
                message=f"Failed to deserialize gate command: {error}",
                node_id=self._node_id,
                job_id=entry.job_id,
            ))
            return None

    # =========================================================================
    # Job CRUD Handlers
    # =========================================================================

    async def _apply_set_job(self, command: GateRaftCommand) -> None:
        """Apply SET_JOB: set or update a job."""
        self._job_manager.set_job(
            job_id=command.job_id,
            job=command.job,
        )

    async def _apply_delete_job(self, command: GateRaftCommand) -> None:
        """Apply DELETE_JOB: delete a job and associated data."""
        self._job_manager.delete_job(job_id=command.job_id)

    # =========================================================================
    # Target DC Handlers
    # =========================================================================

    async def _apply_set_target_dcs(self, command: GateRaftCommand) -> None:
        """Apply SET_TARGET_DCS: replace target datacenter set."""
        self._job_manager.set_target_dcs(
            job_id=command.job_id,
            dcs=command.target_dcs,
        )

    async def _apply_add_target_dc(self, command: GateRaftCommand) -> None:
        """Apply ADD_TARGET_DC: add a single datacenter."""
        self._job_manager.add_target_dc(
            job_id=command.job_id,
            dc_id=command.dc_id,
        )

    # =========================================================================
    # DC Results Handler
    # =========================================================================

    async def _apply_set_dc_result(self, command: GateRaftCommand) -> None:
        """Apply SET_DC_RESULT: set final result from a datacenter."""
        self._job_manager.set_dc_result(
            job_id=command.job_id,
            dc_id=command.dc_id,
            result=command.dc_result,
        )

    # =========================================================================
    # Callback Handlers
    # =========================================================================

    async def _apply_set_callback(self, command: GateRaftCommand) -> None:
        """Apply SET_CALLBACK: set callback address for a job."""
        self._job_manager.set_callback(
            job_id=command.job_id,
            addr=command.callback_addr,
        )

    async def _apply_remove_callback(self, command: GateRaftCommand) -> None:
        """Apply REMOVE_CALLBACK: remove callback address."""
        self._job_manager.remove_callback(job_id=command.job_id)

    # =========================================================================
    # Fence Token Handler
    # =========================================================================

    async def _apply_set_fence_token(self, command: GateRaftCommand) -> None:
        """Apply SET_FENCE_TOKEN: set fence token for a job."""
        self._job_manager.set_fence_token(
            job_id=command.job_id,
            token=command.fence_token,
        )

    # =========================================================================
    # Cleanup Handler
    # =========================================================================

    async def _apply_cleanup_old_jobs(self, command: GateRaftCommand) -> None:
        """Apply CLEANUP_OLD_JOBS: remove jobs older than max age."""
        self._job_manager.cleanup_old_jobs(
            max_age_seconds=command.max_age_seconds,
        )

    # =========================================================================
    # Raft Control
    # =========================================================================

    async def _apply_no_op(self, command: GateRaftCommand) -> None:
        """Apply NO_OP: no state change. Used for leadership confirmation."""
        pass
