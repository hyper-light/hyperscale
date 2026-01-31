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
    from hyperscale.distributed.jobs.job_leadership_tracker import JobLeadershipTracker
    from hyperscale.distributed.nodes.gate.state import GateRuntimeState
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
        "_leadership_tracker",
        "_gate_state",
        "_logger",
        "_node_id",
        "_handlers",
    )

    def __init__(
        self,
        job_manager: "GateJobManager",
        leadership_tracker: "JobLeadershipTracker",
        gate_state: "GateRuntimeState",
        logger: "Logger",
        node_id: str,
    ) -> None:
        self._job_manager = job_manager
        self._leadership_tracker = leadership_tracker
        self._gate_state = gate_state
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
            GateRaftCommandType.ASSUME_GATE_LEADERSHIP: self._apply_assume_leadership,
            GateRaftCommandType.TAKEOVER_GATE_LEADERSHIP: self._apply_takeover_leadership,
            GateRaftCommandType.RELEASE_GATE_LEADERSHIP: self._apply_release_leadership,
            GateRaftCommandType.PROCESS_LEADERSHIP_CLAIM: self._apply_process_leadership_claim,
            GateRaftCommandType.UPDATE_DC_MANAGER: self._apply_update_dc_manager,
            GateRaftCommandType.RELEASE_DC_MANAGERS: self._apply_release_dc_managers,
            GateRaftCommandType.CREATE_LEASE: self._apply_create_lease,
            GateRaftCommandType.RELEASE_LEASE: self._apply_release_lease,
            GateRaftCommandType.SET_JOB_SUBMISSION: self._apply_set_job_submission,
            GateRaftCommandType.SET_WORKFLOW_DC_RESULT: self._apply_set_workflow_dc_result,
            GateRaftCommandType.GATE_MEMBERSHIP_EVENT: self._apply_gate_membership_event,
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
    # Gate Leadership Handlers
    # =========================================================================

    async def _apply_assume_leadership(self, command: GateRaftCommand) -> None:
        """Apply ASSUME_GATE_LEADERSHIP: this gate assumes job leadership."""
        self._leadership_tracker.assume_leadership(
            job_id=command.job_id,
            metadata=command.metadata,
            initial_token=command.initial_token,
        )

    async def _apply_takeover_leadership(self, command: GateRaftCommand) -> None:
        """Apply TAKEOVER_GATE_LEADERSHIP: this gate takes over leadership."""
        self._leadership_tracker.takeover_leadership(
            job_id=command.job_id,
            metadata=command.metadata,
        )

    async def _apply_release_leadership(self, command: GateRaftCommand) -> None:
        """Apply RELEASE_GATE_LEADERSHIP: release leadership of a job."""
        self._leadership_tracker.release_leadership(job_id=command.job_id)

    async def _apply_process_leadership_claim(self, command: GateRaftCommand) -> None:
        """Apply PROCESS_LEADERSHIP_CLAIM: process peer's leadership claim."""
        self._leadership_tracker.process_leadership_claim(
            job_id=command.job_id,
            claimer_id=command.claimer_id,
            claimer_addr=command.claimer_addr,
            fencing_token=command.fencing_token,
            metadata=command.metadata,
        )

    # =========================================================================
    # DC Manager Tracking Handlers
    # =========================================================================

    async def _apply_update_dc_manager(self, command: GateRaftCommand) -> None:
        """Apply UPDATE_DC_MANAGER: update DC manager for a job."""
        self._leadership_tracker._update_dc_manager(
            job_id=command.job_id,
            dc_id=command.dc_id,
            manager_id=command.manager_id,
            manager_addr=command.manager_addr,
            fencing_token=command.fencing_token,
        )

    async def _apply_release_dc_managers(self, command: GateRaftCommand) -> None:
        """Apply RELEASE_DC_MANAGERS: release all DC manager tracking for a job."""
        self._leadership_tracker._dc_managers.pop(command.job_id, None)

    # =========================================================================
    # Lease Management Handlers
    # =========================================================================

    async def _apply_create_lease(self, command: GateRaftCommand) -> None:
        """Apply CREATE_LEASE: create a datacenter lease."""
        from hyperscale.distributed.models import DatacenterLease

        lease = DatacenterLease()
        lease.job_id = command.job_id
        lease.datacenter = command.datacenter
        lease.lease_holder = command.lease_holder
        lease.fence_token = command.fence_token
        lease.expires_at = command.expires_at
        lease.version = command.lease_version
        self._gate_state._leases[command.job_id] = lease

    async def _apply_release_lease(self, command: GateRaftCommand) -> None:
        """Apply RELEASE_LEASE: release a datacenter lease."""
        self._gate_state._leases.pop(command.job_id, None)

    # =========================================================================
    # Job Submission State Handler
    # =========================================================================

    async def _apply_set_job_submission(self, command: GateRaftCommand) -> None:
        """Apply SET_JOB_SUBMISSION: store the job submission."""
        self._gate_state._job_submissions[command.job_id] = command.submission

    # =========================================================================
    # Workflow DC Result Handler
    # =========================================================================

    async def _apply_set_workflow_dc_result(self, command: GateRaftCommand) -> None:
        """Apply SET_WORKFLOW_DC_RESULT: store a workflow result from a DC."""
        job_id = command.job_id
        dc_id = command.dc_id
        workflow_id = command.workflow_id
        if job_id not in self._gate_state._workflow_dc_results:
            self._gate_state._workflow_dc_results[job_id] = {}
        if dc_id not in self._gate_state._workflow_dc_results[job_id]:
            self._gate_state._workflow_dc_results[job_id][dc_id] = {}
        self._gate_state._workflow_dc_results[job_id][dc_id][workflow_id] = command.workflow_result

    # =========================================================================
    # Membership Handler
    # =========================================================================

    async def _apply_gate_membership_event(self, command: GateRaftCommand) -> None:
        """Apply GATE_MEMBERSHIP_EVENT: record a cluster membership change."""
        pass

    # =========================================================================
    # Raft Control
    # =========================================================================

    async def _apply_no_op(self, command: GateRaftCommand) -> None:
        """Apply NO_OP: no state change. Used for leadership confirmation."""
        pass
