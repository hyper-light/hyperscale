"""
Gate Raft command model.

Serializable command carrying all data needed for a single
GateJobManager mutation. Stored in Raft log entries and applied
deterministically by the gate state machine.
"""

from dataclasses import dataclass

from hyperscale.distributed.models import GlobalJobStatus, JobFinalResult

from .gate_command_types import GateRaftCommandType


@dataclass(slots=True)
class GateRaftCommand:
    """
    Serializable command for a GateJobManager mutation.

    Only the fields relevant to command_type need to be set.
    All others remain None.
    """

    command_type: GateRaftCommandType

    # Job CRUD (SET_JOB, DELETE_JOB)
    job_id: str | None = None
    job: GlobalJobStatus | None = None

    # Target DC management (SET_TARGET_DCS, ADD_TARGET_DC)
    target_dcs: set[str] | None = None
    dc_id: str | None = None

    # DC results (SET_DC_RESULT)
    dc_result: JobFinalResult | None = None

    # Callback management (SET_CALLBACK, REMOVE_CALLBACK)
    callback_addr: tuple[str, int] | None = None

    # Fence token management (SET_FENCE_TOKEN)
    fence_token: int | None = None

    # Cleanup (CLEANUP_OLD_JOBS)
    max_age_seconds: float | None = None
