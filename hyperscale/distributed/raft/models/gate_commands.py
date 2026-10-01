"""
Gate Raft command model.

Serializable command carrying all data needed for a single
GateJobManager mutation. Stored in Raft log entries and applied
deterministically by the gate state machine.
"""

from dataclasses import dataclass

from typing import Any

from hyperscale.distributed.models import GlobalJobStatus, JobFinalResult, JobSubmission, WorkflowResultPush

from hyperscale.distributed.ledger.events.event_type import JobEventType

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

    # Gate leadership (assume, takeover, release, process_claim)
    metadata: Any | None = None
    initial_token: int = 1
    claimer_id: str | None = None
    claimer_addr: tuple[str, int] | None = None
    fencing_token: int = 0

    # DC manager tracking (update, release)
    manager_id: str | None = None
    manager_addr: tuple[str, int] | None = None

    # Lease management (create, release)
    datacenter: str | None = None
    lease_holder: str | None = None
    expires_at: float = 0.0
    lease_version: int = 0

    # Job submission state
    submission: JobSubmission | None = None

    # Workflow DC results
    workflow_id: str | None = None
    workflow_result: WorkflowResultPush | None = None

    # Membership events
    event_type: str | None = None
    node_id: str | None = None
    node_addr: tuple[str, int] | None = None

    # AD-38 ledger replication (ledger_append): the WAL entry's event
    ledger_event_type: JobEventType | None = None
    ledger_payload: bytes | None = None


def gate_ledger_append_command(
    job_id: str,
    event_type: JobEventType,
    payload: bytes,
) -> GateRaftCommand:
    """A gate group's LEDGER_APPEND for one job-ledger WAL entry."""
    return GateRaftCommand(
        command_type=GateRaftCommandType.LEDGER_APPEND,
        job_id=job_id,
        ledger_event_type=event_type,
        ledger_payload=payload,
    )
