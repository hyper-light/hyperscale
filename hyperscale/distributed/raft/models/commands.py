"""
Manager Raft command model.

Serializable command carrying all data needed for a single
JobManager mutation. Stored in Raft log entries and applied
deterministically by the state machine.
"""

from dataclasses import dataclass
from typing import Any

from hyperscale.core.graph.workflow import Workflow
from hyperscale.distributed.models import (
    JobSubmission,
    WorkflowFinalResult,
    WorkflowProgress,
    WorkflowStatus,
)

from hyperscale.distributed.ledger.events.event_type import JobEventType

from .command_types import RaftCommandType


@dataclass(slots=True)
class RaftCommand:
    """
    Serializable command for a JobManager mutation.

    Only the fields relevant to command_type need to be set.
    All others remain None.
    """

    command_type: RaftCommandType

    # Job lifecycle (create_job, track_remote_job, complete_job)
    submission: JobSubmission | None = None
    callback_addr: tuple[str, int] | None = None
    leader_node_id: str | None = None
    leader_addr: tuple[str, int] | None = None

    # Workflow registration
    job_id: str | None = None
    workflow_id: str | None = None
    workflow_name: str | None = None
    workflow: Workflow | None = None
    worker_id: str | None = None
    cores_allocated: int | None = None

    # Progress and results
    sub_workflow_token: str | None = None
    progress: WorkflowProgress | None = None
    result: WorkflowFinalResult | None = None

    # Workflow completion
    workflow_token: str | None = None
    error: str | None = None
    from_worker: bool = True
    new_status: WorkflowStatus | None = None

    # State management
    job_token: str | None = None
    status: str | None = None
    context_updates: dict[str, Any] | None = None

    # Job leadership (assume, takeover, release)
    metadata: Any | None = None
    initial_token: int = 1
    fencing_token: int = 0

    # Cancellation (initiate, complete)
    pending_workflows: set[str] | None = None

    # Provisioning (provision_confirmed)
    confirming_node_id: str | None = None

    # Stats (flush_stats_window)
    stats_data: bytes | None = None

    # Membership events
    event_type: str | None = None
    node_id: str | None = None

    # AD-38 ledger replication (ledger_append): the WAL entry's event
    ledger_event_type: JobEventType | None = None
    ledger_payload: bytes | None = None
    node_addr: tuple[str, int] | None = None


def ledger_append_command(
    job_id: str,
    event_type: JobEventType,
    payload: bytes,
) -> RaftCommand:
    """A manager group's LEDGER_APPEND for one job-ledger WAL entry."""
    return RaftCommand(
        command_type=RaftCommandType.LEDGER_APPEND,
        job_id=job_id,
        ledger_event_type=event_type,
        ledger_payload=payload,
    )
