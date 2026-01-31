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
    node_addr: tuple[str, int] | None = None
