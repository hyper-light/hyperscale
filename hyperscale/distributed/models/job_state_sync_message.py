"""Wire model ``JobStateSyncMessage`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass, field
from .message import Message
from .sub_workflow_state_snapshot import SubWorkflowStateSnapshot
from .workflow_state_snapshot import WorkflowStateSnapshot


@dataclass(slots=True)
class JobStateSyncMessage(Message):
    """
    Periodic job state sync from job leader to peer managers.

    Sent every MANAGER_PEER_SYNC_INTERVAL seconds to ensure peer managers
    have up-to-date job state for faster failover recovery. Contains summary
    info that allows non-leaders to serve read queries and prepare for takeover.

    This supplements SWIM heartbeat embedding (which has limited capacity)
    with richer job metadata.
    """

    leader_id: str
    job_id: str
    status: str
    fencing_token: int
    workflows_total: int
    workflows_completed: int
    workflows_failed: int
    workflow_statuses: dict[str, str] = field(default_factory=dict)
    elapsed_seconds: float = 0.0
    timestamp: float = 0.0
    origin_gate_addr: tuple[str, int] | None = None
    callback_addr: tuple[str, int] | None = None
    leader_addr: tuple[str, int] | None = None
    workflow_snapshots: dict[str, WorkflowStateSnapshot] = field(default_factory=dict)
    sub_workflow_snapshots: dict[str, SubWorkflowStateSnapshot] = field(default_factory=dict)
    replace_existing: bool = True
    context_snapshot: dict[str, dict[str, object]] = field(default_factory=dict)
    layer_version: int = 0
    # The job's Raft group voters, agreed by every member (AD-52): a peer
    # learning a live job by sync joins its group with them. Empty once the
    # job is terminal (its group is gone).
    raft_voters: list[str] = field(default_factory=list)
