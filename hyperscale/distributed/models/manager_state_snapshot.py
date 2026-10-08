"""Wire model ``ManagerStateSnapshot`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from typing import TYPE_CHECKING
from dataclasses import dataclass, field
from .message import Message
from .job_state_sync_message import JobStateSyncMessage

if TYPE_CHECKING:
    from .job_progress import JobProgress
    from .worker_state_snapshot import WorkerStateSnapshot


@dataclass(slots=True)
class ManagerStateSnapshot(Message):
    """
    Complete state snapshot from a manager.

    Used for state sync between managers.
    """

    node_id: str  # Manager identifier
    datacenter: str  # Datacenter
    is_leader: bool  # Leadership status
    term: int  # Current term
    version: int  # State version
    workers: list["WorkerStateSnapshot"] = field(default_factory=list)
    jobs: dict[str, "JobProgress"] = field(default_factory=dict)
    # Context consistency protocol state
    job_leaders: dict[str, str] = field(
        default_factory=dict
    )  # job_id -> leader_node_id
    job_leader_addrs: dict[str, tuple[str, int]] = field(
        default_factory=dict
    )  # job_id -> (host, tcp_port)
    job_fence_tokens: dict[str, int] = field(default_factory=dict)
    job_states: dict[str, JobStateSyncMessage] = field(default_factory=dict)
    # Pending stats checkpoint for recovery (Task 33)
    # List of (timestamp, value) tuples from the stats buffer
    pending_stats_checkpoint: list[tuple[float, float]] = field(default_factory=list)
