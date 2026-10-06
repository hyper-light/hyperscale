"""Wire model ``JobLeadershipAnnouncement`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass, field
from .message import Message


@dataclass(slots=True)
class JobLeadershipAnnouncement(Message):
    """
    Announcement of job leadership to peer managers.

    When a manager accepts a job, it broadcasts this to all peer managers
    so they know who the job leader is. This enables:
    - Proper routing of workflow results to job leader
    - Correct forwarding of context updates
    - Job state consistency across the manager cluster
    - Workflow query support (non-leaders can report job status)
    """

    job_id: str  # Job being led
    leader_id: str  # Node ID of the job leader
    # Host/port can be provided as separate fields or as tuple
    leader_host: str = ""  # Host of the job leader
    leader_tcp_port: int = 0  # TCP port of the job leader
    term: int = 0  # Cluster term when job was accepted
    workflow_count: int = 0  # Number of workflows in job
    timestamp: float = 0.0  # When job was accepted
    # Workflow names for query support (non-leaders can track job contents)
    workflow_names: list[str] = field(default_factory=list)
    # Alternative form: address as tuple and target_dc_count
    leader_addr: tuple[str, int] | None = None
    target_dc_count: int = 0
    fence_token: int = 0
    # Push-notification destinations replicated to peers so a manager
    # that takes over job leadership after the original leader dies can
    # push completion / cancellation notifications to the originating
    # client and origin gate. Without this, only the original leader knows
    # the callback addresses, and any post-takeover completion silently
    # drops on the floor.
    callback_addr: tuple[str, int] | None = None
    origin_gate_addr: tuple[str, int] | None = None
    # A manager-tier job's Raft group voters, agreed by every member
    # (AD-52): the leader decided them creating the group, and each peer
    # joins the group with them. Gate announcements carry none -- a gate
    # job's replica does.
    raft_voters: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Handle leader_addr alias for leader_host/leader_tcp_port."""
        if self.leader_addr is not None:
            object.__setattr__(self, "leader_host", self.leader_addr[0])
            object.__setattr__(self, "leader_tcp_port", self.leader_addr[1])
        if self.target_dc_count > 0 and self.term == 0:
            object.__setattr__(self, "term", self.target_dc_count)
