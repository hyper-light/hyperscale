"""Wire model ``GateStateSnapshot`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from typing import TYPE_CHECKING
from dataclasses import dataclass, field
from .message import Message

if TYPE_CHECKING:
    from .datacenter_status import DatacenterStatus
    from .global_job_status import GlobalJobStatus
    from .job_submission import JobSubmission
    from .workflow_result_push import WorkflowResultPush


@dataclass(slots=True)
class GateStateSnapshot(Message):
    """
    Complete state snapshot from a gate.

    Used for state sync between gates when a new leader is elected.
    Contains global job state and datacenter status.
    """

    node_id: str  # Gate identifier
    is_leader: bool  # Leadership status
    term: int  # Current term
    version: int  # State version
    jobs: dict[str, "GlobalJobStatus"] = field(default_factory=dict)
    datacenter_status: dict[str, "DatacenterStatus"] = field(default_factory=dict)
    # Manager discovery - shared between gates
    datacenter_managers: dict[str, list[tuple[str, int]]] = field(default_factory=dict)
    datacenter_manager_udp: dict[str, list[tuple[str, int]]] = field(
        default_factory=dict
    )
    # Per-job leadership tracking (independent of SWIM cluster leadership)
    job_leaders: dict[str, str] = field(
        default_factory=dict
    )  # job_id -> leader_node_id
    job_leader_addrs: dict[str, tuple[str, int]] = field(
        default_factory=dict
    )  # job_id -> (host, tcp_port)
    job_fencing_tokens: dict[str, int] = field(
        default_factory=dict
    )  # job_id -> fencing token (for leadership consistency)
    # Per-job per-DC manager leader tracking (which manager accepted each job in each DC)
    job_dc_managers: dict[str, dict[str, tuple[str, int]]] = field(
        default_factory=dict
    )  # job_id -> {dc_id -> (host, port)}
    workflow_dc_results: dict[str, dict[str, dict[str, "WorkflowResultPush"]]] = field(
        default_factory=dict
    )
    job_submissions: dict[str, "JobSubmission"] = field(default_factory=dict)
    progress_callbacks: dict[str, tuple[str, int]] = field(default_factory=dict)
