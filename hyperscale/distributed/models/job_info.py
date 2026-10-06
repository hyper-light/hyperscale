"""Wire model ``JobInfo`` -- pickled under the wire namespace
``hyperscale.distributed.models.jobs`` (see that module)."""

import asyncio
from dataclasses import dataclass, field
from hyperscale.core.state.context import Context
from hyperscale.distributed.models.distributed import JobStatus, JobSubmission
from hyperscale.distributed.runtime import Clock, RealClock

from .sub_workflow_info import SubWorkflowInfo
from .timeout_tracking_state import TimeoutTrackingState
from .tracking_token import TrackingToken
from .workflow_info import WorkflowInfo

_DEFAULT_CLOCK: Clock = RealClock()


@dataclass(slots=True)
class JobInfo:
    """All state for a single job, protected by its own lock."""

    token: TrackingToken  # Job-level token (DC:manager:job)
    submission: JobSubmission | None  # None for remote jobs tracked by non-leaders
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    # Internal progress tracking (separate from wire protocol JobProgress)
    status: str = JobStatus.QUEUED.value
    workflows_total: int = 0
    workflows_completed: int = 0
    workflows_failed: int = 0
    started_at: float = 0.0  # _DEFAULT_CLOCK.monotonic() when job started (local-only; do not compare across nodes)
    completed_at: float = 0.0  # Wall-clock seconds; set from RaftLogEntry.timestamp (HLC) in apply, _DEFAULT_CLOCK.time() locally
    timestamp: float = 0.0  # Wall-clock seconds of last update; same semantic as completed_at; compare with _DEFAULT_CLOCK.time()

    # Workflow tracking - keyed by token string for fast lookup
    workflows: dict[str, WorkflowInfo] = field(
        default_factory=dict
    )  # workflow_token_str -> info
    sub_workflows: dict[str, SubWorkflowInfo] = field(
        default_factory=dict
    )  # sub_workflow_token_str -> info

    # Context for dependent workflows
    context: Context = field(default_factory=Context)
    layer_version: int = 0

    # Job leadership (for multi-manager setups)
    leader_node_id: str | None = None
    leader_addr: tuple[str, int] | None = None
    fencing_token: int = 0

    # Callbacks
    callback_addr: tuple[str, int] | None = None

    # Timeout tracking (AD-34) - persisted across leader transfers
    timeout_tracking: TimeoutTrackingState | None = None

    @property
    def job_id(self) -> str:
        """Get job_id from token."""
        return self.token.job_id

    @property
    def datacenter(self) -> str:
        """Get datacenter from token."""
        return self.token.datacenter

    def elapsed_seconds(self) -> float:
        """Calculate elapsed time since job started."""
        if self.started_at == 0.0:
            return 0.0
        return _DEFAULT_CLOCK.monotonic() - self.started_at
