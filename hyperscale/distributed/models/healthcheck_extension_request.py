"""Wire model ``HealthcheckExtensionRequest`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True)
class HealthcheckExtensionRequest(Message):
    """
    Request from worker for deadline extension (AD-26).

    Workers can request deadline extensions when:
    - Executing long-running workflows
    - System is under heavy load but making progress
    - Approaching timeout but not stuck

    Extensions use logarithmic decay (AD-26 line 32):
        grant = max(min_grant, base_deadline / 2 ** extension_count)
    where ``extension_count`` is the count *before* this grant.
    - First extension (count=0):  base   (e.g., 30s with base=30s)
    - Second extension (count=1): base/2 (e.g., 15s)
    - Third extension (count=2):  base/4 (e.g., 7.5s)
    - ...continues until min_grant is reached.

    Sent from: Worker -> Manager

    AD-26 Issue 4: Absolute metrics provide more robust progress tracking
    than relative 0-1 progress values. For long-running work, absolute
    metrics (100 items → 101 items) are easier to track than relative
    progress (0.995 → 0.996) and avoid float precision issues.
    """

    worker_id: str  # Worker requesting extension
    reason: str  # Why extension is needed
    current_progress: float  # Progress metric (must increase for approval) - kept for backward compatibility
    estimated_completion: float  # Estimated seconds until completion
    active_workflow_count: int  # Number of workflows currently executing
    # AD-26 Issue 4: Absolute progress metrics (preferred over relative progress)
    completed_items: int | None = None  # Absolute count of completed items
    total_items: int | None = None  # Total items to complete
    # Phase H3 — multi-dimensional WorkflowProgressSnapshot fields.
    # ``completed_items`` is the primary (cores_completed) signal;
    # the three below carry the secondary, tertiary, and timestamp
    # signals that AD-26 H5 multi-witness decision uses for
    # tamper-resistant progress validation. Defaults to 0/None for
    # back-compat with peers that pre-date Phase H.
    workflow_id: str = ""  # Specific workflow this snapshot belongs to
    step_transitions: int = 0  # AD-54 step-state transitions since dispatch
    actions_completed: int = 0  # Sum of StepStats.completed_count across active steps
    snapshot_time: float = 0.0  # time.monotonic() on worker when snapshot was constructed
