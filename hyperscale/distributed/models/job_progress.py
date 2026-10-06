"""Wire model ``JobProgress`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from typing import TYPE_CHECKING
from dataclasses import dataclass, field
from .message import Message

if TYPE_CHECKING:
    from .step_stats import StepStats
    from .workflow_progress import WorkflowProgress


@dataclass(slots=True)
class JobProgress(Message):
    """
    Aggregated job progress from manager to gate.

    Contains summary of all workflows in the job.

    Time alignment:
    - collected_at: Unix timestamp when stats were aggregated at the manager.
      Used for time-aligned aggregation across DCs at the gate.
    - timestamp: Monotonic timestamp for local ordering (not cross-node comparable).

    Ordering fields:
    - progress_sequence: Per-job per-datacenter monotonic counter incremented on
      each progress update. Used by gates to reject out-of-order updates.
    - fence_token: Leadership fencing token (NOT for progress ordering).
    """

    job_id: str  # Job identifier
    datacenter: str  # Reporting datacenter
    status: str  # JobStatus value
    workflows: list["WorkflowProgress"] = field(default_factory=list)
    total_completed: int = 0  # Total actions completed
    total_failed: int = 0  # Total actions failed
    overall_rate: float = 0.0  # Aggregate rate
    elapsed_seconds: float = 0.0  # Time since job start
    timestamp: float = 0.0  # Monotonic timestamp (local ordering)
    collected_at: float = 0.0  # Unix timestamp when aggregated (cross-DC alignment)
    # Aggregated step stats across all workflows in the job
    step_stats: list["StepStats"] = field(default_factory=list)
    fence_token: int = 0  # Fencing token for at-most-once semantics (leadership safety)
    # Per-update sequence for ordering (incremented by manager on each progress update)
    progress_sequence: int = 0
