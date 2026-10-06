"""Wire model ``WorkflowFinalResult`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from typing import TYPE_CHECKING
from dataclasses import dataclass
from hyperscale.reporting.common.results_types import WorkflowStats
from .message import Message

if TYPE_CHECKING:
    from .workflow_progress import WorkflowProgress


@dataclass(slots=True)
class WorkflowFinalResult(Message):
    """
    Final result of a workflow execution.

    Sent from worker to manager when a workflow completes (success or failure).
    This triggers:
    1. Context storage (for dependent workflows)
    2. Job completion check
    3. Final result aggregation
    4. Core availability update (manager uses worker_available_cores to track capacity)

    Note: WorkflowStats already contains run_id, elapsed, and step results.
    """

    job_id: str  # Parent job
    workflow_id: str  # Workflow instance
    workflow_name: str  # Workflow class name
    status: str  # COMPLETED | FAILED
    results: list[WorkflowStats]  # Cloudpickled list[WorkflowResults]
    context_updates: bytes  # Cloudpickled context dict (for Provide hooks)
    error: str | None = None  # Error message if failed (no traceback)
    worker_id: str = ""  # Worker that executed this workflow
    worker_available_cores: int = 0  # Worker's available cores after completion
    # The worker's core availability version as of worker_available_cores.
    worker_cores_version: int = 0
    fence_token: int = 0  # Dispatch fence token accepted by the worker
    job_leader_addr: tuple[str, int] | None = None  # Manager that dispatched this workflow
    # The run's final progress (its final counts), carried with the result:
    # sent separately it could arrive after the result and the job's
    # totals would be built from an earlier snapshot.
    final_progress: "WorkflowProgress | None" = None
