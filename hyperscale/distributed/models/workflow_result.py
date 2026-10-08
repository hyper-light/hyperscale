"""Wire model ``WorkflowResult`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from typing import TYPE_CHECKING
from dataclasses import dataclass, field
from hyperscale.reporting.common.results_types import WorkflowStats
from .message import Message

if TYPE_CHECKING:
    from .job_final_result import JobFinalResult


@dataclass(slots=True)
class WorkflowResult(Message):
    """
    Simplified workflow result for aggregation (without context).

    Used in JobFinalResult for Manager -> Gate communication.
    Context is NOT included because gates don't need it.

    For gate-bound jobs: results contains raw per-core WorkflowStats for cross-DC aggregation
    For direct-client jobs: results contains aggregated WorkflowStats (single item list)
    """

    workflow_id: str  # Workflow instance ID
    workflow_name: str  # Workflow class name
    status: str  # COMPLETED | FAILED
    results: list[WorkflowStats] = field(
        default_factory=list
    )  # Per-core or aggregated stats
    error: str | None = None  # Error message if failed
