"""Wire model ``ClientWorkflowResult`` -- pickled under the wire namespace
``hyperscale.distributed.models.client`` (see that module)."""

from dataclasses import dataclass, field
from hyperscale.reporting.common.results_types import WorkflowStats

from .client_workflow_dc_result import ClientWorkflowDCResult


@dataclass(slots=True)
class ClientWorkflowResult:
    """Result of a completed workflow within a job as seen by the client."""

    workflow_id: str
    workflow_name: str
    status: str
    stats: WorkflowStats | None = None  # Aggregated WorkflowStats (cross-DC if from gate)
    error: str | None = None
    elapsed_seconds: float = 0.0
    # Completion timestamp for ordering (Unix timestamp)
    completed_at: float = 0.0
    # Per-datacenter breakdown (populated for multi-DC jobs via gates)
    per_dc_results: list[ClientWorkflowDCResult] = field(default_factory=list)
