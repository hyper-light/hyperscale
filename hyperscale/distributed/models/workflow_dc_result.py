"""Wire model ``WorkflowDCResult`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass, field
from hyperscale.reporting.common.results_types import WorkflowStats


@dataclass(slots=True)
class WorkflowDCResult:
    """Per-datacenter workflow result for cross-DC visibility."""

    datacenter: str  # Datacenter identifier
    status: str  # COMPLETED | FAILED
    stats: WorkflowStats | None = None  # Aggregated stats for this DC (test workflows)
    error: str | None = None  # Error message if failed
    elapsed_seconds: float = 0.0
    # Raw results list for non-test workflows (unaggregated)
    raw_results: list[WorkflowStats] = field(default_factory=list)
    # AD-36 mid-flight failover: the datacenter whose share of the job this
    # one re-ran after losing it mid-job ("" for a datacenter's own share).
    rerun_of: str = ""
