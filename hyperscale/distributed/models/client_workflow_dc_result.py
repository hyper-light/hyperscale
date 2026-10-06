"""Wire model ``ClientWorkflowDCResult`` -- pickled under the wire namespace
``hyperscale.distributed.models.client`` (see that module)."""

from dataclasses import dataclass
from hyperscale.reporting.common.results_types import WorkflowStats


@dataclass(slots=True)
class ClientWorkflowDCResult:
    """Per-datacenter workflow result for client-side tracking."""

    datacenter: str
    status: str
    stats: WorkflowStats | None = None  # WorkflowStats for this DC
    error: str | None = None
    elapsed_seconds: float = 0.0
    # The datacenter whose share this one re-ran after the job lost it
    # mid-run (AD-36), "" for a datacenter's own share.
    rerun_of: str = ""
