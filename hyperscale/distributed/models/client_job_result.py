"""Wire model ``ClientJobResult`` -- pickled under the wire namespace
``hyperscale.distributed.models.client`` (see that module)."""

from dataclasses import dataclass, field

from .aggregated_job_stats import AggregatedJobStats
from .client_reporter_result import ClientReporterResult
from .client_workflow_result import ClientWorkflowResult


@dataclass(slots=True)
class ClientJobResult:
    """
    Result of a completed job as seen by the client.

    For single-DC jobs, only basic fields are populated.
    For multi-DC jobs (via gates), per_datacenter_results and aggregated are populated.
    """

    job_id: str
    status: str  # JobStatus value
    total_completed: int = 0
    total_failed: int = 0
    overall_rate: float = 0.0
    elapsed_seconds: float = 0.0
    error: str | None = None
    # Workflow results (populated as each workflow completes)
    workflow_results: dict[str, ClientWorkflowResult] = field(
        default_factory=dict
    )  # workflow_id -> result
    # Workflows the job was submitted with that have no result when the
    # job is waited out: a failed or cancelled job's unrun workflows, or
    # a result that never arrived
    missing_workflow_results: list[str] = field(default_factory=list)
    # Multi-DC fields (populated when result comes from a gate)
    per_datacenter_results: list = field(default_factory=list)  # list[JobFinalResult]
    per_datacenter_statuses: dict[str, str] = field(default_factory=dict)
    aggregated: AggregatedJobStats | None = None
    # AD-44 best-effort: why the job completed before every datacenter
    # reported, and the datacenters it stopped waiting for
    completion_reason: str = ""
    unreported_datacenters: list[str] = field(default_factory=list)
    # False while late datacenter results may still update the result
    # (AD-44 late-result ``update`` policy).
    is_final: bool = True
    # Reporter results (populated as reporters complete)
    reporter_results: dict[str, ClientReporterResult] = field(
        default_factory=dict
    )  # reporter_type -> result
