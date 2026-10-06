"""Wire model ``GlobalJobResult`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from typing import TYPE_CHECKING
from dataclasses import dataclass, field
from .datacenter_substitution import DatacenterSubstitution
from .message import Message
from .aggregated_job_stats import AggregatedJobStats

if TYPE_CHECKING:
    from .job_final_result import JobFinalResult


@dataclass(slots=True)
class GlobalJobResult(Message):
    """
    Global job result aggregated across all datacenters.

    Sent from Gate to Client as the final result.
    Contains per-DC breakdown and cross-DC aggregation.
    """

    job_id: str  # Job identifier
    status: str  # COMPLETED | FAILED | PARTIAL
    # Per-datacenter breakdown
    per_datacenter_results: list["JobFinalResult"] = field(default_factory=list)
    per_datacenter_statuses: dict[str, str] = field(default_factory=dict)
    # Cross-DC aggregated stats
    aggregated: "AggregatedJobStats" = field(default_factory=AggregatedJobStats)
    # Summary
    total_completed: int = 0  # Sum across all DCs
    total_failed: int = 0  # Sum across all DCs
    successful_datacenters: int = 0
    failed_datacenters: int = 0
    errors: list[str] = field(default_factory=list)  # All errors from all DCs
    elapsed_seconds: float = 0.0  # Max elapsed across all DCs
    # AD-44 best-effort: why the job completed before every datacenter
    # reported ("" otherwise), and the datacenters it stopped waiting for
    # (cancelled, their results not included).
    completion_reason: str = ""
    unreported_datacenters: list[str] = field(default_factory=list)
    # AD-36 mid-flight failover: each datacenter the job lost while it ran,
    # the one its unfinished workflows re-ran in, and its work until it
    # was lost (counted in the totals; the replacement's final result is
    # among the per-datacenter results).
    datacenter_substitutions: list[DatacenterSubstitution] = field(default_factory=list)
