"""
Client-side result models for HyperscaleClient.

These dataclasses represent the results returned to users when interacting
with the Hyperscale distributed system through the client API. They provide
a clean interface for accessing job, workflow, and reporter results.
"""

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class ClientReporterResult:
    """Result of a reporter submission as seen by the client."""
    reporter_type: str
    success: bool
    error: str | None = None
    elapsed_seconds: float = 0.0
    source: str = ""  # "manager" or "gate"
    datacenter: str = ""  # For manager source


@dataclass(slots=True)
class ClientWorkflowDCResult:
    """Per-datacenter workflow result for client-side tracking."""
    datacenter: str
    status: str
    stats: Any = None  # WorkflowStats for this DC
    error: str | None = None
    elapsed_seconds: float = 0.0


@dataclass(slots=True)
class ClientWorkflowResult:
    """Result of a completed workflow within a job as seen by the client."""
    workflow_id: str
    workflow_name: str
    status: str
    stats: Any = None  # Aggregated WorkflowStats (cross-DC if from gate)
    error: str | None = None
    elapsed_seconds: float = 0.0
    # Completion timestamp for ordering (Unix timestamp)
    completed_at: float = 0.0
    # Per-datacenter breakdown (populated for multi-DC jobs via gates)
    per_dc_results: list[ClientWorkflowDCResult] = field(default_factory=list)


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
    workflow_results: dict[str, ClientWorkflowResult] = field(default_factory=dict)  # workflow_id -> result
    # Multi-DC fields (populated when result comes from a gate)
    per_datacenter_results: list = field(default_factory=list)  # list[JobFinalResult]
    aggregated: Any = None  # AggregatedJobStats
    # Reporter results (populated as reporters complete)
    reporter_results: dict[str, ClientReporterResult] = field(default_factory=dict)  # reporter_type -> result
