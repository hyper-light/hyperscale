"""Wire model ``WorkflowProgress`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from typing import TYPE_CHECKING
from dataclasses import dataclass, field
from .message import Message

if TYPE_CHECKING:
    from .step_stats import StepStats


@dataclass(slots=True)
class WorkflowProgress(Message):
    """
    Progress update for a running workflow.

    Sent from worker to manager during execution.

    Key fields for rapid provisioning:
    - assigned_cores: Which CPU cores are executing this workflow
    - cores_completed: How many cores have finished their portion

    When cores_completed > 0, the manager can immediately provision new
    workflows to the freed cores without waiting for the entire workflow
    to complete on all cores.

    Time alignment:
    - collected_at: Unix timestamp when stats were collected at the worker.
      Used for time-aligned aggregation across workers/DCs.
    - timestamp: Monotonic timestamp for local ordering (not cross-node comparable).
    """

    job_id: str  # Parent job
    workflow_id: str  # Workflow instance
    workflow_name: str  # Workflow class name
    status: str  # WorkflowStatus value
    completed_count: int  # Total actions completed
    failed_count: int  # Total actions failed
    rate_per_second: float  # Current execution rate
    elapsed_seconds: float  # Time since start
    step_stats: list["StepStats"] = field(default_factory=list)
    timestamp: float = 0.0  # Monotonic timestamp (local ordering)
    collected_at: float = (
        0.0  # Unix timestamp when stats were collected (cross-node alignment)
    )
    assigned_cores: list[int] = field(default_factory=list)  # Per-core assignment
    cores_completed: int = 0  # Cores that have finished their portion
    avg_cpu_percent: float = 0.0  # Average CPU utilization
    avg_memory_mb: float = 0.0  # Average memory usage in MB
    vus: int = 0  # Virtual users (from workflow config)
    # AD-41: the workflow's whole footprint, summed across its executor
    # processes (the averages above are per process).
    # Kalman estimates of the workflow's total use with their standard
    # deviations -- what the manager's resource enforcer judges. Carried on
    # progress (TCP, one message per workflow) rather than the spec's
    # heartbeat map: heartbeats ride SWIM datagrams whose size a
    # per-workflow map grows without bound.
    total_cpu_percent: float = 0.0
    total_cpu_uncertainty: float = 0.0
    total_memory_mb: float = 0.0
    total_memory_uncertainty_mb: float = 0.0
    worker_workflow_assigned_cores: int = 0
    worker_workflow_completed_cores: int = 0
    worker_available_cores: int = 0  # Available cores for worker.
    # The worker's core availability version as of worker_available_cores.
    worker_cores_version: int = 0
