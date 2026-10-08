"""``WindowBucket`` -- pickled under the namespace
``hyperscale.distributed.jobs.windowed_stats_collector`` (see that module)."""

from dataclasses import dataclass
from hyperscale.distributed.models import WorkflowProgress


@dataclass(slots=True)
class WindowBucket:
    """Stats collected within a single time window."""

    window_start: float  # Unix timestamp of window start
    window_end: float  # Unix timestamp of window end
    job_id: str
    workflow_id: str
    workflow_name: str
    worker_stats: dict[str, WorkflowProgress]  # worker_id -> progress
    created_at: float  # When this bucket was created (for cleanup)
