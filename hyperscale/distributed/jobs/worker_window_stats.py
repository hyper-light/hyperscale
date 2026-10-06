"""``WorkerWindowStats`` -- pickled under the namespace
``hyperscale.distributed.jobs.windowed_stats_collector`` (see that module)."""

from dataclasses import dataclass, field
from hyperscale.distributed.models import StepStats


@dataclass(slots=True)
class WorkerWindowStats:
    """Individual worker stats within a time window."""

    worker_id: str
    completed_count: int = 0
    failed_count: int = 0
    rate_per_second: float = 0.0
    step_stats: list[StepStats] = field(default_factory=list)
    avg_cpu_percent: float = 0.0
    avg_memory_mb: float = 0.0
