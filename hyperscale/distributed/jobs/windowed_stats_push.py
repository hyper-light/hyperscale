"""``WindowedStatsPush`` -- pickled under the namespace
``hyperscale.distributed.jobs.windowed_stats_collector`` (see that module)."""

from dataclasses import dataclass, field
from hyperscale.distributed.models import StepStats, Message

from .worker_window_stats import WorkerWindowStats


@dataclass(slots=True)
class WindowedStatsPush(Message):
    job_id: str
    workflow_id: str
    workflow_name: str = ""
    window_start: float = 0.0
    window_end: float = 0.0
    completed_count: int = 0
    failed_count: int = 0
    rate_per_second: float = 0.0
    step_stats: list[StepStats] = field(default_factory=list)
    worker_count: int = 0
    avg_cpu_percent: float = 0.0
    avg_memory_mb: float = 0.0
    per_worker_stats: list[WorkerWindowStats] = field(default_factory=list)
    is_aggregated: bool = True
    datacenter: str = ""
