from typing import Dict, Literal
from .workflow_status import WorkflowStatus

StepStatsType = Literal[
    "total",
    "ok",
    "err",
]


class WorkflowStatusUpdate:
    __slots__ = (
        "workflow",
        "node_id",
        "status",
        "completed_count",
        "failed_count",
        "step_stats",
        "avg_cpu_usage",
        "avg_memory_usage_mb",
        "workers_completed",
        "total_cpu_usage",
        "total_memory_usage_mb",
    )

    def __init__(
        self,
        workflow: str,
        status: WorkflowStatus,
        node_id: int | None = None,
        completed_count: int | None = None,
        failed_count: int | None = None,
        step_stats: Dict[str, Dict[StepStatsType, int]] | None = None,
        avg_cpu_usage: float | None = None,
        avg_memory_usage_mb: float | None = None,
        workers_completed: int | None = None,
        total_cpu_usage: float | None = None,
        total_memory_usage_mb: float | None = None,
    ) -> None:
        self.workflow = workflow
        self.node_id = node_id
        self.status = status.value
        self.completed_count = completed_count
        self.failed_count = failed_count
        self.step_stats = step_stats
        self.avg_cpu_usage = avg_cpu_usage
        self.avg_memory_usage_mb = avg_memory_usage_mb
        self.workers_completed = workers_completed
        # Summed across the workflow's executor processes (the averages
        # above are per process): what the workflow as a whole consumes.
        self.total_cpu_usage = total_cpu_usage
        self.total_memory_usage_mb = total_memory_usage_mb
