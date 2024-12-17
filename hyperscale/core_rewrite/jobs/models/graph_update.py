from .workflow_status import WorkflowStatus


class GraphUpdate:
    __slots__ = (
        "workflow",
        "status",
        "completed_count",
        "failed_count",
        "avg_cpu_usage",
        "avg_memory_usage_mb",
    )

    def __init__(
        self,
        workflow: str,
        status: WorkflowStatus,
        completed_count: int,
        failed_count: int,
        avg_cpu_usage: float,
        avg_memory_usage_mb: float,
    ):
        self.workflow = workflow
        self.status = status.value
        self.completed_count = completed_count
        self.failed_count = failed_count
        self.avg_cpu_usage = avg_cpu_usage
        self.avg_memory_usage_mb = avg_memory_usage_mb
