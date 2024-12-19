from .workflow_status import WorkflowStatus


class WorkflowStatusUpdate:
    __slots__ = (
        "workflow",
        "node_id",
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
        node_id: int | None = None,
        completed_count: int | None = None,
        failed_count: int | None = None,
        avg_cpu_usage: float | None = None,
        avg_memory_usage_mb: float | None = None,
    ) -> None:
        self.workflow = workflow
        self.node_id = node_id
        self.status = status.value
        self.completed_count = completed_count
        self.failed_count = failed_count
        self.avg_cpu_usage = avg_cpu_usage
        self.avg_memory_usage_mb = avg_memory_usage_mb
