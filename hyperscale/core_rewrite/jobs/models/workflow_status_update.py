from .workflow_status import WorkflowStatus


class WorkflowStatusUpdate:
    __slots__ = (
        "workflow",
        "node_id",
        "status",
        "completed_count",
    )

    def __init__(
        self,
        workflow: str,
        node_id: int,
        status: WorkflowStatus,
        completed_count: int,
    ) -> None:
        self.workflow = workflow
        self.node_id = node_id
        self.status = status.value
        self.completed_count = completed_count
