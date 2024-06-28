from .workflow_status import WorkflowStatus


class WorkflowStatusUpdate:
    __slots__ = ("status", "message", "expected_completions")

    def __init__(
        self,
        status: WorkflowStatus,
        message: str,
        expected_completions: int,
    ) -> None:
        self.status = status.value
        self.message = message
        self.expected_completions = expected_completions
