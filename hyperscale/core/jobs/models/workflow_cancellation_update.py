class WorkflowCancellationUpdate:

    __slots__ = (
        "workflow_name",
        "status",
        "error"
    )

    def __init__(
        self,
        workflow_name: str,
        status: str,
        error: str | None = None
    ):
        self.workflow_name = workflow_name
        self.status = status
        self.error = error