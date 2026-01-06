class WorkflowCancellation:

    __slots__ = (
        "workflow_name",
        "graceful_timeout"
    )

    def __init__(
        self,
        workflow_name: str,
        graceful_timeout: int,
    ):
        self.workflow_name = workflow_name
        self.graceful_timeout = graceful_timeout