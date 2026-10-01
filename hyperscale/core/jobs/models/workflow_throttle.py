class WorkflowThrottle:
    """AD-41 THROTTLE request for a running workflow: cut its concurrency
    to ``scale`` of its operating point, or -- ``scale`` None -- restore
    it."""

    __slots__ = (
        "workflow_name",
        "scale",
    )

    def __init__(
        self,
        workflow_name: str,
        scale: float | None,
    ):
        self.workflow_name = workflow_name
        self.scale = scale
