class AcknowledgedCompletion:
    __slots__ = ("workflow", "completed_node")

    def __init__(
        self,
        workflow: str,
        completed_node: int,
    ) -> None:
        self.workflow = workflow
        self.completed_node = completed_node
