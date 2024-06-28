class AcknowledgedCompletion:
    __slots__ = "completions"

    def __init__(
        self,
        workflow: str,
        completions: int,
    ) -> None:
        self.workflow = workflow
        self.completions = completions
