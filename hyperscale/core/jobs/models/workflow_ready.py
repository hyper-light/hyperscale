class WorkflowReady:
    """A node's report that its run of the workflow is set up -- clients
    built, targets resolved and connected -- and waits at its start gate
    for the leader's ``WorkflowRelease``."""

    __slots__ = ("workflow_name",)

    def __init__(
        self,
        workflow_name: str,
    ):
        self.workflow_name = workflow_name
