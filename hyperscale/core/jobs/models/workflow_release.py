class WorkflowRelease:
    """The leader's start for a run of the workflow. Sent to every ready
    node at once, and returned as the answer to each ``WorkflowReady``:
    ``released`` True there means the run already started without this
    node (it reported ready late, or its run has no start barrier), so
    it starts at once instead of waiting."""

    __slots__ = (
        "workflow_name",
        "released",
    )

    def __init__(
        self,
        workflow_name: str,
        released: bool,
    ):
        self.workflow_name = workflow_name
        self.released = released
