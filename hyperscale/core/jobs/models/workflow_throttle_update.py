class WorkflowThrottleUpdate:
    """A node's answer to a ``WorkflowThrottle``: the workflow's concurrency
    cap after a throttle, or whether a release restored one. ``applied``
    is False when the node runs no concurrency-gated loop for the
    workflow (not running there, or an ACTION workflow)."""

    __slots__ = (
        "workflow_name",
        "applied",
        "concurrency_cap",
    )

    def __init__(
        self,
        workflow_name: str,
        applied: bool,
        concurrency_cap: int | None = None,
    ):
        self.workflow_name = workflow_name
        self.applied = applied
        self.concurrency_cap = concurrency_cap
