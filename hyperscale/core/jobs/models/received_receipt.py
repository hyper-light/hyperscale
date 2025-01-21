class ReceivedReceipt:
    __slots__ = (
        "workflow",
        "node_id",
    )

    def __init__(
        self,
        workflow: str,
        node_id: int,
    ) -> None:
        self.workflow = workflow
        self.node_id = node_id
