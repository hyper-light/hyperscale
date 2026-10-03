import asyncio


class WorkflowStartBarrier:
    """
    The leader's view of one run of a workflow starting across the nodes
    it was submitted to. Each node sets up, reports ready, and waits at
    its start gate. ``all_reported`` is set once every expected node has
    either reported ready or already finished -- a node whose setup fails
    sends its results instead of readiness -- so the leader starts the
    ready nodes together and never waits on a node that can no longer
    report.
    """

    __slots__ = (
        "expected_nodes",
        "ready_nodes",
        "finished_nodes",
        "all_reported",
    )

    def __init__(
        self,
        expected_nodes: set[int],
    ):
        self.expected_nodes = expected_nodes
        self.ready_nodes: set[int] = set()
        self.finished_nodes: set[int] = set()
        self.all_reported = asyncio.Event()

        self._update_all_reported()

    def mark_ready(self, node_id: int) -> None:
        self.ready_nodes.add(node_id)
        self._update_all_reported()

    def mark_finished(self, node_id: int) -> None:
        self.finished_nodes.add(node_id)
        self._update_all_reported()

    def _update_all_reported(self) -> None:
        if self.expected_nodes <= self.ready_nodes | self.finished_nodes:
            self.all_reported.set()
