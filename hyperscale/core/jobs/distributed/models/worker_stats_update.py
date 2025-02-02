class WorkerStatsUpdate:

    __slots__ = (
        'node_id',
        'available_threads'
    )

    def __init__(
        self,
        node_id: int,
        available_threads: int,
    ):
        self.node_id = node_id
        self.available_threads = available_threads