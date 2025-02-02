class TestReceivedReceipt:

    __slots__ = (
        'test',
        'node_id'
    )

    def __init__(
        self,
        test: str,
        node_id: int
    ):
        self.test = test
        self.node_id = node_id