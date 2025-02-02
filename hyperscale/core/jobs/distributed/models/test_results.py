from hyperscale.reporting.common.results_types import RunResults


class TestResults:
    
    __slots__ = (
        'node_id',
        'test',
        'results',
        'error'
    )

    def __init__(
        self,
        node_id: int,
        test: str,
        results: RunResults | None = None,
        error: str | None = None
    ):
        self.node_id = node_id
        self.test = test
        self.results = results
        self.error = error
