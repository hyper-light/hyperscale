
from hyperscale.core.graph import Workflow

class Test:

    __slots__ = (
        'name',
        'workflows',
    )

    def __init__(
        self,
        name: str,
        workflows: list[Workflow],
    ):
        self.name = name
        self.workflows = workflows