
from hyperscale.core.graph import Workflow

class WorkflowRun:

    __slots__ = (
        'workflow',
        'threads',
    )

    def __init__(
        self,
        workflow: Workflow,
        threads: int,
        vus: int,
    ):
        self.workflow = workflow
        self.threads = threads
        self.vus = vus