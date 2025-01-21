from typing import Any, Dict

from hyperscale.core.graph import Workflow
from hyperscale.core.state.workflow_context import WorkflowContext


class WorkflowJob:
    __slots__ = (
        "workflow",
        "context",
        "vus",
    )

    def __init__(
        self,
        workflow: Workflow,
        context: WorkflowContext,
        vus: int,
    ) -> None:
        self.workflow = workflow
        self.context: Dict[str, Any] = context.dict()
        self.vus = vus
