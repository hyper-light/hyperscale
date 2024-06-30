from typing import Any, Dict

from hyperscale.core_rewrite.graph import Workflow
from hyperscale.core_rewrite.state.workflow_context import WorkflowContext


class WorkflowJob:
    __slots__ = (
        "workflow",
        "context",
    )

    def __init__(
        self,
        workflow: Workflow,
        context: WorkflowContext,
    ) -> None:
        self.workflow = workflow
        self.context: Dict[str, Any] = context.dict()
