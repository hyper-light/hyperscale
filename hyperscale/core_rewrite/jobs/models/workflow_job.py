from typing import Any, Dict

from hyperscale.core_rewrite.graph import Workflow
from hyperscale.core_rewrite.state import Context


class WorkflowJob:
    __slots__ = (
        "workflow",
        "context",
    )

    def __init__(
        self,
        workflow: Workflow,
        context: Context,
    ) -> None:
        self.workflow = workflow
        self.context: Dict[str, Dict[str, Any]] = context.dict()
