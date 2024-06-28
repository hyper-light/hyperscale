from typing import Any, Dict

from hyperscale.core_rewrite.results.workflow_types import WorkflowStats
from hyperscale.core_rewrite.state import Context


class WorkflowResults:
    __slots__ = (
        "workflow",
        "results",
        "context",
    )

    def __init__(
        self,
        workflow: str,
        results: WorkflowStats | Dict[str, Any | Exception],
        context: Context,
    ) -> None:
        self.workflow = workflow
        self.results = results
        self.context: Dict[str, Dict[str, Any]] = context.dict()
