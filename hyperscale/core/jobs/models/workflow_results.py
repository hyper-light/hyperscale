from typing import Any, Dict

from hyperscale.core.jobs.models.workflow_status import WorkflowStatus
from hyperscale.core.state.workflow_context import WorkflowContext
from hyperscale.reporting.common.results_types import WorkflowStats


class WorkflowResults:
    __slots__ = (
        "workflow",
        "results",
        "context",
        "error",
        "status",
    )

    def __init__(
        self,
        workflow: str,
        results: WorkflowStats | Dict[str, Any | Exception] | None,
        context: WorkflowContext | Dict[str, Any],
        error: Exception | None,
        status: WorkflowStatus,
    ) -> None:
        self.workflow = workflow
        self.results = results

        if isinstance(context, WorkflowContext):
            context = context.dict()

        self.context = context
        self.error = error if error is None else str(error)
        self.status = status.value
