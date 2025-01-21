from typing import Any, List

from .workflow import Workflow


class DependentWorkflow:
    def __init__(
        self,
        workflow: Workflow,
        dependencies: List[str],
    ) -> None:
        self.dependent_workflow = workflow
        self.dependencies = dependencies

    def __call__(self, *args: Any, **kwds: Any) -> Any:
        self.dependent_workflow = self.dependent_workflow(*args, **kwds)
        return self
