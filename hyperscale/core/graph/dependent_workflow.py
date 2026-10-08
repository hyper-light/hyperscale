from typing import List, Self

from .workflow import Workflow


class DependentWorkflow:
    def __init__(
        self,
        workflow: type[Workflow],
        dependencies: List[str],
    ) -> None:
        self.dependent_workflow = workflow()
        self.dependencies = dependencies


    def __call__(self, *args: object, **kwds: object) -> Self:
        self.dependent_workflow = self.dependent_workflow
        return self
