from .dependent_workflow import DependentWorkflow
from .workflow import Workflow


def depends(*args: str):
    def wrapper(workflow: Workflow):
        return DependentWorkflow(
            workflow,
            list(set(args)),
        )

    return wrapper
