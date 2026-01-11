from .workflow import Workflow


def depends(*args: str):

    dependencies = list(set(args))

    def wrapper(workflow: Workflow):
        workflow._dependencies = dependencies

        return workflow

    return wrapper
