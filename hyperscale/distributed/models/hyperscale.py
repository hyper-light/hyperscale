import pickle
import msgspec
import cloudpickle
from hyperscale.core.state import Context
from hyperscale.core.graph.workflow import Workflow
from hyperscale.core.graph.dependent_workflow import DependentWorkflow
from .restricted_unpickler import restricted_loads


class Job(msgspec.Struct):
    context: str
    vus: int
    workflow: str

    def load_context(self) -> Context:
        # Use restricted unpickler to prevent arbitrary code execution
        return restricted_loads(
            self.context.encode(),
        )

    def load_workflow(self) -> Workflow | DependentWorkflow:
        # Use restricted unpickler to prevent arbitrary code execution
        return restricted_loads(
            self.workflow.encode(),
        )
    
    @classmethod
    def serialize(
        self,
        context: Context,
        vus: int,
        workflow: Workflow,
    ):
        return Job(
            context=cloudpickle.dumps(
                context, 
                protocol=pickle.HIGHEST_PROTOCOL,
            ).decode(),
            vus=vus,
            workflow=cloudpickle.dumps(
                workflow,
                protocol=pickle.HIGHEST_PROTOCOL,
            ).decode(),
        )