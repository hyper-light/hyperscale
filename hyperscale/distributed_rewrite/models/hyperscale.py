import pickle
import msgspec
import cloudpickle
from hyperscale.core.state import Context
from hyperscale.core.graph.workflow import Workflow
from hyperscale.core.graph.dependent_workflow import DependentWorkflow


class Job(msgspec.Struct):
    context: str
    vus: int
    workflow: str

    def load_context(self) -> Context:
        return cloudpickle.loads(
            self.context.encode(),
        )

    def load_workflow(self) -> Workflow | DependentWorkflow:
        return cloudpickle.loads(
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