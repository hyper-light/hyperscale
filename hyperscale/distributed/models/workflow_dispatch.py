"""Wire model ``WorkflowDispatch`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from hyperscale.core.graph import Workflow
from .message import Message


@dataclass(slots=True)
class WorkflowDispatch(Message):
    """
    Dispatch a single workflow to a worker.

    Sent from manager to worker for execution.

    Resource Model:
    - vus: Virtual users (can be large, e.g., 50,000)
    - cores: CPU cores to allocate (determined by workflow priority)

    VUs are distributed across the allocated cores. For example:
    - 50,000 VUs / 4 cores = 12,500 VUs per core

    Context (AD-49): ``context`` is the job's context by workflow namespace
    (``Context.dict()``: workflow name -> values) as of ``context_version``,
    the job's context layer when the workflow was dispatched. The run starts
    from all of it, as a run does in one process: ``Provide`` hooks write
    the namespaces they target and ``Use`` hooks read the ones they name.
    """

    job_id: str  # Parent job identifier
    workflow_id: str  # Unique workflow instance ID
    workflow: bytes = b""  # Cloudpickled Workflow class
    context: bytes = b""  # Cloudpickled job context by workflow namespace
    vus: int = 0  # Virtual users (can be 50k+)
    cores: int = 0  # CPU cores to allocate (from priority)
    timeout_seconds: float = 0.0  # Execution timeout
    fence_token: int = 0  # Fencing token for at-most-once
    # Context Consistency Protocol fields
    context_version: int = 0  # Layer version for staleness detection
    # Additional fields for dispatch handling
    workflow_name: str = ""  # Name of the workflow
    job_leader_addr: tuple[str, int] | None = None  # Address of job leader

    def load_workflow(self) -> Workflow:
        return Message.load(self.workflow)

    def load_context(self) -> dict[str, dict[str, object]]:
        """The job's context by workflow namespace (empty when none)."""
        if not self.context:
            return {}
        return Message.load(self.context)
