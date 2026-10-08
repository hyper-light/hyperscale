"""Wire model ``SingleWorkflowCancelResponse`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass, field
from .message import Message


@dataclass(slots=True)
class SingleWorkflowCancelResponse(Message):
    """
    Response to a single workflow cancellation request (Section 6).

    Contains the status of the cancellation and any dependents that
    were also cancelled as a result.
    """

    job_id: str  # Parent job ID
    workflow_id: str  # Requested workflow
    request_id: str  # Echoed request ID
    status: str  # WorkflowCancellationStatus value
    cancelled_dependents: list[str] = field(
        default_factory=list
    )  # IDs of cancelled deps
    errors: list[str] = field(default_factory=list)  # Any errors during cancellation
    datacenter: str = ""  # Responding datacenter
