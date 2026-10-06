"""Wire model ``WorkflowCancellationResponse`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True)
class WorkflowCancellationResponse(Message):
    """
    Response to workflow cancellation query.

    Contains the current cancellation status for a workflow.
    """

    job_id: str
    workflow_id: str
    workflow_name: str
    status: str  # WorkflowCancellationStatus value
    error: str | None = None
