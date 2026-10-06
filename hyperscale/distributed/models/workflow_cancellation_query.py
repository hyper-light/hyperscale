"""Wire model ``WorkflowCancellationQuery`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True)
class WorkflowCancellationQuery(Message):
    """
    Query for workflow cancellation status.

    Sent from manager to worker to poll for cancellation progress.
    """

    job_id: str
    workflow_id: str
