"""Wire model ``WorkflowQueryRequest`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass
from .message import Message


@dataclass(slots=True, kw_only=True)
class WorkflowQueryRequest(Message):
    """
    Request to query workflow status by name.

    Client sends this to managers or gates to get status of specific
    workflows. Unknown workflow names are silently ignored.
    """

    request_id: str  # Unique request identifier
    workflow_names: list[str]  # Workflow class names to query
    job_id: str | None = None  # Optional: filter to specific job
