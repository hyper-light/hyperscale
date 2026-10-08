"""Wire model ``CancelJobWorkflowsResponse`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from typing import TYPE_CHECKING
from dataclasses import dataclass, field
from .message import Message

if TYPE_CHECKING:
    from .cancel_job_workflows_request import CancelJobWorkflowsRequest


@dataclass(slots=True)
class CancelJobWorkflowsResponse(Message):
    """
    Response to a ``CancelJobWorkflowsRequest``.

    ``cancelled_workflow_ids`` is the set of sub-workflow token
    strings the worker actually cancelled — empty when the worker
    had no in-flight workflows for the job.
    """

    job_id: str
    worker_id: str = ""
    cancelled_workflow_ids: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
