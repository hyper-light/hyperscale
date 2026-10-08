"""Wire model ``WorkflowQueryResponse`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass, field
from .message import Message
from .workflow_status_info import WorkflowStatusInfo


@dataclass(slots=True, kw_only=True)
class WorkflowQueryResponse(Message):
    """
    Response to workflow query from a manager.

    Contains status for all matching workflows.
    """

    request_id: str  # Echoed from request
    manager_id: str  # Responding manager's node_id
    datacenter: str  # Manager's datacenter
    workflows: list[WorkflowStatusInfo] = field(default_factory=list)
