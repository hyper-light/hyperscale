"""Wire model ``DatacenterWorkflowStatus`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from typing import TYPE_CHECKING
from dataclasses import dataclass, field
from .message import Message
from .workflow_status_info import WorkflowStatusInfo

if TYPE_CHECKING:
    from .gate_workflow_query_response import GateWorkflowQueryResponse


@dataclass(slots=True, kw_only=True)
class DatacenterWorkflowStatus(Message):
    """
    Workflow status for a single datacenter.

    Used in GateWorkflowQueryResponse to group results by DC.
    """

    dc_id: str  # Datacenter identifier
    workflows: list[WorkflowStatusInfo] = field(default_factory=list)
