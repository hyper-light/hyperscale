"""Wire model ``GateWorkflowQueryResponse`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass, field
from .message import Message
from .datacenter_workflow_status import DatacenterWorkflowStatus


@dataclass(slots=True, kw_only=True)
class GateWorkflowQueryResponse(Message):
    """
    Response to workflow query from a gate.

    Contains status grouped by datacenter.
    """

    request_id: str  # Echoed from request
    gate_id: str  # Responding gate's node_id
    datacenters: list[DatacenterWorkflowStatus] = field(default_factory=list)
