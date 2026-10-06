"""Wire model ``DatacenterListResponse`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass, field
from .message import Message
from .datacenter_info import DatacenterInfo


@dataclass(slots=True)
class DatacenterListResponse(Message):
    """
    Response containing list of registered datacenters.

    Returns datacenter information including health status and capacity.
    """

    request_id: str = ""  # Echoed from request
    gate_id: str = ""  # Responding gate's node_id
    datacenters: list[DatacenterInfo] = field(default_factory=list)  # Per-DC info
    total_available_cores: int = 0  # Total available cores across all DCs
    healthy_datacenter_count: int = 0  # Count of healthy DCs
