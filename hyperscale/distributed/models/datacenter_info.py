"""Wire model ``DatacenterInfo`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from typing import TYPE_CHECKING
from dataclasses import dataclass
from .message import Message

if TYPE_CHECKING:
    from hyperscale.distributed.resources.datacenter_resource_view import DatacenterResourceView
    from .gate_ping_response import GatePingResponse


@dataclass(slots=True, kw_only=True)
class DatacenterInfo(Message):
    """
    Information about a datacenter as seen by a gate.

    Used in GatePingResponse to report per-DC status.
    """

    dc_id: str  # Datacenter identifier
    health: str  # DatacenterHealth value
    leader_addr: tuple[str, int] | None = None  # DC leader's TCP address
    available_cores: int = 0  # Available cores in DC
    manager_count: int = 0  # Managers in DC
    worker_count: int = 0  # Workers in DC
    # AD-41: the DC's resource pressure, when its managers have reported it
    resources: "DatacenterResourceView | None" = None
