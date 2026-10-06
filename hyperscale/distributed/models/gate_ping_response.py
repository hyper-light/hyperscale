"""Wire model ``GatePingResponse`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from dataclasses import dataclass, field
from .message import Message
from .datacenter_info import DatacenterInfo


@dataclass(slots=True, kw_only=True)
class GatePingResponse(Message):
    """
    Ping response from a gate.

    Contains gate status and datacenter health info.
    """

    request_id: str  # Echoed from request
    gate_id: str  # Gate's node_id
    datacenter: str  # Gate's home datacenter
    host: str  # Gate TCP host
    port: int  # Gate TCP port
    is_leader: bool  # Whether this gate is the gate cluster leader
    state: str  # GateState value
    term: int  # Current leadership term
    # Datacenters
    datacenters: list[DatacenterInfo] = field(default_factory=list)  # Per-DC status
    active_datacenter_count: int = 0  # Number of active datacenters
    # Jobs
    active_job_ids: list[str] = field(default_factory=list)  # Currently active jobs
    active_job_count: int = 0  # Number of active jobs
    # Cluster info
    peer_gates: list[tuple[str, int]] = field(
        default_factory=list
    )  # Known peer gate addrs
