"""Wire model ``ManagerPingResponse`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from typing import TYPE_CHECKING
from dataclasses import dataclass, field
from .message import Message
from .worker_status import WorkerStatus

if TYPE_CHECKING:
    from hyperscale.distributed.resources.datacenter_resource_view import DatacenterResourceView


@dataclass(slots=True, kw_only=True)
class ManagerPingResponse(Message):
    """
    Ping response from a manager.

    Contains manager status, worker health, and active job info.
    """

    request_id: str  # Echoed from request
    manager_id: str  # Manager's node_id
    datacenter: str  # Datacenter identifier
    host: str  # Manager TCP host
    port: int  # Manager TCP port
    is_leader: bool  # Whether this manager is the DC leader
    state: str  # ManagerState value
    term: int  # Current leadership term
    # Capacity
    total_cores: int = 0  # Total cores across all workers
    available_cores: int = 0  # Available cores (healthy workers only)
    # Workers
    worker_count: int = 0  # Total registered workers
    healthy_worker_count: int = 0  # Workers responding to SWIM
    workers: list[WorkerStatus] = field(default_factory=list)  # Per-worker status
    # Jobs
    active_job_ids: list[str] = field(default_factory=list)  # Currently active jobs
    active_job_count: int = 0  # Number of active jobs
    active_workflow_count: int = 0  # Number of active workflows
    # Cluster info
    peer_managers: list[tuple[str, int]] = field(
        default_factory=list
    )  # Known peer manager addrs
    # AD-41: the datacenter's resource pressure as this manager's gossip
    # with its peers sees it -- what a gate reports, for a client running
    # jobs on this datacenter without one. None until a fresh report knows
    # the datacenter's capacity.
    resources: "DatacenterResourceView | None" = None
