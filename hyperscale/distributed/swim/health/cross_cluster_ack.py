"""``CrossClusterAck`` -- pickled under the namespace
``hyperscale.distributed.swim.health.federated_health_monitor`` (see that module)."""

from dataclasses import dataclass
from hyperscale.distributed.models import Message


@dataclass(slots=True)
class CrossClusterAck(Message):
    """
    Cross-cluster health acknowledgment (xack).

    Response from DC leader with aggregate datacenter health.
    """

    # Identity
    datacenter: str
    node_id: str
    incarnation: int  # External incarnation (separate from cluster incarnation)

    # Leadership
    is_leader: bool
    leader_term: int

    # Cluster health
    cluster_size: int  # Total managers in DC
    healthy_managers: int  # Managers responding to SWIM

    # Worker capacity
    worker_count: int
    healthy_workers: int
    total_cores: int
    available_cores: int

    # Workload
    active_jobs: int
    active_workflows: int

    # Self-reported health
    dc_health: str  # "HEALTHY", "DEGRADED", "BUSY", "UNHEALTHY"

    # Optional: reason for non-healthy status
    health_reason: str = ""
