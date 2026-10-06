"""Wire model ``GateHeartbeat`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from typing import TYPE_CHECKING
from dataclasses import dataclass, field
from .message import Message

if TYPE_CHECKING:
    from hyperscale.distributed.models.coordinates import NetworkCoordinate


@dataclass(slots=True)
class GateHeartbeat(Message):
    """
    Periodic heartbeat from gate embedded in SWIM messages.

    Contains gate-level status for cross-DC coordination.
    Gates are the top-level coordinators managing global job state.

    Piggybacking (like manager/worker discovery):
    - known_managers: Managers this gate knows about, for manager discovery
    - known_gates: Other gates this gate knows about (for gate cluster membership)
    - job_leaderships: Jobs this gate leads (for distributed consistency, like managers)
    - job_dc_managers: Per-DC manager leaders for each job (for query routing)

    Health piggyback fields (AD-19):
    - health_has_dc_connectivity: Whether gate has DC connectivity
    - health_connected_dc_count: Number of connected datacenters
    - health_throughput: Current job forwarding throughput
    - health_expected_throughput: Expected throughput
    - health_overload_state: Overload state from HybridOverloadDetector
    """

    node_id: str  # Gate identifier
    datacenter: str  # Gate's home datacenter
    is_leader: bool  # Is this the leader gate?
    term: int  # Leadership term
    version: int  # State version
    state: str  # GateState value (syncing, active, draining)
    active_jobs: int  # Number of active global jobs
    active_datacenters: int  # Number of datacenters with active work
    manager_count: int  # Number of registered managers
    tcp_host: str = ""  # Gate's TCP host (for proper storage/routing)
    tcp_port: int = 0  # Gate's TCP port (for proper storage/routing)
    # Network coordinate for RTT estimation (AD-35)
    coordinate: "NetworkCoordinate | None" = None
    # Piggybacked discovery info - managers learn about other managers/gates
    # Maps node_id -> (tcp_host, tcp_port, udp_host, udp_port, datacenter)
    known_managers: dict[str, tuple[str, int, str, int, str]] = field(
        default_factory=dict
    )
    # Maps node_id -> (tcp_host, tcp_port, udp_host, udp_port)
    known_gates: dict[str, tuple[str, int, str, int]] = field(default_factory=dict)
    # Per-job leadership - piggybacked on SWIM UDP for distributed consistency (like managers)
    # Maps job_id -> (fencing_token, target_dc_count) for jobs this gate leads
    job_leaderships: dict[str, tuple[int, int]] = field(default_factory=dict)
    # Per-job per-DC manager leaders - for query routing after failover
    # Maps job_id -> {dc_id -> (manager_host, manager_port)}
    job_dc_managers: dict[str, dict[str, tuple[str, int]]] = field(default_factory=dict)
    # Health piggyback fields (AD-19)
    health_has_dc_connectivity: bool = True
    health_connected_dc_count: int = 0
    health_throughput: float = 0.0
    health_expected_throughput: float = 0.0
    health_overload_state: str = "healthy"
    # AD-19 addendum (Phase D): uniform LHM gossip across all heartbeat
    # tiers. Gate reports its raw LocalHealthMultiplier.score (0-8) so
    # cross_dc_correlation can see gate stress alongside manager/worker
    # LHM and classify systemic load patterns.
    lhm_score: int = 0  # Local Health Multiplier score (0-8)
