"""
Distributed system message types for Gate, Manager, and Worker nodes.

These dataclasses define the wire format for all TCP communication
in the distributed Hyperscale architecture.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any

from hyperscale.core.graph import Workflow
from hyperscale.core.state import Context
from hyperscale.reporting.common.results_types import WorkflowStats
from .message import Message

if TYPE_CHECKING:
    from hyperscale.core.jobs.workers.stage_priority import StagePriority
    from hyperscale.distributed.models.coordinates import NetworkCoordinate


# =============================================================================
# Enums and Type Definitions
# =============================================================================


class NodeRole(str, Enum):
    """Role of a node in the distributed system."""

    CLIENT = "client"
    GATE = "gate"
    MANAGER = "manager"
    WORKER = "worker"


class JobStatus(str, Enum):
    """Status of a distributed job."""

    SUBMITTED = "submitted"  # Job received, not yet dispatched
    QUEUED = "queued"  # Queued for execution
    DISPATCHING = "dispatching"  # Being dispatched to workers
    RUNNING = "running"  # Active execution
    COMPLETING = "completing"  # Wrapping up, gathering results
    COMPLETED = "completed"  # Successfully finished
    FAILED = "failed"  # Failed (may be retried)
    CANCELLED = "cancelled"  # User cancelled
    TIMEOUT = "timeout"  # Exceeded time limit


class WorkflowStatus(str, Enum):
    """Status of a single workflow within a job."""

    PENDING = "pending"  # Not yet started
    ASSIGNED = "assigned"  # Assigned/dispatched to worker(s)
    RUNNING = "running"  # Executing
    COMPLETED = "completed"  # Finished successfully
    FAILED = "failed"  # Failed
    CANCELLED = "cancelled"  # Cancelled
    AGGREGATED = "aggregated"  # Results successfully aggregated (internal)
    AGGREGATION_FAILED = "aggregation_failed"  # Aggregation failed (internal)


class WorkerState(str, Enum):
    """State of a worker node."""

    HEALTHY = "healthy"  # Normal operation
    DEGRADED = "degraded"  # High load, accepting with backpressure
    DRAINING = "draining"  # Not accepting new work
    OFFLINE = "offline"  # Not responding


class ManagerState(str, Enum):
    """
    State of a manager node in the cluster.

    New Manager Join Process:
    1. Manager joins SWIM cluster → State = SYNCING
    2. SYNCING managers are NOT counted in quorum
    3. Request state sync from leader (if not leader)
    4. Apply state snapshot
    5. State = ACTIVE → now counted in quorum

    This prevents new/recovering managers from affecting quorum
    until they have synchronized state from the cluster.
    """

    SYNCING = "syncing"  # Joined cluster, syncing state (not in quorum)
    ACTIVE = "active"  # Fully operational (counted in quorum)
    DRAINING = "draining"  # Not accepting new work, draining existing
    OFFLINE = "offline"  # Not responding (aborted or crashed)


class GateState(str, Enum):
    """
    State of a gate node in the cluster.

    New Gate Join Process:
    1. Gate joins SWIM cluster → State = SYNCING
    2. SYNCING gates are NOT counted in quorum
    3. Request state sync from leader (if not leader)
    4. Apply state snapshot
    5. State = ACTIVE → now counted in quorum

    This prevents new/recovering gates from affecting quorum
    until they have synchronized state from the cluster.
    """

    SYNCING = "syncing"  # Joined cluster, syncing state (not in quorum)
    ACTIVE = "active"  # Fully operational (counted in quorum)
    DRAINING = "draining"  # Not accepting new work, draining existing


class DatacenterHealth(str, Enum):
    """
    Health classification for datacenter routing decisions.

    Key insight: BUSY ≠ UNHEALTHY
    - BUSY = transient, will clear when workflows complete → accept job (queued)
    - UNHEALTHY = structural problem, requires intervention → try fallback

    See AD-16 in docs/architecture.md for design rationale.
    """

    HEALTHY = "healthy"  # Managers responding, workers available, capacity exists
    BUSY = "busy"  # Managers responding, workers available, no immediate capacity
    DEGRADED = "degraded"  # Some managers responding, reduced capacity
    UNHEALTHY = "unhealthy"  # No managers responding OR all workers down


class DatacenterRegistrationStatus(str, Enum):
    """
    Registration status for a datacenter (distinct from health).

    Registration tracks whether managers have announced themselves to the gate.
    Health classification only applies to READY datacenters.

    State machine:
      AWAITING_INITIAL → (first heartbeat) → INITIALIZING
      INITIALIZING → (quorum heartbeats) → READY
      INITIALIZING → (grace period, no quorum) → UNAVAILABLE
      READY → (heartbeats continue) → READY
      READY → (heartbeats stop, < quorum) → PARTIAL
      READY → (all heartbeats stop) → UNAVAILABLE
    """

    AWAITING_INITIAL = "awaiting_initial"  # Configured but no heartbeats received yet
    INITIALIZING = "initializing"  # Some managers registered, waiting for quorum
    READY = "ready"  # Quorum of managers registered, health classification applies
    PARTIAL = "partial"  # Was ready, now below quorum (degraded but not lost)
    UNAVAILABLE = "unavailable"  # Was ready, lost all heartbeats (need recovery)


class UpdateTier(str, Enum):
    """
    Tiered update strategy for cross-DC stat synchronization.

    Not all stats need real-time updates. This enum defines the
    urgency/frequency tier for different types of updates.

    See AD-15 in docs/architecture.md for design rationale.
    """

    IMMEDIATE = "immediate"  # Event-driven, TCP push - completion, failure, critical
    PERIODIC = "periodic"  # Every 1-5s, TCP batch - progress, aggregate rates
    ON_DEMAND = "on_demand"  # Client request, TCP pull - step stats, historical


# =============================================================================
# Node Identity and Registration
# =============================================================================


@dataclass(slots=True)
class NodeInfo(Message):
    """
    Identity information for any node in the cluster.

    Used for registration, heartbeats, and state sync.
    """

    node_id: str  # Unique node identifier
    role: str  # NodeRole value
    host: str  # Network host
    port: int  # TCP port
    datacenter: str  # Datacenter identifier
    version: int = 0  # State version (Lamport clock)
    udp_port: int = 0  # UDP port for SWIM (defaults to 0, derived from port if not set)


@dataclass(slots=True)
class ManagerInfo(Message):
    """
    Manager identity and address information for worker discovery.

    Workers use this to maintain a list of known managers for
    redundant communication and failover.
    """

    node_id: str  # Manager's unique identifier
    tcp_host: str  # TCP host for data operations
    tcp_port: int  # TCP port for data operations
    udp_host: str  # UDP host for SWIM healthchecks
    udp_port: int  # UDP port for SWIM healthchecks
    datacenter: str  # Datacenter identifier
    is_leader: bool = False  # Whether this manager is the current leader


@dataclass(slots=True, kw_only=True)
class ManagerPeerRegistration(Message):
    """
    Registration request from one manager to another peer manager.

    When a manager discovers a new peer (via SWIM or seed list),
    it sends this registration to establish the bidirectional relationship.

    Protocol Version (AD-25):
    - protocol_version_major/minor: For version compatibility checks
    - capabilities: Comma-separated list of supported features

    Cluster Isolation (AD-28 Issue 2):
    - cluster_id: Cluster identifier for isolation validation
    - environment_id: Environment identifier for isolation validation
    """

    node: ManagerInfo  # Registering manager's info
    term: int  # Current leadership term
    is_leader: bool  # Whether registering manager is leader
    cluster_id: str = "hyperscale"  # Cluster identifier for isolation
    environment_id: str = "default"  # Environment identifier for isolation
    # Protocol version fields (AD-25) - defaults for backwards compatibility
    protocol_version_major: int = 1
    protocol_version_minor: int = 0
    capabilities: str = ""  # Comma-separated feature list


@dataclass(slots=True, kw_only=True)
class ManagerPeerRegistrationResponse(Message):
    """
    Registration acknowledgment from manager to peer manager.

    Contains list of all known peer managers so the registering
    manager can discover the full cluster topology.

    Protocol Version (AD-25):
    - protocol_version_major/minor: For version compatibility checks
    - capabilities: Comma-separated list of supported features
    """

    accepted: bool  # Whether registration was accepted
    manager_id: str  # Responding manager's node_id
    is_leader: bool  # Whether responding manager is leader
    term: int  # Responding manager's term
    known_peers: list[ManagerInfo]  # All known peer managers (for discovery)
    error: str | None = None  # Error message if not accepted
    # Protocol version fields (AD-25) - defaults for backwards compatibility
    protocol_version_major: int = 1
    protocol_version_minor: int = 0
    capabilities: str = ""  # Comma-separated feature list


@dataclass(slots=True, kw_only=True)
class RegistrationResponse(Message):
    """
    Registration acknowledgment from manager to worker.

    Contains list of all known healthy managers so worker can
    establish redundant communication channels.

    Protocol Version (AD-25):
    - protocol_version_major/minor: For version compatibility checks
    - capabilities: Comma-separated negotiated features
    """

    accepted: bool  # Whether registration was accepted
    manager_id: str  # Responding manager's node_id
    healthy_managers: list[ManagerInfo]  # All known healthy managers (including self)
    error: str | None = None  # Error message if not accepted
    # Protocol version fields (AD-25) - defaults for backwards compatibility
    protocol_version_major: int = 1
    protocol_version_minor: int = 0
    capabilities: str = ""  # Comma-separated negotiated features


@dataclass(slots=True, kw_only=True)
class ManagerToWorkerRegistration(Message):
    """
    Registration request from manager to worker.

    Enables bidirectional registration: workers register with managers,
    AND managers can register with workers discovered via state sync.
    This speeds up cluster formation by allowing managers to proactively
    reach out to workers they learn about from peer managers.
    """

    manager: ManagerInfo  # Registering manager's info
    is_leader: bool  # Whether this manager is the cluster leader
    term: int  # Current leadership term
    known_managers: list[ManagerInfo] = field(
        default_factory=list
    )  # Other managers worker should know


@dataclass(slots=True, kw_only=True)
class ManagerToWorkerRegistrationAck(Message):
    """
    Acknowledgment from worker to manager registration.
    """

    accepted: bool  # Whether registration was accepted
    worker_id: str  # Worker's node_id
    total_cores: int = 0  # Worker's total cores
    available_cores: int = 0  # Worker's available cores
    error: str | None = None  # Error message if not accepted


@dataclass(slots=True, kw_only=True)
class WorkflowProgressAck(Message):
    """
    Acknowledgment for workflow progress updates.

    Includes updated manager list so workers can maintain
    accurate view of cluster topology and leadership.

    Also includes job_leader_addr for the specific job, enabling workers
    to route progress updates to the correct manager even after failover.

    Backpressure fields (AD-23):
    When the manager's stats buffer fill level reaches thresholds, it signals
    backpressure to workers via these fields. Workers should adjust their
    update behavior accordingly (throttle, batch-only, or drop non-critical).
    """

    manager_id: str  # Responding manager's node_id
    is_leader: bool  # Whether this manager is cluster leader
    healthy_managers: list[ManagerInfo]  # Current healthy managers
    # Job leader address - the manager currently responsible for this job.
    # None if the job is unknown or this manager doesn't track it.
    # Workers should update their routing to send progress to this address.
    job_leader_addr: tuple[str, int] | None = None
    # AD-23: Backpressure fields for stats update throttling
    backpressure_level: int = (
        0  # BackpressureLevel enum value (0=NONE, 1=THROTTLE, 2=BATCH, 3=REJECT)
    )
    backpressure_delay_ms: int = 0  # Suggested delay before next update (milliseconds)
    backpressure_batch_only: bool = False  # Should sender switch to batch mode?

    def __getstate__(self) -> dict[str, object]:
        return {
            "manager_id": self.manager_id,
            "is_leader": self.is_leader,
            "healthy_managers": self.healthy_managers,
            "job_leader_addr": self.job_leader_addr,
            "backpressure_level": self.backpressure_level,
            "backpressure_delay_ms": self.backpressure_delay_ms,
            "backpressure_batch_only": self.backpressure_batch_only,
        }

    def __setstate__(self, state: object) -> None:
        if isinstance(state, dict):
            manager_id = state.get("manager_id", "")
            is_leader = state.get("is_leader", False)
            healthy_managers = state.get("healthy_managers", [])
            job_leader_addr = state.get("job_leader_addr")
            backpressure_level = state.get("backpressure_level", 0)
            backpressure_delay_ms = state.get("backpressure_delay_ms", 0)
            backpressure_batch_only = state.get("backpressure_batch_only", False)
        elif isinstance(state, (list, tuple)):
            values = list(state)
            manager_id = values[0] if len(values) > 0 else ""
            is_leader = values[1] if len(values) > 1 else False
            healthy_managers = values[2] if len(values) > 2 else []
            job_leader_addr = values[3] if len(values) > 3 else None
            backpressure_level = values[4] if len(values) > 4 else 0
            backpressure_delay_ms = values[5] if len(values) > 5 else 0
            backpressure_batch_only = values[6] if len(values) > 6 else False
        else:
            raise TypeError("Unsupported WorkflowProgressAck state")

        if healthy_managers is None:
            healthy_managers = []
        elif isinstance(healthy_managers, tuple):
            healthy_managers = list(healthy_managers)

        if isinstance(job_leader_addr, list):
            job_leader_addr = tuple(job_leader_addr)

        self.manager_id = manager_id
        self.is_leader = is_leader
        self.healthy_managers = healthy_managers
        self.job_leader_addr = job_leader_addr
        self.backpressure_level = backpressure_level
        self.backpressure_delay_ms = backpressure_delay_ms
        self.backpressure_batch_only = backpressure_batch_only


# =============================================================================
# Gate Node Identity and Discovery (Manager <-> Gate)
# =============================================================================


@dataclass(slots=True)
class GateInfo(Message):
    """
    Gate identity and address information for manager discovery.

    Managers use this to maintain a list of known gates for
    redundant communication and failover.
    """

    node_id: str  # Gate's unique identifier
    tcp_host: str  # TCP host for data operations
    tcp_port: int  # TCP port for data operations
    udp_host: str  # UDP host for SWIM healthchecks
    udp_port: int  # UDP port for SWIM healthchecks
    datacenter: str  # Datacenter identifier (gate's home DC)
    is_leader: bool = False  # Whether this gate is the current leader


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


@dataclass(slots=True, kw_only=True)
class ManagerRegistrationResponse(Message):
    """
    Registration acknowledgment from gate to manager.

    Contains list of all known healthy gates so manager can
    establish redundant communication channels.

    Protocol Version (AD-25):
    - protocol_version_major/minor: For version compatibility checks
    - capabilities: Comma-separated negotiated features
    """

    accepted: bool  # Whether registration was accepted
    gate_id: str  # Responding gate's node_id
    healthy_gates: list[GateInfo]  # All known healthy gates (including self)
    error: str | None = None  # Error message if not accepted
    # Protocol version fields (AD-25) - defaults for backwards compatibility
    protocol_version_major: int = 1
    protocol_version_minor: int = 0
    capabilities: str = ""  # Comma-separated negotiated features


@dataclass(slots=True, kw_only=True)
class GateRegistrationRequest(Message):
    """
    Registration request from gate to manager.

    Gates register with all managers at startup (symmetric to managers
    registering with all gates). This ensures managers know about all
    gates for proper routing and health tracking.

    Protocol Version (AD-25):
    - protocol_version_major/minor: For version compatibility checks
    - capabilities: Comma-separated list of supported features

    Cluster Isolation (AD-28 Issue 2):
    - cluster_id: Cluster identifier for isolation validation
    - environment_id: Environment identifier for isolation validation
    """

    node_id: str  # Gate's unique identifier
    tcp_host: str  # Gate's TCP host
    tcp_port: int  # Gate's TCP port
    udp_host: str  # Gate's UDP host
    udp_port: int  # Gate's UDP port
    is_leader: bool  # Whether this gate is the leader
    term: int  # Current leadership term
    state: str  # GateState value
    cluster_id: str = "hyperscale"  # Cluster identifier for isolation
    environment_id: str = "default"  # Environment identifier for isolation
    active_jobs: int = 0  # Number of active jobs
    manager_count: int = 0  # Number of known managers
    # Protocol version fields (AD-25)
    protocol_version_major: int = 1
    protocol_version_minor: int = 0
    capabilities: str = ""  # Comma-separated feature list


@dataclass(slots=True, kw_only=True)
class GateRegistrationResponse(Message):
    """
    Registration acknowledgment from manager to gate.

    Contains list of all known managers so gate can establish
    redundant communication channels across datacenters.

    Protocol Version (AD-25):
    - protocol_version_major/minor: For version compatibility checks
    - capabilities: Comma-separated negotiated features
    """

    accepted: bool  # Whether registration was accepted
    manager_id: str  # Responding manager's node_id
    datacenter: str  # Manager's datacenter
    healthy_managers: list[ManagerInfo]  # All known healthy managers
    error: str | None = None  # Error message if not accepted
    # Protocol version fields (AD-25)
    protocol_version_major: int = 1
    protocol_version_minor: int = 0
    capabilities: str = ""  # Comma-separated negotiated features


@dataclass(slots=True, kw_only=True)
class ManagerDiscoveryBroadcast(Message):
    """
    Broadcast from one gate to another about a newly discovered manager.

    Used for cross-gate synchronization of manager discovery.
    When a manager registers with one gate, that gate broadcasts
    to all peer gates so they can also track the manager.

    Includes manager status so peer gates can also update _datacenter_status.
    """

    datacenter: str  # Manager's datacenter
    manager_tcp_addr: tuple[str, int]  # Manager's TCP address
    manager_udp_addr: tuple[str, int] | None = None  # Manager's UDP address (if known)
    source_gate_id: str = ""  # Gate that received the original registration
    # Manager status info (from registration heartbeat)
    worker_count: int = 0  # Number of workers manager has
    healthy_worker_count: int = 0  # Healthy workers (SWIM responding)
    available_cores: int = 0  # Available cores for job dispatch
    total_cores: int = 0  # Total cores across all workers


@dataclass(slots=True, kw_only=True)
class WorkerDiscoveryBroadcast(Message):
    """
    Broadcast from one manager to another about a newly discovered worker.

    Used for cross-manager synchronization of worker discovery.
    When a worker registers with one manager, that manager broadcasts
    to all peer managers so they can also track the worker.
    """

    worker_id: str  # Worker's node_id
    worker_tcp_addr: tuple[str, int]  # Worker's TCP address
    worker_udp_addr: tuple[str, int]  # Worker's UDP address
    datacenter: str  # Worker's datacenter
    available_cores: int  # Worker's available cores
    source_manager_id: str = ""  # Manager that received the original registration


@dataclass(slots=True, kw_only=True)
class JobProgressAck(Message):
    """
    Acknowledgment for job progress updates from gates to managers.

    Includes updated gate list so managers can maintain
    accurate view of gate cluster topology and leadership.
    """

    gate_id: str  # Responding gate's node_id
    is_leader: bool  # Whether this gate is leader
    healthy_gates: list[GateInfo]  # Current healthy gates


@dataclass(slots=True)
class WorkerRegistration(Message):
    """
    Worker registration message sent to managers.

    Contains worker identity and capacity information.

    Protocol Version (AD-25):
    - protocol_version_major/minor: For version compatibility checks
    - capabilities: Comma-separated list of supported features

    Cluster Isolation (AD-28 Issue 2):
    - cluster_id: Cluster identifier for isolation validation
    - environment_id: Environment identifier for isolation validation
    """

    node: NodeInfo  # Worker identity
    total_cores: int  # Total CPU cores available
    available_cores: int  # Currently free cores
    memory_mb: int  # Total memory in MB
    available_memory_mb: int = 0  # Currently free memory
    cluster_id: str = ""  # Cluster identifier for isolation
    environment_id: str = ""  # Environment identifier for isolation
    # Protocol version fields (AD-25) - defaults for backwards compatibility
    protocol_version_major: int = 1
    protocol_version_minor: int = 0
    capabilities: str = ""  # Comma-separated feature list


@dataclass(slots=True)
class WorkerHeartbeat(Message):
    """
    Periodic heartbeat from worker to manager.

    Contains current state and resource utilization.

    Health piggyback fields (AD-19):
    - health_accepting_work: Whether worker is accepting new work
    - health_throughput: Current workflow completions per interval
    - health_expected_throughput: Expected throughput based on capacity
    - health_overload_state: Overload state from HybridOverloadDetector
    """

    node_id: str  # Worker identifier
    state: str  # WorkerState value
    available_cores: int  # Free cores
    queue_depth: int  # Pending workflow count
    cpu_percent: float  # CPU utilization 0-100
    memory_percent: float  # Memory utilization 0-100
    version: int  # State version for sync
    # Active workflows and their status
    active_workflows: dict[str, str] = field(default_factory=dict)
    # TCP address for routing (populated in UDP heartbeats)
    tcp_host: str = ""
    tcp_port: int = 0
    # Network coordinate for RTT estimation (AD-35)
    coordinate: "NetworkCoordinate | None" = None
    # Health piggyback fields (AD-19)
    health_accepting_work: bool = True
    health_throughput: float = 0.0
    health_expected_throughput: float = 0.0
    health_overload_state: str = "healthy"
    # Extension request piggyback (AD-26)
    # Workers can request deadline extensions via heartbeat instead of separate TCP call
    extension_requested: bool = False
    extension_reason: str = ""
    extension_current_progress: float = (
        0.0  # 0.0-1.0 progress indicator (backward compatibility)
    )
    extension_estimated_completion: float = 0.0  # Estimated seconds until completion
    extension_active_workflow_count: int = 0  # Number of workflows currently executing
    # AD-26 Issue 4: Absolute progress metrics (preferred over relative progress)
    extension_completed_items: int = 0  # Absolute count of completed items
    extension_total_items: int = 0  # Total items to complete


@dataclass(slots=True)
class ManagerHeartbeat(Message):
    """
    Periodic heartbeat from manager to gates (if gates present).

    Contains datacenter-level job status summary.

    Datacenter Health Classification (evaluated in order):
    1. DEGRADED: majority of workers unhealthy (healthy_worker_count < worker_count // 2 + 1)
       OR majority of managers unhealthy (alive_managers < total_managers // 2 + 1)
       (structural problem - reduced capacity, may need intervention)
    2. BUSY: NOT degraded AND available_cores == 0
       (transient - all cores occupied, jobs will be queued until capacity frees up)
    3. HEALTHY: NOT degraded AND available_cores > 0
       (normal operation - capacity available for new jobs)
    4. UNHEALTHY: no managers responding OR no workers registered
       (severe - cannot process jobs)

    Piggybacking:
    - job_leaderships: Jobs this manager leads (for distributed consistency)
    - known_gates: Gates this manager knows about (for gate discovery)

    Health piggyback fields (AD-19):
    - health_accepting_jobs: Whether manager is accepting new jobs
    - health_has_quorum: Whether manager has worker quorum
    - health_throughput: Current job/workflow throughput
    - health_expected_throughput: Expected throughput based on capacity
    - health_overload_state: Overload state from HybridOverloadDetector

    Protocol Version (AD-25):
    - protocol_version_major/minor: For version compatibility checks
    - capabilities: Comma-separated list of supported features

    Cluster Isolation (AD-28 Issue 2):
    - cluster_id: Cluster identifier for isolation validation
    - environment_id: Environment identifier for isolation validation
    """

    node_id: str  # Manager identifier
    datacenter: str  # Datacenter identifier
    is_leader: bool  # Is this the leader manager?
    term: int  # Leadership term
    version: int  # State version
    active_jobs: int  # Number of active jobs
    active_workflows: int  # Number of active workflows
    worker_count: int  # Number of registered workers (total)
    healthy_worker_count: int  # Number of workers responding to SWIM probes
    available_cores: int  # Total available cores across healthy workers
    total_cores: int  # Total cores across all registered workers
    cluster_id: str = "hyperscale"  # Cluster identifier for isolation
    environment_id: str = "default"  # Environment identifier for isolation
    state: str = "active"  # ManagerState value (syncing/active/draining)
    tcp_host: str = ""  # Manager's TCP host (for proper storage key)
    tcp_port: int = 0  # Manager's TCP port (for proper storage key)
    udp_host: str = ""  # Manager's UDP host (for SWIM registration)
    udp_port: int = 0  # Manager's UDP port (for SWIM registration)
    # Network coordinate for RTT estimation (AD-35)
    coordinate: "NetworkCoordinate | None" = None
    # Per-job leadership - piggybacked on SWIM UDP for distributed consistency
    # Maps job_id -> (fencing_token, layer_version) for jobs this manager leads
    job_leaderships: dict[str, tuple[int, int]] = field(default_factory=dict)
    # Piggybacked gate discovery - gates learn about other gates from managers
    # Maps gate_id -> (tcp_host, tcp_port, udp_host, udp_port)
    known_gates: dict[str, tuple[str, int, str, int]] = field(default_factory=dict)
    # Gate cluster leadership tracking - propagated among managers for consistency
    # When a manager discovers a gate leader, it piggybacks this info to peer managers
    current_gate_leader_id: str | None = None
    current_gate_leader_host: str | None = None
    current_gate_leader_port: int | None = None
    # Health piggyback fields (AD-19)
    health_accepting_jobs: bool = True
    health_has_quorum: bool = True
    health_throughput: float = 0.0
    health_expected_throughput: float = 0.0
    health_overload_state: str = "healthy"
    # Worker overload tracking for DC-level health classification
    # Counts workers in "overloaded" state (from HybridOverloadDetector)
    # Used by gates to factor overload into DC health, not just connectivity
    overloaded_worker_count: int = 0
    stressed_worker_count: int = 0
    busy_worker_count: int = 0
    # Extension and LHM tracking for cross-DC correlation (Phase 7)
    # Used by gates to distinguish load from failures
    workers_with_extensions: int = 0  # Workers currently with active extensions
    lhm_score: int = 0  # Local Health Multiplier score (0-8, higher = more stressed)
    # AD-37: Backpressure fields for gate throttling
    # Gates use these to throttle forwarded updates when managers are under load
    backpressure_level: int = (
        0  # BackpressureLevel enum value (0=NONE, 1=THROTTLE, 2=BATCH, 3=REJECT)
    )
    backpressure_delay_ms: int = 0  # Suggested delay before next update (milliseconds)
    # Protocol version fields (AD-25) - defaults for backwards compatibility
    protocol_version_major: int = 1
    protocol_version_minor: int = 0
    capabilities: str = ""  # Comma-separated feature list


# =============================================================================
# Job Submission and Dispatch
# =============================================================================


@dataclass(slots=True)
class JobSubmission(Message):
    """
    Job submission from client to gate or manager.

    A job contains one or more workflow classes to execute.

    Workflow format (cloudpickled):
        list[tuple[str, list[str], Workflow]]
        - str: workflow_id (client-generated, globally unique)
        - list[str]: dependency workflow names
        - Workflow: the workflow instance

    The workflow_id is generated by the client to ensure consistency across
    all datacenters. Gates and managers use these IDs to track and correlate
    results from different DCs for the same logical workflow.

    If callback_addr is provided, the gate/manager will push status
    updates to the client via TCP instead of requiring polling.

    If reporting_configs is provided (cloudpickled list of ReporterConfig),
    the manager/gate will submit results to reporters after aggregation
    and notify the client of success/failure per reporter.

    Protocol Version (AD-25):
    - protocol_version_major/minor: For version compatibility checks
    - capabilities: Comma-separated list of features client supports
    """

    job_id: str  # Unique job identifier
    workflows: bytes  # Cloudpickled list[tuple[str, list[str], Workflow]]
    vus: int  # Virtual users (cores to use per workflow)
    timeout_seconds: float  # Maximum execution time
    datacenter_count: int = 1  # Number of DCs to run in (gates only)
    datacenters: list[str] = field(default_factory=list)
    # Optional callback address for push notifications
    # If set, server pushes status updates to this address
    callback_addr: tuple[str, int] | None = None
    # Origin gate address for direct DC-to-Job-Leader routing
    # Set by the job leader gate when dispatching to managers
    # Managers send results directly to this gate instead of all gates
    origin_gate_addr: tuple[str, int] | None = None
    # Optional reporter configs for result submission
    # Cloudpickled list of ReporterConfig objects
    # If set, manager/gate submits results to these reporters after aggregation
    reporting_configs: bytes = b""
    # Protocol version fields (AD-25) - defaults for backwards compatibility
    protocol_version_major: int = 1
    protocol_version_minor: int = 0
    capabilities: str = ""  # Comma-separated feature list
    # Idempotency key (AD-40) - if provided, gate uses idempotency cache to prevent duplicate processing
    idempotency_key: str | None = None


@dataclass(slots=True)
class JobAck(Message):
    """
    Acknowledgment of job submission.

    Returned immediately after job is accepted for processing.
    If rejected due to not being leader, leader_addr provides redirect target.

    Protocol Version (AD-25):
    - protocol_version_major/minor: Server's protocol version
    - capabilities: Comma-separated negotiated features
    """

    job_id: str  # Job identifier
    accepted: bool  # Whether job was accepted
    error: str | None = None  # Error message if rejected
    queued_position: int = 0  # Position in queue (if queued)
    leader_addr: tuple[str, int] | None = None  # Leader address for redirect
    # Protocol version fields (AD-25) - defaults for backwards compatibility
    protocol_version_major: int = 1
    protocol_version_minor: int = 0
    capabilities: str = ""  # Comma-separated negotiated features


@dataclass(slots=True)
class WorkflowDispatch(Message):
    """
    Dispatch a single workflow to a worker.

    Sent from manager to worker for execution.

    Resource Model:
    - vus: Virtual users (can be large, e.g., 50,000)
    - cores: CPU cores to allocate (determined by workflow priority)

    VUs are distributed across the allocated cores. For example:
    - 50,000 VUs / 4 cores = 12,500 VUs per core

    Context Consistency Protocol:
    - context_version: The layer version this dispatch is for
    - dependency_context: Context from dependencies (subset of full context)

    Workers can verify they have the correct context version before execution.
    """

    job_id: str  # Parent job identifier
    workflow_id: str  # Unique workflow instance ID
    workflow: bytes = b""  # Cloudpickled Workflow class
    context: bytes = b""  # Cloudpickled context dict (legacy, may be empty)
    vus: int = 0  # Virtual users (can be 50k+)
    cores: int = 0  # CPU cores to allocate (from priority)
    timeout_seconds: float = 0.0  # Execution timeout
    fence_token: int = 0  # Fencing token for at-most-once
    # Context Consistency Protocol fields
    context_version: int = 0  # Layer version for staleness detection
    dependency_context: bytes = b""  # Context from dependencies only
    # Additional fields for dispatch handling
    workflow_name: str = ""  # Name of the workflow
    job_leader_addr: tuple[str, int] | None = None  # Address of job leader

    def load_workflow(self) -> Workflow:
        return Message.load(self.workflow)

    def load_context(self) -> dict[str, Any]:
        if not self.context:
            return {}
        return Message.load(self.context)


@dataclass(slots=True)
class WorkflowDispatchAck(Message):
    """
    Worker acknowledgment of workflow dispatch.
    """

    workflow_id: str  # Workflow identifier
    accepted: bool  # Whether worker accepted
    error: str | None = None  # Error message if rejected
    cores_assigned: int = 0  # Actual cores assigned


# =============================================================================
# Cancellation (AD-20)
# =============================================================================


@dataclass(slots=True)
class JobCancelRequest(Message):
    """
    Request to cancel a running job (AD-20).

    Can be sent from:
    - Client -> Gate (global cancellation across all DCs)
    - Client -> Manager (DC-local cancellation)
    - Gate -> Manager (forwarding client request)
    - Manager -> Worker (cancel specific workflows)

    The fence_token is used for consistency:
    - If provided, only cancel if the job's current fence token matches
    - This prevents cancelling a restarted job after a crash recovery
    """

    job_id: str  # Job to cancel
    requester_id: str  # Who requested cancellation (for audit)
    timestamp: float  # When cancellation was requested
    fence_token: int = 0  # Fence token for consistency (0 = ignore)
    reason: str = ""  # Optional cancellation reason


@dataclass(slots=True)
class JobCancelResponse(Message):
    """
    Response to a job cancellation request (AD-20).

    Returned by:
    - Gate: Aggregated result from all DCs
    - Manager: DC-local result
    - Worker: Workflow-level result
    """

    job_id: str  # Job that was cancelled
    success: bool  # Whether cancellation succeeded
    cancelled_workflow_count: int = 0  # Number of workflows cancelled
    already_cancelled: bool = False  # True if job was already cancelled
    already_completed: bool = False  # True if job was already completed
    error: str | None = None  # Error message if failed


@dataclass(slots=True)
class WorkflowCancelRequest(Message):
    """
    Request to cancel a specific workflow on a worker (AD-20).

    Sent from Manager -> Worker for individual workflow cancellation.
    """

    job_id: str  # Parent job ID
    workflow_id: str  # Specific workflow to cancel
    requester_id: str = ""  # Who requested cancellation
    timestamp: float = 0.0  # When cancellation was requested
    reason: str = ""  # Optional cancellation reason


@dataclass(slots=True)
class WorkflowCancelResponse(Message):
    """
    Response to a workflow cancellation request (AD-20).

    Returned by Worker -> Manager after attempting cancellation.
    """

    job_id: str  # Parent job ID
    workflow_id: str  # Workflow that was cancelled
    success: bool  # Whether cancellation succeeded
    was_running: bool = False  # True if workflow was actively running
    already_completed: bool = False  # True if already finished
    error: str | None = None  # Error message if failed


@dataclass(slots=True)
class WorkflowCancellationComplete(Message):
    """
    Push notification from Worker -> Manager when workflow cancellation completes.

    Sent after _cancel_workflow() finishes (success or failure) to notify the
    manager that the workflow has been fully cancelled and cleanup is done.
    This enables the manager to:
    1. Update workflow status to CANCELLED
    2. Aggregate errors across all workers
    3. Push completion notification to origin gate/client
    """

    job_id: str  # Parent job ID
    workflow_id: str  # Workflow that was cancelled
    success: bool  # True if cancellation succeeded without errors
    errors: list[str] = field(default_factory=list)  # Any errors during cancellation
    cancelled_at: float = 0.0  # Timestamp when cancellation completed
    node_id: str = ""  # Worker node ID that performed cancellation


@dataclass(slots=True)
class JobCancellationComplete(Message):
    """
    Push notification from Manager -> Gate/Client when job cancellation completes.

    Sent after all workflows for a job have been cancelled. Aggregates results
    from all workers and includes any errors encountered during cancellation.
    This enables the client to:
    1. Know when cancellation is fully complete (not just acknowledged)
    2. See any errors that occurred during cancellation
    3. Clean up local job state
    """

    job_id: str  # Job that was cancelled
    success: bool  # True if all workflows cancelled without errors
    cancelled_workflow_count: int = 0  # Number of workflows that were cancelled
    total_workflow_count: int = 0  # Total workflows that needed cancellation
    errors: list[str] = field(
        default_factory=list
    )  # Aggregated errors from all workers
    cancelled_at: float = 0.0  # Timestamp when cancellation completed


# =============================================================================
# Workflow-Level Cancellation (Section 6)
# =============================================================================


class WorkflowCancellationStatus(str, Enum):
    """Status result for workflow cancellation request."""

    CANCELLED = "cancelled"  # Successfully cancelled
    PENDING_CANCELLED = "pending_cancelled"  # Was pending, now cancelled
    ALREADY_CANCELLED = "already_cancelled"  # Was already cancelled
    ALREADY_COMPLETED = "already_completed"  # Already finished, can't cancel
    NOT_FOUND = "not_found"  # Workflow not found
    CANCELLING = "cancelling"  # Cancellation in progress


@dataclass(slots=True)
class SingleWorkflowCancelRequest(Message):
    """
    Request to cancel a specific workflow (Section 6).

    Can be sent from:
    - Client -> Gate (cross-DC workflow cancellation)
    - Gate -> Manager (DC-specific workflow cancellation)
    - Client -> Manager (direct DC workflow cancellation)

    If cancel_dependents is True, all workflows that depend on this one
    will also be cancelled recursively.
    """

    job_id: str  # Parent job ID
    workflow_id: str  # Specific workflow to cancel
    request_id: str  # Unique request ID for tracking/dedup
    requester_id: str  # Who requested cancellation
    timestamp: float  # When request was made
    cancel_dependents: bool = True  # Also cancel dependent workflows
    origin_gate_addr: tuple[str, int] | None = None  # For result push
    origin_client_addr: tuple[str, int] | None = None  # For direct client push


@dataclass(slots=True)
class SingleWorkflowCancelResponse(Message):
    """
    Response to a single workflow cancellation request (Section 6).

    Contains the status of the cancellation and any dependents that
    were also cancelled as a result.
    """

    job_id: str  # Parent job ID
    workflow_id: str  # Requested workflow
    request_id: str  # Echoed request ID
    status: str  # WorkflowCancellationStatus value
    cancelled_dependents: list[str] = field(
        default_factory=list
    )  # IDs of cancelled deps
    errors: list[str] = field(default_factory=list)  # Any errors during cancellation
    datacenter: str = ""  # Responding datacenter


@dataclass(slots=True)
class WorkflowCancellationPeerNotification(Message):
    """
    Peer notification for workflow cancellation (Section 6).

    Sent from manager-to-manager or gate-to-gate to synchronize
    cancellation state across the cluster. Ensures all peers mark
    the workflow (and dependents) as cancelled to prevent resurrection.
    """

    job_id: str  # Parent job ID
    workflow_id: str  # Primary workflow cancelled
    request_id: str  # Original request ID
    origin_node_id: str  # Node that initiated cancellation
    cancelled_workflows: list[str] = field(
        default_factory=list
    )  # All cancelled (incl deps)
    timestamp: float = 0.0  # When cancellation occurred


@dataclass(slots=True)
class CancelledWorkflowInfo:
    """
    Tracking info for a cancelled workflow (Section 6).

    Stored in manager's _cancelled_workflows bucket to prevent
    resurrection of cancelled workflows.
    """

    job_id: str  # Parent job ID
    workflow_id: str  # Cancelled workflow ID
    cancelled_at: float  # When cancelled
    request_id: str  # Original request ID
    dependents: list[str] = field(default_factory=list)  # Cancelled dependents


# =============================================================================
# Adaptive Healthcheck Extensions (AD-26)
# =============================================================================


@dataclass(slots=True)
class HealthcheckExtensionRequest(Message):
    """
    Request from worker for deadline extension (AD-26).

    Workers can request deadline extensions when:
    - Executing long-running workflows
    - System is under heavy load but making progress
    - Approaching timeout but not stuck

    Extensions use logarithmic decay:
    - First extension: base/2 (e.g., 15s with base=30s)
    - Second extension: base/4 (e.g., 7.5s)
    - Continues until min_grant is reached

    Sent from: Worker -> Manager

    AD-26 Issue 4: Absolute metrics provide more robust progress tracking
    than relative 0-1 progress values. For long-running work, absolute
    metrics (100 items → 101 items) are easier to track than relative
    progress (0.995 → 0.996) and avoid float precision issues.
    """

    worker_id: str  # Worker requesting extension
    reason: str  # Why extension is needed
    current_progress: float  # Progress metric (must increase for approval) - kept for backward compatibility
    estimated_completion: float  # Estimated seconds until completion
    active_workflow_count: int  # Number of workflows currently executing
    # AD-26 Issue 4: Absolute progress metrics (preferred over relative progress)
    completed_items: int | None = None  # Absolute count of completed items
    total_items: int | None = None  # Total items to complete


@dataclass(slots=True)
class HealthcheckExtensionResponse(Message):
    """
    Response to a healthcheck extension request (AD-26).

    If granted, the worker's deadline is extended by extension_seconds.
    If denied, the denial_reason explains why.

    Extensions may be denied if:
    - Maximum extensions already granted
    - No progress since last extension
    - Worker is being evicted

    Graceful exhaustion:
    - is_exhaustion_warning: True when close to exhaustion (remaining <= threshold)
    - grace_period_remaining: Seconds of grace time left after exhaustion
    - in_grace_period: True if exhausted but still within grace period

    Sent from: Manager -> Worker
    """

    granted: bool  # Whether extension was granted
    extension_seconds: float  # Seconds of extension granted (0 if denied)
    new_deadline: float  # New deadline timestamp (if granted)
    remaining_extensions: int  # Number of extensions remaining
    denial_reason: str | None = None  # Why extension was denied
    is_exhaustion_warning: bool = False  # True if about to exhaust extensions
    grace_period_remaining: float = 0.0  # Seconds of grace remaining after exhaustion
    in_grace_period: bool = False  # True if exhausted but within grace period


# =============================================================================
# Status Updates and Reporting
# =============================================================================


@dataclass(slots=True)
class StepStats(Message):
    """
    Statistics for a single workflow step.
    """

    step_name: str  # Step method name
    completed_count: int = 0  # Successful executions
    failed_count: int = 0  # Failed executions
    total_count: int = 0  # Total attempts


@dataclass(slots=True)
class WorkflowProgress(Message):
    """
    Progress update for a running workflow.

    Sent from worker to manager during execution.

    Key fields for rapid provisioning:
    - assigned_cores: Which CPU cores are executing this workflow
    - cores_completed: How many cores have finished their portion

    When cores_completed > 0, the manager can immediately provision new
    workflows to the freed cores without waiting for the entire workflow
    to complete on all cores.

    Time alignment:
    - collected_at: Unix timestamp when stats were collected at the worker.
      Used for time-aligned aggregation across workers/DCs.
    - timestamp: Monotonic timestamp for local ordering (not cross-node comparable).
    """

    job_id: str  # Parent job
    workflow_id: str  # Workflow instance
    workflow_name: str  # Workflow class name
    status: str  # WorkflowStatus value
    completed_count: int  # Total actions completed
    failed_count: int  # Total actions failed
    rate_per_second: float  # Current execution rate
    elapsed_seconds: float  # Time since start
    step_stats: list["StepStats"] = field(default_factory=list)
    timestamp: float = 0.0  # Monotonic timestamp (local ordering)
    collected_at: float = (
        0.0  # Unix timestamp when stats were collected (cross-node alignment)
    )
    assigned_cores: list[int] = field(default_factory=list)  # Per-core assignment
    cores_completed: int = 0  # Cores that have finished their portion
    avg_cpu_percent: float = 0.0  # Average CPU utilization
    avg_memory_mb: float = 0.0  # Average memory usage in MB
    vus: int = 0  # Virtual users (from workflow config)
    worker_workflow_assigned_cores: int = 0
    worker_workflow_completed_cores: int = 0
    worker_available_cores: int = 0  # Available cores for worker.


@dataclass(slots=True)
class WorkflowFinalResult(Message):
    """
    Final result of a workflow execution.

    Sent from worker to manager when a workflow completes (success or failure).
    This triggers:
    1. Context storage (for dependent workflows)
    2. Job completion check
    3. Final result aggregation
    4. Core availability update (manager uses worker_available_cores to track capacity)

    Note: WorkflowStats already contains run_id, elapsed, and step results.
    """

    job_id: str  # Parent job
    workflow_id: str  # Workflow instance
    workflow_name: str  # Workflow class name
    status: str  # COMPLETED | FAILED
    results: list[WorkflowStats]  # Cloudpickled list[WorkflowResults]
    context_updates: bytes  # Cloudpickled context dict (for Provide hooks)
    error: str | None = None  # Error message if failed (no traceback)
    worker_id: str = ""  # Worker that executed this workflow
    worker_available_cores: int = 0  # Worker's available cores after completion


@dataclass(slots=True)
class WorkflowResult(Message):
    """
    Simplified workflow result for aggregation (without context).

    Used in JobFinalResult for Manager -> Gate communication.
    Context is NOT included because gates don't need it.

    For gate-bound jobs: results contains raw per-core WorkflowStats for cross-DC aggregation
    For direct-client jobs: results contains aggregated WorkflowStats (single item list)
    """

    workflow_id: str  # Workflow instance ID
    workflow_name: str  # Workflow class name
    status: str  # COMPLETED | FAILED
    results: list[WorkflowStats] = field(
        default_factory=list
    )  # Per-core or aggregated stats
    error: str | None = None  # Error message if failed


@dataclass(slots=True)
class WorkflowDCResult:
    """Per-datacenter workflow result for cross-DC visibility."""

    datacenter: str  # Datacenter identifier
    status: str  # COMPLETED | FAILED
    stats: WorkflowStats | None = None  # Aggregated stats for this DC (test workflows)
    error: str | None = None  # Error message if failed
    elapsed_seconds: float = 0.0
    # Raw results list for non-test workflows (unaggregated)
    raw_results: list[WorkflowStats] = field(default_factory=list)


@dataclass(slots=True)
class WorkflowResultPush(Message):
    """
    Push notification for a completed workflow's results.

    Sent from Manager to Client (aggregated) or Manager to Gate (raw) as soon
    as each workflow completes, without waiting for the entire job to finish.

    For client-bound from manager: results contains single aggregated WorkflowStats, per_dc_results empty
    For client-bound from gate: results contains cross-DC aggregated, per_dc_results has per-DC breakdown
    For gate-bound: results contains raw per-core WorkflowStats list for cross-DC aggregation
    """

    job_id: str  # Parent job
    workflow_id: str  # Workflow instance ID
    workflow_name: str  # Workflow class name
    datacenter: str  # Source datacenter (or "aggregated" for cross-DC)
    status: str  # COMPLETED | FAILED
    fence_token: int = 0
    results: list[WorkflowStats] = field(default_factory=list)
    error: str | None = None  # Error message if failed
    elapsed_seconds: float = 0.0
    # Per-DC breakdown (populated when gate aggregates cross-DC results)
    per_dc_results: list[WorkflowDCResult] = field(default_factory=list)
    # Completion timestamp for ordering
    completed_at: float = 0.0  # Unix timestamp when workflow completed
    # Whether this workflow contains test hooks (determines aggregation behavior)
    # True: aggregate results using merge_results()
    # False: return raw list of WorkflowStats per DC
    is_test: bool = True


@dataclass(slots=True)
class JobFinalResult(Message):
    """
    Final result for a job from one datacenter.

    Sent from Manager to Gate (or directly to Client if no gates).
    Contains per-workflow results and aggregated stats.
    """

    job_id: str  # Job identifier
    datacenter: str  # Reporting datacenter
    status: str  # COMPLETED | FAILED | PARTIAL
    workflow_results: list["WorkflowResult"] = field(default_factory=list)
    total_completed: int = 0  # Total successful actions
    total_failed: int = 0  # Total failed actions
    errors: list[str] = field(default_factory=list)  # All error messages
    elapsed_seconds: float = 0.0  # Max elapsed across workflows
    fence_token: int = 0  # Fencing token for at-most-once semantics


@dataclass(slots=True)
class AggregatedJobStats(Message):
    """
    Aggregated statistics across all datacenters.

    Part of GlobalJobResult for cross-DC aggregation.
    """

    total_requests: int = 0  # Total actions across all DCs
    successful_requests: int = 0  # Successful actions
    failed_requests: int = 0  # Failed actions
    overall_rate: float = 0.0  # Combined rate (requests/sec)
    avg_latency_ms: float = 0.0  # Average latency
    p50_latency_ms: float = 0.0  # Median latency
    p95_latency_ms: float = 0.0  # 95th percentile
    p99_latency_ms: float = 0.0  # 99th percentile


@dataclass(slots=True)
class GlobalJobResult(Message):
    """
    Global job result aggregated across all datacenters.

    Sent from Gate to Client as the final result.
    Contains per-DC breakdown and cross-DC aggregation.
    """

    job_id: str  # Job identifier
    status: str  # COMPLETED | FAILED | PARTIAL
    # Per-datacenter breakdown
    per_datacenter_results: list["JobFinalResult"] = field(default_factory=list)
    per_datacenter_statuses: dict[str, str] = field(default_factory=dict)
    # Cross-DC aggregated stats
    aggregated: "AggregatedJobStats" = field(default_factory=AggregatedJobStats)
    # Summary
    total_completed: int = 0  # Sum across all DCs
    total_failed: int = 0  # Sum across all DCs
    successful_datacenters: int = 0
    failed_datacenters: int = 0
    errors: list[str] = field(default_factory=list)  # All errors from all DCs
    elapsed_seconds: float = 0.0  # Max elapsed across all DCs


@dataclass(slots=True)
class JobProgress(Message):
    """
    Aggregated job progress from manager to gate.

    Contains summary of all workflows in the job.

    Time alignment:
    - collected_at: Unix timestamp when stats were aggregated at the manager.
      Used for time-aligned aggregation across DCs at the gate.
    - timestamp: Monotonic timestamp for local ordering (not cross-node comparable).
    """

    job_id: str  # Job identifier
    datacenter: str  # Reporting datacenter
    status: str  # JobStatus value
    workflows: list["WorkflowProgress"] = field(default_factory=list)
    total_completed: int = 0  # Total actions completed
    total_failed: int = 0  # Total actions failed
    overall_rate: float = 0.0  # Aggregate rate
    elapsed_seconds: float = 0.0  # Time since job start
    timestamp: float = 0.0  # Monotonic timestamp (local ordering)
    collected_at: float = 0.0  # Unix timestamp when aggregated (cross-DC alignment)
    # Aggregated step stats across all workflows in the job
    step_stats: list["StepStats"] = field(default_factory=list)
    fence_token: int = 0  # Fencing token for at-most-once semantics


@dataclass(slots=True)
class GlobalJobStatus(Message):
    """
    Global job status aggregated by gate across datacenters.

    This is what gets returned to the client.
    """

    job_id: str  # Job identifier
    status: str  # JobStatus value
    datacenters: list["JobProgress"] = field(default_factory=list)
    total_completed: int = 0  # Global total completed
    total_failed: int = 0  # Global total failed
    overall_rate: float = 0.0  # Global aggregate rate
    elapsed_seconds: float = 0.0  # Time since submission
    completed_datacenters: int = 0  # DCs finished
    failed_datacenters: int = 0  # DCs failed
    errors: list[str] = field(default_factory=list)
    resolution_details: str = ""
    timestamp: float = 0.0  # Monotonic time when job was submitted
    fence_token: int = 0


@dataclass(slots=True)
class JobLeadershipAnnouncement(Message):
    """
    Announcement of job leadership to peer managers.

    When a manager accepts a job, it broadcasts this to all peer managers
    so they know who the job leader is. This enables:
    - Proper routing of workflow results to job leader
    - Correct forwarding of context updates
    - Job state consistency across the manager cluster
    - Workflow query support (non-leaders can report job status)
    """

    job_id: str  # Job being led
    leader_id: str  # Node ID of the job leader
    # Host/port can be provided as separate fields or as tuple
    leader_host: str = ""  # Host of the job leader
    leader_tcp_port: int = 0  # TCP port of the job leader
    term: int = 0  # Cluster term when job was accepted
    workflow_count: int = 0  # Number of workflows in job
    timestamp: float = 0.0  # When job was accepted
    # Workflow names for query support (non-leaders can track job contents)
    workflow_names: list[str] = field(default_factory=list)
    # Alternative form: address as tuple and target_dc_count
    leader_addr: tuple[str, int] | None = None
    target_dc_count: int = 0
    fence_token: int = 0

    def __post_init__(self) -> None:
        """Handle leader_addr alias for leader_host/leader_tcp_port."""
        if self.leader_addr is not None:
            object.__setattr__(self, "leader_host", self.leader_addr[0])
            object.__setattr__(self, "leader_tcp_port", self.leader_addr[1])
        if self.target_dc_count > 0 and self.term == 0:
            object.__setattr__(self, "term", self.target_dc_count)


@dataclass(slots=True)
class JobLeadershipAck(Message):
    """
    Acknowledgment of job leadership announcement.
    """

    job_id: str  # Job being acknowledged
    accepted: bool  # Whether announcement was accepted
    responder_id: str  # Node ID of responder
    error: str | None = None  # Error message if not accepted


@dataclass(slots=True)
class JobLeadershipNotification(Message):
    """
    Notification of job leadership to peer gates.

    When a gate takes ownership of a job, it notifies peers so they
    can route results and requests correctly.
    """

    job_id: str  # Job identifier
    leader_gate_id: str  # Node ID of the gate that owns the job
    leader_addr: tuple[str, int]  # TCP address of the leader gate
    fence_token: int = 0  # Fencing token for consistency


@dataclass(slots=True)
class JobStateSyncMessage(Message):
    """
    Periodic job state sync from job leader to peer managers.

    Sent every MANAGER_PEER_SYNC_INTERVAL seconds to ensure peer managers
    have up-to-date job state for faster failover recovery. Contains summary
    info that allows non-leaders to serve read queries and prepare for takeover.

    This supplements SWIM heartbeat embedding (which has limited capacity)
    with richer job metadata.
    """

    leader_id: str
    job_id: str
    status: str
    fencing_token: int
    workflows_total: int
    workflows_completed: int
    workflows_failed: int
    workflow_statuses: dict[str, str] = field(default_factory=dict)
    elapsed_seconds: float = 0.0
    timestamp: float = 0.0
    origin_gate_addr: tuple[str, int] | None = None
    context_snapshot: dict[str, dict[str, Any]] = field(default_factory=dict)
    layer_version: int = 0


@dataclass(slots=True)
class JobStateSyncAck(Message):
    """
    Acknowledgment of job state sync.
    """

    job_id: str  # Job being acknowledged
    responder_id: str  # Node ID of responder
    accepted: bool = True  # Whether sync was applied


@dataclass(slots=True)
class JobLeaderGateTransfer(Message):
    """
    Notification that job leadership has transferred to a new gate.

    Sent from the new job leader gate to all managers in relevant DCs
    when gate failure triggers job ownership transfer. Managers update
    their origin_gate_addr to route results to the new leader.

    This is part of Direct DC-to-Job-Leader Routing:
    - Gate-A fails while owning job-123
    - Gate-B takes over via consistent hashing
    - Gate-B sends JobLeaderGateTransfer to managers
    - Managers update _job_origin_gates[job-123] = Gate-B address
    """

    job_id: str  # Job being transferred
    new_gate_id: str  # Node ID of new job leader gate
    new_gate_addr: tuple[str, int]  # TCP address of new leader gate
    fence_token: int  # Incremented fence token for consistency
    old_gate_id: str | None = None  # Node ID of old leader gate (if known)


@dataclass(slots=True)
class JobLeaderGateTransferAck(Message):
    """
    Acknowledgment of job leader gate transfer.
    """

    job_id: str  # Job being acknowledged
    manager_id: str  # Node ID of responding manager
    accepted: bool = True  # Whether transfer was applied


@dataclass(slots=True)
class JobLeaderManagerTransfer(Message):
    """
    Notification that job leadership has transferred to a new manager (AD-31).

    Sent from the new job leader manager to the origin gate when manager
    failure triggers job ownership transfer within a datacenter. Gate updates
    its _job_dc_managers mapping to route requests to the new leader manager.

    Flow:
    - Manager-A (job leader in DC) fails
    - Manager-B (cluster leader) takes over job leadership
    - Manager-B sends JobLeaderManagerTransfer to origin gate
    - Gate updates _job_dc_managers[job_id][dc_id] = Manager-B address
    """

    job_id: str  # Job being transferred
    datacenter_id: str  # DC where leadership changed
    new_manager_id: str  # Node ID of new job leader manager
    new_manager_addr: tuple[str, int]  # TCP address of new leader manager
    fence_token: int  # Incremented fence token for consistency
    old_manager_id: str | None = None  # Node ID of old leader manager (if known)


@dataclass(slots=True)
class JobLeaderManagerTransferAck(Message):
    """
    Acknowledgment of job leader manager transfer.
    """

    job_id: str  # Job being acknowledged
    gate_id: str  # Node ID of responding gate
    accepted: bool = True  # Whether transfer was applied


@dataclass(slots=True)
class JobLeaderWorkerTransfer(Message):
    """
    Notification to workers that job leadership has transferred (AD-31).

    Sent from the new job leader manager to workers with active workflows
    for the job. Workers update their _workflow_job_leader mapping to route
    progress updates to the new manager.

    Flow:
    - Manager-A (job leader) fails
    - Manager-B takes over job leadership
    - Manager-B sends JobLeaderWorkerTransfer to workers with active sub-workflows
    - Workers update _workflow_job_leader for affected workflows
    """

    job_id: str  # Job whose leadership transferred
    workflow_ids: list[str]  # Workflow IDs affected (worker's active workflows)
    new_manager_id: str  # Node ID of new job leader manager
    new_manager_addr: tuple[str, int]  # TCP address of new leader manager
    fence_token: int  # Fencing token for consistency
    old_manager_id: str | None = None  # Node ID of old leader manager (if known)


@dataclass(slots=True)
class JobLeaderWorkerTransferAck(Message):
    """
    Acknowledgment of job leader worker transfer notification (Section 8.4).

    Sent from worker to new job leader manager after processing transfer.
    Contains workflow state information so the new leader can verify all workers acknowledged.
    """

    job_id: str  # Job being acknowledged
    worker_id: str  # Node ID of responding worker
    workflows_updated: int  # Number of workflow routings updated
    accepted: bool = True  # Whether transfer was applied
    rejection_reason: str = ""  # Reason if rejected (8.2)
    fence_token_received: int = 0  # The fence token from the transfer (8.4)
    workflow_states: dict[str, str] = field(
        default_factory=dict
    )  # workflow_id -> status (8.4)


@dataclass(slots=True)
class PendingTransfer:
    """
    Tracks a transfer that arrived before the job/workflow was known (Section 8.3).

    This handles the edge case where a transfer notification arrives
    before the original workflow dispatch.
    """

    job_id: str
    workflow_ids: list[str]
    new_manager_id: str
    new_manager_addr: tuple[str, int]
    fence_token: int
    old_manager_id: str | None
    received_at: float


# =============================================================================
# Section 9: Client Leadership Tracking Models
# =============================================================================


@dataclass(slots=True)
class GateLeaderInfo:
    """
    Information about a gate acting as job leader for a specific job (Section 9.1.1).

    Used by clients to track which gate is the authoritative source
    for a job's status and control operations.
    """

    gate_addr: tuple[str, int]  # (host, port) of the gate
    fence_token: int  # Fencing token for ordering
    last_updated: float  # time.monotonic() when last updated


@dataclass(slots=True)
class ManagerLeaderInfo:
    """
    Information about a manager acting as job leader (Section 9.2.1).

    Tracks manager leadership per datacenter for multi-DC deployments.
    """

    manager_addr: tuple[str, int]  # (host, port) of the manager
    fence_token: int  # Fencing token for ordering
    datacenter_id: str  # Which datacenter this manager serves
    last_updated: float  # time.monotonic() when last updated


@dataclass(slots=True)
class OrphanedJobInfo:
    """
    Information about a job whose leaders are unknown/failed (Section 9.5.1).

    Tracks jobs in orphan state pending either leader discovery or timeout.
    """

    job_id: str
    orphan_timestamp: float  # When job became orphaned
    last_known_gate: tuple[str, int] | None
    last_known_manager: tuple[str, int] | None
    datacenter_id: str = ""


@dataclass(slots=True)
class LeadershipRetryPolicy:
    """
    Configurable retry behavior for leadership changes (Section 9.3.3).

    Controls how clients retry operations when leadership changes occur.
    """

    max_retries: int = 3
    retry_delay: float = 0.5
    exponential_backoff: bool = True
    max_delay: float = 5.0


@dataclass(slots=True)
class GateJobLeaderTransfer(Message):
    """
    Notification to client that gate job leadership has transferred (Section 9.1.2).

    Sent from new gate leader to client when taking over job leadership.
    """

    job_id: str
    new_gate_id: str
    new_gate_addr: tuple[str, int]
    fence_token: int
    old_gate_id: str | None = None
    old_gate_addr: tuple[str, int] | None = None


@dataclass(slots=True)
class GateJobLeaderTransferAck(Message):
    """
    Acknowledgment of gate job leader transfer notification.
    """

    job_id: str
    client_id: str
    accepted: bool = True
    rejection_reason: str = ""


@dataclass(slots=True)
class ManagerJobLeaderTransfer(Message):
    """
    Notification to client that manager job leadership has transferred (Section 9.2.2).

    Typically forwarded by gate to client when a manager job leader changes.
    """

    job_id: str
    new_manager_id: str
    new_manager_addr: tuple[str, int]
    fence_token: int
    datacenter_id: str
    old_manager_id: str | None = None
    old_manager_addr: tuple[str, int] | None = None


@dataclass(slots=True)
class ManagerJobLeaderTransferAck(Message):
    """
    Acknowledgment of manager job leader transfer notification.
    """

    job_id: str
    client_id: str
    datacenter_id: str
    accepted: bool = True
    rejection_reason: str = ""


# =============================================================================
# Client Push Notifications
# =============================================================================


@dataclass(slots=True)
class JobStatusPush(Message):
    """
    Push notification for job status changes.

    Sent from Gate/Manager to Client when significant status changes occur.
    This is a Tier 1 (immediate) notification for:
    - Job started
    - Job completed
    - Job failed
    - Datacenter completion

    Includes both aggregated totals AND per-DC breakdown for visibility.
    """

    job_id: str  # Job identifier
    status: str  # JobStatus value
    message: str  # Human-readable status message
    total_completed: int = 0  # Completed count (aggregated across all DCs)
    total_failed: int = 0  # Failed count (aggregated across all DCs)
    overall_rate: float = 0.0  # Current rate (aggregated across all DCs)
    elapsed_seconds: float = 0.0  # Time since submission
    is_final: bool = False  # True if job is complete (no more updates)
    # Per-datacenter breakdown (for clients that want granular visibility)
    per_dc_stats: list["DCStats"] = field(default_factory=list)
    fence_token: int = 0  # Fencing token for at-most-once semantics


@dataclass(slots=True)
class DCStats(Message):
    """
    Per-datacenter statistics for real-time status updates.

    Used in JobStatusPush to provide per-DC visibility without
    the full detail of JobProgress (which includes workflow-level stats).
    """

    datacenter: str  # Datacenter identifier
    status: str  # DC-specific status
    completed: int = 0  # Completed in this DC
    failed: int = 0  # Failed in this DC
    rate: float = 0.0  # Rate in this DC


@dataclass(slots=True)
class JobBatchPush(Message):
    """
    Batched statistics push notification.

    Sent periodically (Tier 2) with aggregated progress data.
    Contains step-level statistics and detailed progress.
    Includes per-DC breakdown for granular visibility.
    """

    job_id: str  # Job identifier
    status: str  # Current JobStatus
    step_stats: list["StepStats"] = field(default_factory=list)
    total_completed: int = 0  # Aggregated across all DCs
    total_failed: int = 0  # Aggregated across all DCs
    overall_rate: float = 0.0  # Aggregated across all DCs
    elapsed_seconds: float = 0.0
    # Per-datacenter breakdown (for clients that want granular visibility)
    per_dc_stats: list["DCStats"] = field(default_factory=list)


@dataclass(slots=True)
class RegisterCallback(Message):
    """
    Client request to register for push notifications for a job.

    Used for client reconnection after disconnect. Client sends this
    to the job owner gate/manager to re-subscribe to status updates.

    Part of Client Reconnection (Component 5):
    1. Client disconnects from Gate-A
    2. Client reconnects and sends RegisterCallback(job_id=X)
    3. Gate/Manager adds callback_addr to job's notification list
    4. Client receives remaining status updates
    """

    job_id: str  # Job to register callback for
    callback_addr: tuple[str, int]  # Client's TCP address for push notifications
    last_sequence: int = 0


@dataclass(slots=True)
class RegisterCallbackResponse(Message):
    """
    Response to RegisterCallback request.

    Indicates whether callback registration succeeded and provides
    current job status for immediate sync.
    """

    job_id: str  # Job being registered
    success: bool  # Whether registration succeeded
    status: str = ""  # Current JobStatus value
    total_completed: int = 0  # Current completion count
    total_failed: int = 0  # Current failure count
    elapsed_seconds: float = 0.0  # Time since job started
    error: str | None = None  # Error message if failed


@dataclass(slots=True)
class JobUpdateRecord(Message):
    """
    Record of a client update for replay/polling.
    """

    sequence: int
    message_type: str
    payload: bytes
    timestamp: float


@dataclass(slots=True)
class JobUpdatePollRequest(Message):
    """
    Request for job updates since a sequence.
    """

    job_id: str
    last_sequence: int = 0


@dataclass(slots=True)
class JobUpdatePollResponse(Message):
    """
    Response containing queued job updates for a client.
    """

    job_id: str
    updates: list["JobUpdateRecord"] = field(default_factory=list)
    latest_sequence: int = 0
    truncated: bool = False
    oldest_sequence: int = 0


@dataclass(slots=True)
class ReporterResultPush(Message):
    """
    Push notification for reporter submission result.

    Sent from Manager/Gate to Client after submitting results to a reporter.
    Each reporter config generates one notification (success or failure).

    This is sent as a background task completes, not batched.
    Clients can track which reporters succeeded or failed for a job.
    """

    job_id: str  # Job the results were for
    reporter_type: str  # ReporterTypes enum value (e.g., "json", "datadog")
    success: bool  # Whether submission succeeded
    error: str | None = None  # Error message if failed
    elapsed_seconds: float = 0.0  # Time taken for submission
    # Source information for multi-DC scenarios
    source: str = ""  # "manager" or "gate"
    datacenter: str = ""  # Datacenter that submitted (manager only)


@dataclass(slots=True)
class RateLimitResponse(Message):
    """
    Response indicating rate limit exceeded.

    Returned when a client exceeds their request rate limit.
    Client should wait retry_after_seconds before retrying.

    Protocol:
    1. Client sends request via TCP
    2. Server checks rate limit for client_id (from addr) + operation
    3. If exceeded, returns RateLimitResponse with retry_after
    4. Client waits and retries (using CooperativeRateLimiter)

    Integration:
    - Gate: Rate limits job_submit, job_status, cancel, workflow_query
    - Manager: Rate limits workflow_dispatch, provision requests
    - Both use ServerRateLimiter with per-client token buckets
    """

    operation: str  # Operation that was rate limited
    retry_after_seconds: float  # Seconds to wait before retry
    error: str = "Rate limit exceeded"  # Error message
    tokens_remaining: float = 0.0  # Remaining tokens (for debugging)


# =============================================================================
# Job Timeout Messages (AD-34)
# =============================================================================


@dataclass(slots=True)
class JobProgressReport(Message):
    """
    Manager → Gate: Periodic progress report (AD-34 multi-DC coordination).

    Sent every ~10 seconds during job execution to keep gate informed of
    DC-local progress. Used by gate to detect global timeouts and stuck DCs.

    Extension Integration (AD-26):
    - total_extensions_granted: Total seconds of extensions granted in this DC
    - max_worker_extension: Largest extension granted to any single worker
    - workers_with_extensions: Count of workers currently with active extensions
    """

    job_id: str
    datacenter: str
    manager_id: str
    manager_host: str  # For gate to send replies
    manager_port: int
    workflows_total: int
    workflows_completed: int
    workflows_failed: int
    has_recent_progress: bool  # Any workflow progressed in last 10s
    timestamp: float
    fence_token: int  # Manager's fence token

    # Extension tracking (AD-26 integration)
    total_extensions_granted: float = 0.0  # Total seconds granted to workers
    max_worker_extension: float = 0.0  # Largest extension granted
    workers_with_extensions: int = 0  # Count of workers with active extensions


@dataclass(slots=True)
class JobTimeoutReport(Message):
    """
    Manager → Gate: DC-local timeout detected (AD-34 multi-DC coordination).

    Sent when manager detects job timeout or stuck workflows in its datacenter.
    Gate aggregates timeout reports from all DCs to declare global timeout.

    Manager sends this but does NOT mark job failed locally - waits for gate's
    global timeout decision (JobGlobalTimeout).
    """

    job_id: str
    datacenter: str
    manager_id: str
    manager_host: str
    manager_port: int
    reason: str  # "timeout" | "stuck" | other descriptive reason
    elapsed_seconds: float
    fence_token: int


@dataclass(slots=True)
class JobGlobalTimeout(Message):
    """
    Gate → Manager: Global timeout declared (AD-34 multi-DC coordination).

    Gate has determined the job is globally timed out (based on timeout reports
    from DCs, overall timeout exceeded, or all DCs stuck). Manager must cancel
    job locally and mark as timed out.

    Fence token validation prevents stale timeout decisions after leader transfers.
    """

    job_id: str
    reason: str  # Why gate timed out the job
    timed_out_at: float  # Gate's timestamp
    fence_token: int  # Gate's fence token for this decision


@dataclass(slots=True)
class JobLeaderTransfer(Message):
    """
    Manager → Gate: Notify gate of leader change (AD-34 multi-DC coordination).

    Sent by new leader after taking over job leadership. Gate updates its
    tracking to send future timeout decisions to the new leader.

    Includes incremented fence token to prevent stale operations.
    """

    job_id: str
    datacenter: str
    new_leader_id: str
    new_leader_host: str
    new_leader_port: int
    fence_token: int  # New leader's fence token


@dataclass(slots=True)
class JobFinalStatus(Message):
    """
    Manager → Gate: Final job status for cleanup (AD-34 lifecycle management).

    Sent when job reaches terminal state (completed/failed/cancelled/timed out).
    Gate uses this to clean up timeout tracking for the job.

    When all DCs report terminal status, gate removes job from tracking to
    prevent memory leaks.
    """

    job_id: str
    datacenter: str
    manager_id: str
    status: str  # JobStatus.COMPLETED/FAILED/CANCELLED/TIMEOUT value
    timestamp: float
    fence_token: int


# =============================================================================
# State Synchronization
# =============================================================================


@dataclass(slots=True)
class WorkerStateSnapshot(Message):
    """
    Complete state snapshot from a worker.

    Used for state sync when a new manager becomes leader.
    """

    node_id: str  # Worker identifier
    state: str  # WorkerState value
    total_cores: int  # Total cores
    available_cores: int  # Free cores
    version: int  # State version
    # Host/port for registration reconstruction during state sync
    host: str = ""
    tcp_port: int = 0
    udp_port: int = 0
    active_workflows: dict[str, "WorkflowProgress"] = field(default_factory=dict)


@dataclass(slots=True)
class ManagerStateSnapshot(Message):
    """
    Complete state snapshot from a manager.

    Used for state sync between managers.
    """

    node_id: str  # Manager identifier
    datacenter: str  # Datacenter
    is_leader: bool  # Leadership status
    term: int  # Current term
    version: int  # State version
    workers: list["WorkerStateSnapshot"] = field(default_factory=list)
    jobs: dict[str, "JobProgress"] = field(default_factory=dict)
    # Context consistency protocol state
    job_leaders: dict[str, str] = field(
        default_factory=dict
    )  # job_id -> leader_node_id
    job_leader_addrs: dict[str, tuple[str, int]] = field(
        default_factory=dict
    )  # job_id -> (host, tcp_port)
    job_fence_tokens: dict[str, int] = field(default_factory=dict)
    job_layer_versions: dict[str, int] = field(
        default_factory=dict
    )  # job_id -> layer version
    job_contexts: bytes = b""  # Serialized contexts (cloudpickle)


@dataclass(slots=True)
class GateStateSnapshot(Message):
    """
    Complete state snapshot from a gate.

    Used for state sync between gates when a new leader is elected.
    Contains global job state and datacenter status.
    """

    node_id: str  # Gate identifier
    is_leader: bool  # Leadership status
    term: int  # Current term
    version: int  # State version
    jobs: dict[str, "GlobalJobStatus"] = field(default_factory=dict)
    datacenter_status: dict[str, "DatacenterStatus"] = field(default_factory=dict)
    leases: dict[str, "DatacenterLease"] = field(default_factory=dict)
    # Manager discovery - shared between gates
    datacenter_managers: dict[str, list[tuple[str, int]]] = field(default_factory=dict)
    datacenter_manager_udp: dict[str, list[tuple[str, int]]] = field(
        default_factory=dict
    )
    # Per-job leadership tracking (independent of SWIM cluster leadership)
    job_leaders: dict[str, str] = field(
        default_factory=dict
    )  # job_id -> leader_node_id
    job_leader_addrs: dict[str, tuple[str, int]] = field(
        default_factory=dict
    )  # job_id -> (host, tcp_port)
    job_fencing_tokens: dict[str, int] = field(
        default_factory=dict
    )  # job_id -> fencing token (for leadership consistency)
    # Per-job per-DC manager leader tracking (which manager accepted each job in each DC)
    job_dc_managers: dict[str, dict[str, tuple[str, int]]] = field(
        default_factory=dict
    )  # job_id -> {dc_id -> (host, port)}
    workflow_dc_results: dict[str, dict[str, dict[str, "WorkflowResultPush"]]] = field(
        default_factory=dict
    )
    job_submissions: dict[str, "JobSubmission"] = field(default_factory=dict)
    progress_callbacks: dict[str, tuple[str, int]] = field(default_factory=dict)


@dataclass(slots=True)
class StateSyncRequest(Message):
    """
    Request for state synchronization.

    Sent by new leader to gather current state.
    """

    requester_id: str  # Requesting node
    requester_role: str  # NodeRole value
    cluster_id: str = "hyperscale"  # Cluster identifier for isolation
    environment_id: str = "default"  # Environment identifier for isolation
    since_version: int = 0  # Only send updates after this version


@dataclass(slots=True)
class StateSyncResponse(Message):
    """
    Response to state sync request.

    The responder_ready field indicates whether the responder has completed
    its own startup and is ready to serve authoritative state. If False,
    the requester should retry after a delay.
    """

    responder_id: str  # Responding node
    current_version: int  # Current state version
    responder_ready: bool = True  # Whether responder has completed startup
    # One of these will be set based on node type
    worker_state: "WorkerStateSnapshot | None" = None
    manager_state: "ManagerStateSnapshot | None" = None
    gate_state: "GateStateSnapshot | None" = None


@dataclass(slots=True)
class GateStateSyncRequest(Message):
    """
    Request for gate-to-gate state synchronization.

    Sent when a gate needs to sync state with a peer gate.
    """

    requester_id: str  # Requesting gate node ID
    known_version: int = 0  # Last known state version


@dataclass(slots=True)
class GateStateSyncResponse(Message):
    """
    Response to gate state sync request.
    """

    responder_id: str  # Responding gate node ID
    is_leader: bool  # Whether responder is the SWIM cluster leader
    term: int  # Current leadership term
    state_version: int  # Current state version
    snapshot: "GateStateSnapshot | None" = None  # Full state snapshot
    error: str | None = None  # Error message if sync failed


# =============================================================================
# Context Synchronization (Layer-Boundary Sync Protocol)
# =============================================================================


@dataclass(slots=True)
class ContextForward(Message):
    """
    Non-leader manager forwards context updates to job leader.

    When a worker sends WorkflowFinalResult to a manager that is NOT the
    job leader, that manager forwards the context portion to the job leader.
    Only the job leader applies context updates (single-writer model).
    """

    job_id: str  # Job identifier
    workflow_id: str  # Source workflow
    context_updates: bytes  # Serialized Dict[key, value]
    context_timestamps: bytes  # Serialized Dict[key, lamport_clock]
    source_manager: str  # Manager node_id that received from worker


@dataclass(slots=True)
class ContextLayerSync(Message):
    """
    Job leader broadcasts at layer completion to sync context to peers.

    Before dispatching layer N+1, the job leader must:
    1. Create a versioned snapshot of context after layer N
    2. Broadcast to all peer managers
    3. Wait for quorum confirmation
    4. Only then dispatch next layer workflows

    This ensures dependent workflows always see correct context.
    """

    job_id: str  # Job identifier
    layer_version: int  # Monotonically increasing per job
    context_snapshot: bytes  # Full context as cloudpickle.dumps(context.dict())
    source_node_id: str  # Job leader's node_id


@dataclass(slots=True)
class ContextLayerSyncAck(Message):
    """
    Peer manager confirms receipt of context layer sync.

    Job leader waits for quorum of these before advancing to next layer.
    """

    job_id: str  # Job identifier
    layer_version: int  # Echoed back for correlation
    applied: bool  # True if applied, False if stale/rejected
    responder_id: str  # Responding manager's node_id


# =============================================================================
# Quorum and Confirmation
# =============================================================================


@dataclass(slots=True)
class ProvisionRequest(Message):
    """
    Request to provision a workflow across the cluster.

    Sent from leader manager to all managers for quorum confirmation.
    """

    job_id: str  # Job identifier
    workflow_id: str  # Workflow to provision
    target_worker: str  # Selected worker node_id
    cores_required: int  # Cores needed
    fence_token: int  # Fencing token
    version: int  # State version for this decision


@dataclass(slots=True)
class ProvisionConfirm(Message):
    """
    Confirmation of provision request.

    Manager acknowledges the provisioning decision.
    """

    job_id: str  # Job identifier
    workflow_id: str  # Workflow
    confirming_node: str  # Node confirming
    confirmed: bool  # Whether confirmed
    version: int  # Node's current version
    error: str | None = None  # Error if not confirmed


@dataclass(slots=True)
class ProvisionCommit(Message):
    """
    Commit message after quorum achieved.

    Tells all managers the provisioning is final.
    """

    job_id: str  # Job identifier
    workflow_id: str  # Workflow
    target_worker: str  # Worker receiving the workflow
    cores_assigned: int  # Cores allocated
    fence_token: int  # Fencing token
    committed_version: int  # Version at commit time


# =============================================================================
# Cancellation
# =============================================================================


@dataclass(slots=True)
class CancelJob(Message):
    """
    Request to cancel a job.

    Flows: client -> gate -> manager -> worker
           or: client -> manager -> worker
    """

    job_id: str  # Job to cancel
    reason: str = ""  # Cancellation reason
    fence_token: int = 0  # Fencing token for validation


@dataclass(slots=True)
class CancelAck(Message):
    """
    Acknowledgment of cancellation.
    """

    job_id: str  # Job identifier
    cancelled: bool  # Whether successfully cancelled
    workflows_cancelled: int = 0  # Number of workflows stopped
    error: str | None = None  # Error if cancellation failed


@dataclass(slots=True)
class WorkflowCancellationQuery(Message):
    """
    Query for workflow cancellation status.

    Sent from manager to worker to poll for cancellation progress.
    """

    job_id: str
    workflow_id: str


@dataclass(slots=True)
class WorkflowCancellationResponse(Message):
    """
    Response to workflow cancellation query.

    Contains the current cancellation status for a workflow.
    """

    job_id: str
    workflow_id: str
    workflow_name: str
    status: str  # WorkflowCancellationStatus value
    error: str | None = None


# =============================================================================
# Lease Management (for Gates)
# =============================================================================


@dataclass(slots=True)
class DatacenterLease(Message):
    """
    Lease for job execution in a datacenter.

    Used by gates for at-most-once semantics across DCs.
    """

    job_id: str  # Job identifier
    datacenter: str  # Datacenter holding lease
    lease_holder: str  # Gate node_id holding lease
    fence_token: int  # Fencing token
    expires_at: float  # Monotonic expiration time
    version: int  # Lease version


@dataclass(slots=True)
class LeaseTransfer(Message):
    """
    Transfer a lease to another gate (during scaling).
    """

    job_id: str  # Job identifier
    datacenter: str  # Datacenter
    from_gate: str  # Current holder
    to_gate: str  # New holder
    new_fence_token: int  # New fencing token
    version: int  # Transfer version


@dataclass(slots=True)
class LeaseTransferAck(Message):
    """
    Acknowledgment of a lease transfer.
    """

    job_id: str  # Job identifier
    accepted: bool  # Whether transfer was accepted
    new_fence_token: int = 0  # New fencing token if accepted
    error: str | None = None  # Error message if rejected


# =============================================================================
# Datacenter Health & Routing
# =============================================================================


@dataclass(slots=True, kw_only=True)
class DatacenterStatus(Message):
    """
    Status of a datacenter for routing decisions.

    Used by gates to classify datacenter health and make
    intelligent routing decisions with fallback support.

    See AD-16 in docs/architecture.md for design rationale.
    """

    dc_id: str
    health: str
    available_capacity: int = 0
    queue_depth: int = 0
    manager_count: int = 0
    worker_count: int = 0
    last_update: float = 0.0
    overloaded_worker_count: int = 0
    stressed_worker_count: int = 0
    busy_worker_count: int = 0
    worker_overload_ratio: float = 0.0
    health_severity_weight: float = 1.0
    overloaded_manager_count: int = 0
    stressed_manager_count: int = 0
    busy_manager_count: int = 0
    manager_overload_ratio: float = 0.0
    leader_overloaded: bool = False


# =============================================================================
# Ping/Health Check Messages
# =============================================================================


@dataclass(slots=True)
class PingRequest(Message):
    """
    Ping request from client to manager or gate.

    Used for health checking and status retrieval without
    submitting a job. Returns current node state.
    """

    request_id: str  # Unique request identifier


@dataclass(slots=True, kw_only=True)
class WorkerStatus(Message):
    """
    Status of a single worker as seen by a manager.

    Used for:
    1. Wire protocol: ManagerPingResponse reports per-worker health
    2. Internal tracking: Manager's WorkerPool tracks worker state

    The registration/heartbeat/last_seen/reserved_cores fields are
    optional and only used for internal manager tracking (not serialized
    for wire protocol responses).

    Properties provide compatibility aliases (node_id -> worker_id, health -> state).
    """

    worker_id: str  # Worker's node_id
    state: str  # WorkerState value (as string for wire)
    available_cores: int = 0  # Currently available cores
    total_cores: int = 0  # Total cores on worker
    queue_depth: int = 0  # Pending workflows
    cpu_percent: float = 0.0  # CPU utilization
    memory_percent: float = 0.0  # Memory utilization
    registration: "WorkerRegistration | None" = None
    heartbeat: "WorkerHeartbeat | None" = None
    last_seen: float = 0.0
    reserved_cores: int = 0
    is_remote: bool = False
    owner_manager_id: str = ""
    overload_state: str = "healthy"  # AD-17: healthy|busy|stressed|overloaded

    @property
    def node_id(self) -> str:
        """Alias for worker_id (internal use)."""
        return self.worker_id

    @property
    def health(self) -> WorkerState:
        """Get state as WorkerState enum (internal use)."""
        try:
            return WorkerState(self.state)
        except ValueError:
            return WorkerState.OFFLINE

    @health.setter
    def health(self, value: WorkerState) -> None:
        """Set state from WorkerState enum (internal use)."""
        object.__setattr__(self, "state", value.value)

    @property
    def short_id(self) -> str:
        """Get short form of node ID for display."""
        return self.worker_id[:12] if len(self.worker_id) > 12 else self.worker_id


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


# =============================================================================
# Datacenter Query Messages
# =============================================================================


@dataclass(slots=True)
class DatacenterListRequest(Message):
    """
    Request to list registered datacenters from a gate.

    Clients use this to discover available datacenters before submitting jobs.
    This is a lightweight query that returns datacenter identifiers and health status.
    """

    request_id: str = ""  # Optional request identifier for correlation


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


# =============================================================================
# Workflow Query Messages
# =============================================================================


@dataclass(slots=True, kw_only=True)
class WorkflowQueryRequest(Message):
    """
    Request to query workflow status by name.

    Client sends this to managers or gates to get status of specific
    workflows. Unknown workflow names are silently ignored.
    """

    request_id: str  # Unique request identifier
    workflow_names: list[str]  # Workflow class names to query
    job_id: str | None = None  # Optional: filter to specific job


@dataclass(slots=True, kw_only=True)
class WorkflowStatusInfo(Message):
    """
    Status information for a single workflow.

    Returned as part of WorkflowQueryResponse.
    """

    workflow_name: str  # Workflow class name
    workflow_id: str  # Unique workflow instance ID
    job_id: str  # Parent job ID
    status: str  # WorkflowStatus value
    # Provisioning info
    provisioned_cores: int = 0  # Cores allocated to this workflow
    vus: int = 0  # Virtual users (from workflow config)
    # Progress info
    completed_count: int = 0  # Actions completed
    failed_count: int = 0  # Actions failed
    rate_per_second: float = 0.0  # Current execution rate
    elapsed_seconds: float = 0.0  # Time since start
    # Queue info
    is_enqueued: bool = False  # True if waiting for cores
    queue_position: int = 0  # Position in queue (0 if not queued)
    # Worker assignment
    assigned_workers: list[str] = field(default_factory=list)  # Worker IDs


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


@dataclass(slots=True, kw_only=True)
class DatacenterWorkflowStatus(Message):
    """
    Workflow status for a single datacenter.

    Used in GateWorkflowQueryResponse to group results by DC.
    """

    dc_id: str  # Datacenter identifier
    workflows: list[WorkflowStatusInfo] = field(default_factory=list)


@dataclass(slots=True, kw_only=True)
class GateWorkflowQueryResponse(Message):
    """
    Response to workflow query from a gate.

    Contains status grouped by datacenter.
    """

    request_id: str  # Echoed from request
    gate_id: str  # Responding gate's node_id
    datacenters: list[DatacenterWorkflowStatus] = field(default_factory=list)


@dataclass(slots=True)
class EagerWorkflowEntry:
    """
    Tracking entry for a workflow pending eager dispatch.

    Contains all information needed to dispatch the workflow once
    its dependencies are met and cores are available.
    """

    job_id: str  # Parent job ID
    workflow_name: str  # Workflow name (graph node)
    workflow_idx: int  # Index in job's workflow list
    workflow: Any  # The workflow instance
    vus: int  # Virtual users for this workflow
    priority: Any  # Workflow priority (StagePriority enum)
    is_test: bool  # Whether this is a test workflow
    dependencies: set[str]  # Set of workflow names this depends on
    completed_dependencies: set[str] = field(
        default_factory=set
    )  # Dependencies that have completed
    dispatched: bool = False  # Whether this workflow has been dispatched


# =============================================================================
# Datacenter Registration State (Gate-side tracking)
# =============================================================================


@dataclass(slots=True)
class ManagerRegistrationState:
    """
    Per-manager registration state tracked by a Gate.

    Tracks when each manager registered and heartbeat patterns for
    adaptive staleness detection. Generation IDs handle manager restarts.
    """

    manager_addr: tuple[str, int]  # (host, tcp_port)
    node_id: str | None = None  # Manager's node_id (from first heartbeat)
    generation: int = 0  # Increments on manager restart (from heartbeat)

    # Timing
    first_seen_at: float = 0.0  # monotonic time of first heartbeat
    last_heartbeat_at: float = 0.0  # monotonic time of most recent heartbeat

    # Heartbeat interval tracking (for adaptive staleness)
    heartbeat_count: int = 0  # Total heartbeats received
    avg_heartbeat_interval: float = 5.0  # Running average interval (seconds)

    @property
    def is_registered(self) -> bool:
        """Manager has sent at least one heartbeat."""
        return self.first_seen_at > 0

    def is_stale(self, now: float, staleness_multiplier: float = 3.0) -> bool:
        """
        Check if manager is stale based on adaptive interval.

        A manager is stale if no heartbeat received for staleness_multiplier
        times the average heartbeat interval.
        """
        if not self.is_registered:
            return False
        expected_interval = max(self.avg_heartbeat_interval, 1.0)
        return (now - self.last_heartbeat_at) > (
            staleness_multiplier * expected_interval
        )

    def record_heartbeat(self, now: float, node_id: str, generation: int) -> bool:
        """
        Record a heartbeat from this manager.

        Returns True if this is a new generation (manager restarted).
        """
        is_new_generation = generation > self.generation

        if is_new_generation or not self.is_registered:
            # New registration or restart - reset state
            self.node_id = node_id
            self.generation = generation
            self.first_seen_at = now
            self.heartbeat_count = 1
            self.avg_heartbeat_interval = 5.0  # Reset to default
        else:
            # Update running average of heartbeat interval
            if self.last_heartbeat_at > 0:
                interval = now - self.last_heartbeat_at
                # Exponential moving average (alpha = 0.2)
                self.avg_heartbeat_interval = (
                    0.8 * self.avg_heartbeat_interval + 0.2 * interval
                )
            self.heartbeat_count += 1

        self.last_heartbeat_at = now
        return is_new_generation


@dataclass(slots=True)
class DatacenterRegistrationState:
    """
    Per-datacenter registration state tracked by a Gate.

    Tracks which managers have registered and provides registration status
    based on quorum requirements. Health classification only applies once
    the datacenter is READY.
    """

    dc_id: str  # Datacenter identifier
    configured_managers: list[tuple[str, int]]  # Manager addrs from config

    # Per-manager tracking
    manager_states: dict[tuple[str, int], ManagerRegistrationState] = field(
        default_factory=dict
    )

    # Timing
    first_heartbeat_at: float = 0.0  # When first manager registered (monotonic)
    last_heartbeat_at: float = 0.0  # Most recent heartbeat from any manager (monotonic)

    def get_registration_status(
        self, now: float, staleness_multiplier: float = 3.0
    ) -> DatacenterRegistrationStatus:
        """
        Compute current registration status based on manager heartbeats.

        Uses quorum (majority) of configured managers as the threshold
        for READY status.
        """
        configured_count = len(self.configured_managers)
        if configured_count == 0:
            return DatacenterRegistrationStatus.UNAVAILABLE

        # Count non-stale registered managers
        active_count = sum(
            1
            for state in self.manager_states.values()
            if state.is_registered and not state.is_stale(now, staleness_multiplier)
        )

        quorum = configured_count // 2 + 1

        if active_count == 0:
            if self.first_heartbeat_at == 0:
                # Never received any heartbeats
                return DatacenterRegistrationStatus.AWAITING_INITIAL
            else:
                # Had heartbeats before but all are now stale/lost
                return DatacenterRegistrationStatus.UNAVAILABLE
        elif active_count < quorum:
            if self.first_heartbeat_at == 0 or self._was_ever_ready():
                # Was ready before, now below quorum
                return DatacenterRegistrationStatus.PARTIAL
            else:
                # Still coming up, not yet at quorum
                return DatacenterRegistrationStatus.INITIALIZING
        else:
            # At or above quorum
            return DatacenterRegistrationStatus.READY

    def _was_ever_ready(self) -> bool:
        """Check if this DC ever had quorum (any manager with heartbeat_count > 1)."""
        # If any manager has received multiple heartbeats, we were likely ready before
        return any(state.heartbeat_count > 1 for state in self.manager_states.values())

    def get_active_manager_count(
        self, now: float, staleness_multiplier: float = 3.0
    ) -> int:
        """Get count of non-stale registered managers."""
        return sum(
            1
            for state in self.manager_states.values()
            if state.is_registered and not state.is_stale(now, staleness_multiplier)
        )

    def record_heartbeat(
        self,
        manager_addr: tuple[str, int],
        node_id: str,
        generation: int,
        now: float,
    ) -> bool:
        """
        Record a heartbeat from a manager in this datacenter.

        Returns True if this is a new manager or a manager restart (new generation).
        """
        if manager_addr not in self.manager_states:
            self.manager_states[manager_addr] = ManagerRegistrationState(
                manager_addr=manager_addr,
            )

        is_new = self.manager_states[manager_addr].record_heartbeat(
            now, node_id, generation
        )

        # Update DC-level timing
        if self.first_heartbeat_at == 0:
            self.first_heartbeat_at = now
        self.last_heartbeat_at = now

        return is_new
