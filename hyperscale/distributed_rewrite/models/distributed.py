"""
Distributed system message types for Gate, Manager, and Worker nodes.

These dataclasses define the wire format for all TCP communication
in the distributed Hyperscale architecture.
"""

from dataclasses import dataclass, field
from enum import Enum
from hyperscale.core.graph import Workflow
from hyperscale.core.state import Context
from hyperscale.core.jobs.models import WorkflowResults
from typing import Any
from .message import Message


# =============================================================================
# Enums and Type Definitions
# =============================================================================

class NodeRole(str, Enum):
    """Role of a node in the distributed system."""
    GATE = "gate"
    MANAGER = "manager"
    WORKER = "worker"


class JobStatus(str, Enum):
    """Status of a distributed job."""
    SUBMITTED = "submitted"      # Job received, not yet dispatched
    QUEUED = "queued"            # Queued for execution
    DISPATCHING = "dispatching"  # Being dispatched to workers
    RUNNING = "running"          # Active execution
    COMPLETING = "completing"    # Wrapping up, gathering results
    COMPLETED = "completed"      # Successfully finished
    FAILED = "failed"            # Failed (may be retried)
    CANCELLED = "cancelled"      # User cancelled
    TIMEOUT = "timeout"          # Exceeded time limit


class WorkflowStatus(str, Enum):
    """Status of a single workflow within a job."""
    PENDING = "pending"          # Not yet started
    ASSIGNED = "assigned"        # Assigned/dispatched to worker(s)
    RUNNING = "running"          # Executing
    COMPLETED = "completed"      # Finished successfully
    FAILED = "failed"            # Failed
    CANCELLED = "cancelled"      # Cancelled
    AGGREGATED = "aggregated"    # Results successfully aggregated (internal)
    AGGREGATION_FAILED = "aggregation_failed"  # Aggregation failed (internal)


class WorkerState(str, Enum):
    """State of a worker node."""
    HEALTHY = "healthy"          # Normal operation
    DEGRADED = "degraded"        # High load, accepting with backpressure
    DRAINING = "draining"        # Not accepting new work
    OFFLINE = "offline"          # Not responding


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
    SYNCING = "syncing"          # Joined cluster, syncing state (not in quorum)
    ACTIVE = "active"            # Fully operational (counted in quorum)
    DRAINING = "draining"        # Not accepting new work, draining existing


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
    SYNCING = "syncing"          # Joined cluster, syncing state (not in quorum)
    ACTIVE = "active"            # Fully operational (counted in quorum)
    DRAINING = "draining"        # Not accepting new work, draining existing


class DatacenterHealth(str, Enum):
    """
    Health classification for datacenter routing decisions.
    
    Key insight: BUSY ≠ UNHEALTHY
    - BUSY = transient, will clear when workflows complete → accept job (queued)
    - UNHEALTHY = structural problem, requires intervention → try fallback
    
    See AD-16 in docs/architecture.md for design rationale.
    """
    HEALTHY = "healthy"      # Managers responding, workers available, capacity exists
    BUSY = "busy"            # Managers responding, workers available, no immediate capacity
    DEGRADED = "degraded"    # Some managers responding, reduced capacity
    UNHEALTHY = "unhealthy"  # No managers responding OR all workers down


class UpdateTier(str, Enum):
    """
    Tiered update strategy for cross-DC stat synchronization.
    
    Not all stats need real-time updates. This enum defines the
    urgency/frequency tier for different types of updates.
    
    See AD-15 in docs/architecture.md for design rationale.
    """
    IMMEDIATE = "immediate"   # Event-driven, TCP push - completion, failure, critical
    PERIODIC = "periodic"     # Every 1-5s, TCP batch - progress, aggregate rates
    ON_DEMAND = "on_demand"   # Client request, TCP pull - step stats, historical


# =============================================================================
# Node Identity and Registration
# =============================================================================

@dataclass(slots=True)
class NodeInfo(Message):
    """
    Identity information for any node in the cluster.

    Used for registration, heartbeats, and state sync.
    """
    node_id: str                 # Unique node identifier
    role: str                    # NodeRole value
    host: str                    # Network host
    port: int                    # TCP port
    datacenter: str              # Datacenter identifier
    version: int = 0             # State version (Lamport clock)
    udp_port: int = 0            # UDP port for SWIM (defaults to 0, derived from port if not set)


@dataclass(slots=True)
class ManagerInfo(Message):
    """
    Manager identity and address information for worker discovery.
    
    Workers use this to maintain a list of known managers for
    redundant communication and failover.
    """
    node_id: str                 # Manager's unique identifier
    tcp_host: str                # TCP host for data operations
    tcp_port: int                # TCP port for data operations
    udp_host: str                # UDP host for SWIM healthchecks
    udp_port: int                # UDP port for SWIM healthchecks
    datacenter: str              # Datacenter identifier
    is_leader: bool = False      # Whether this manager is the current leader


@dataclass(slots=True, kw_only=True)
class ManagerPeerRegistration(Message):
    """
    Registration request from one manager to another peer manager.

    When a manager discovers a new peer (via SWIM or seed list),
    it sends this registration to establish the bidirectional relationship.
    """
    node: ManagerInfo            # Registering manager's info
    term: int                    # Current leadership term
    is_leader: bool              # Whether registering manager is leader


@dataclass(slots=True, kw_only=True)
class ManagerPeerRegistrationResponse(Message):
    """
    Registration acknowledgment from manager to peer manager.

    Contains list of all known peer managers so the registering
    manager can discover the full cluster topology.
    """
    accepted: bool                          # Whether registration was accepted
    manager_id: str                         # Responding manager's node_id
    is_leader: bool                         # Whether responding manager is leader
    term: int                               # Responding manager's term
    known_peers: list[ManagerInfo]          # All known peer managers (for discovery)
    error: str | None = None                # Error message if not accepted


@dataclass(slots=True, kw_only=True)
class RegistrationResponse(Message):
    """
    Registration acknowledgment from manager to worker.

    Contains list of all known healthy managers so worker can
    establish redundant communication channels.
    """
    accepted: bool                          # Whether registration was accepted
    manager_id: str                         # Responding manager's node_id
    healthy_managers: list[ManagerInfo]     # All known healthy managers (including self)
    error: str | None = None                # Error message if not accepted


@dataclass(slots=True, kw_only=True)
class ManagerToWorkerRegistration(Message):
    """
    Registration request from manager to worker.

    Enables bidirectional registration: workers register with managers,
    AND managers can register with workers discovered via state sync.
    This speeds up cluster formation by allowing managers to proactively
    reach out to workers they learn about from peer managers.
    """
    manager: ManagerInfo                    # Registering manager's info
    is_leader: bool                         # Whether this manager is the cluster leader
    term: int                               # Current leadership term
    known_managers: list[ManagerInfo] = field(default_factory=list)  # Other managers worker should know


@dataclass(slots=True, kw_only=True)
class ManagerToWorkerRegistrationAck(Message):
    """
    Acknowledgment from worker to manager registration.
    """
    accepted: bool                          # Whether registration was accepted
    worker_id: str                          # Worker's node_id
    total_cores: int = 0                    # Worker's total cores
    available_cores: int = 0                # Worker's available cores
    error: str | None = None                # Error message if not accepted


@dataclass(slots=True, kw_only=True)
class WorkflowProgressAck(Message):
    """
    Acknowledgment for workflow progress updates.
    
    Includes updated manager list so workers can maintain
    accurate view of cluster topology and leadership.
    """
    manager_id: str                         # Responding manager's node_id
    is_leader: bool                         # Whether this manager is leader
    healthy_managers: list[ManagerInfo]     # Current healthy managers


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
    node_id: str                 # Gate's unique identifier
    tcp_host: str                # TCP host for data operations
    tcp_port: int                # TCP port for data operations
    udp_host: str                # UDP host for SWIM healthchecks
    udp_port: int                # UDP port for SWIM healthchecks
    datacenter: str              # Datacenter identifier (gate's home DC)
    is_leader: bool = False      # Whether this gate is the current leader


@dataclass(slots=True)
class GateHeartbeat(Message):
    """
    Periodic heartbeat from gate embedded in SWIM messages.
    
    Contains gate-level status for cross-DC coordination.
    Gates are the top-level coordinators managing global job state.
    """
    node_id: str                 # Gate identifier
    datacenter: str              # Gate's home datacenter
    is_leader: bool              # Is this the leader gate?
    term: int                    # Leadership term
    version: int                 # State version
    state: str                   # GateState value (syncing, active, draining)
    active_jobs: int             # Number of active global jobs
    active_datacenters: int      # Number of datacenters with active work
    manager_count: int           # Number of registered managers


@dataclass(slots=True, kw_only=True)
class ManagerRegistrationResponse(Message):
    """
    Registration acknowledgment from gate to manager.
    
    Contains list of all known healthy gates so manager can
    establish redundant communication channels.
    """
    accepted: bool                          # Whether registration was accepted
    gate_id: str                            # Responding gate's node_id
    healthy_gates: list[GateInfo]           # All known healthy gates (including self)
    error: str | None = None                # Error message if not accepted


@dataclass(slots=True, kw_only=True)
class ManagerDiscoveryBroadcast(Message):
    """
    Broadcast from one gate to another about a newly discovered manager.
    
    Used for cross-gate synchronization of manager discovery.
    When a manager registers with one gate, that gate broadcasts
    to all peer gates so they can also track the manager.
    
    Includes manager status so peer gates can also update _datacenter_status.
    """
    datacenter: str                         # Manager's datacenter
    manager_tcp_addr: tuple[str, int]       # Manager's TCP address
    manager_udp_addr: tuple[str, int] | None = None  # Manager's UDP address (if known)
    source_gate_id: str = ""                # Gate that received the original registration
    # Manager status info (from registration heartbeat)
    worker_count: int = 0                   # Number of workers manager has
    healthy_worker_count: int = 0           # Healthy workers (SWIM responding)
    available_cores: int = 0                # Available cores for job dispatch
    total_cores: int = 0                    # Total cores across all workers


@dataclass(slots=True, kw_only=True)
class WorkerDiscoveryBroadcast(Message):
    """
    Broadcast from one manager to another about a newly discovered worker.
    
    Used for cross-manager synchronization of worker discovery.
    When a worker registers with one manager, that manager broadcasts
    to all peer managers so they can also track the worker.
    """
    worker_id: str                          # Worker's node_id
    worker_tcp_addr: tuple[str, int]        # Worker's TCP address
    worker_udp_addr: tuple[str, int]        # Worker's UDP address
    datacenter: str                         # Worker's datacenter
    available_cores: int                    # Worker's available cores
    source_manager_id: str = ""             # Manager that received the original registration


@dataclass(slots=True, kw_only=True)
class JobProgressAck(Message):
    """
    Acknowledgment for job progress updates from gates to managers.
    
    Includes updated gate list so managers can maintain
    accurate view of gate cluster topology and leadership.
    """
    gate_id: str                            # Responding gate's node_id
    is_leader: bool                         # Whether this gate is leader
    healthy_gates: list[GateInfo]           # Current healthy gates


@dataclass(slots=True)
class WorkerRegistration(Message):
    """
    Worker registration message sent to managers.
    
    Contains worker identity and capacity information.
    """
    node: NodeInfo               # Worker identity
    total_cores: int             # Total CPU cores available
    available_cores: int         # Currently free cores
    memory_mb: int               # Total memory in MB
    available_memory_mb: int     # Currently free memory


@dataclass(slots=True)
class WorkerHeartbeat(Message):
    """
    Periodic heartbeat from worker to manager.

    Contains current state and resource utilization.
    """
    node_id: str                 # Worker identifier
    state: str                   # WorkerState value
    available_cores: int         # Free cores
    queue_depth: int             # Pending workflow count
    cpu_percent: float           # CPU utilization 0-100
    memory_percent: float        # Memory utilization 0-100
    version: int                 # State version for sync
    # Active workflows and their status
    active_workflows: dict[str, str] = field(default_factory=dict)
    # TCP address for routing (populated in UDP heartbeats)
    tcp_host: str = ""
    tcp_port: int = 0


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
    """
    node_id: str                 # Manager identifier
    datacenter: str              # Datacenter identifier
    is_leader: bool              # Is this the leader manager?
    term: int                    # Leadership term
    version: int                 # State version
    active_jobs: int             # Number of active jobs
    active_workflows: int        # Number of active workflows
    worker_count: int            # Number of registered workers (total)
    healthy_worker_count: int    # Number of workers responding to SWIM probes
    available_cores: int         # Total available cores across healthy workers
    total_cores: int             # Total cores across all registered workers
    state: str = "active"        # ManagerState value (syncing/active/draining)
    tcp_host: str = ""           # Manager's TCP host (for proper storage key)
    tcp_port: int = 0            # Manager's TCP port (for proper storage key)
    udp_host: str = ""           # Manager's UDP host (for SWIM registration)
    udp_port: int = 0            # Manager's UDP port (for SWIM registration)
    # Per-job leadership - piggybacked on SWIM UDP for distributed consistency
    # Maps job_id -> (fencing_token, layer_version) for jobs this manager leads
    job_leaderships: dict[str, tuple[int, int]] = field(default_factory=dict)


# =============================================================================
# Job Submission and Dispatch
# =============================================================================

@dataclass(slots=True)
class JobSubmission(Message):
    """
    Job submission from client to gate or manager.
    
    A job contains one or more workflow classes to execute.
    
    If callback_addr is provided, the gate/manager will push status
    updates to the client via TCP instead of requiring polling.
    """
    job_id: str                  # Unique job identifier
    workflows: bytes             # Cloudpickled list of Workflow classes
    vus: int                     # Virtual users (cores to use per workflow)
    timeout_seconds: float       # Maximum execution time
    datacenter_count: int = 1    # Number of DCs to run in (gates only)
    datacenters: list[str] = field(default_factory=list)
    # Optional callback address for push notifications
    # If set, server pushes status updates to this address
    callback_addr: tuple[str, int] | None = None


@dataclass(slots=True)
class JobAck(Message):
    """
    Acknowledgment of job submission.
    
    Returned immediately after job is accepted for processing.
    If rejected due to not being leader, leader_addr provides redirect target.
    """
    job_id: str                  # Job identifier
    accepted: bool               # Whether job was accepted
    error: str | None = None     # Error message if rejected
    queued_position: int = 0     # Position in queue (if queued)
    leader_addr: tuple[str, int] | None = None  # Leader address for redirect


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
    job_id: str                  # Parent job identifier
    workflow_id: str             # Unique workflow instance ID
    workflow: bytes              # Cloudpickled Workflow class
    context: bytes               # Cloudpickled context dict (legacy, may be empty)
    vus: int                     # Virtual users (can be 50k+)
    cores: int                   # CPU cores to allocate (from priority)
    timeout_seconds: float       # Execution timeout
    fence_token: int             # Fencing token for at-most-once
    # Context Consistency Protocol fields
    context_version: int = 0     # Layer version for staleness detection
    dependency_context: bytes = b''  # Context from dependencies only

    def load_workflow(self) -> Workflow:
        return Message.load(self.workflow)
    
    def load_context(self) -> Context:
        return Message.load(self.context)


@dataclass(slots=True)
class WorkflowDispatchAck(Message):
    """
    Worker acknowledgment of workflow dispatch.
    """
    workflow_id: str             # Workflow identifier
    accepted: bool               # Whether worker accepted
    error: str | None = None     # Error message if rejected
    cores_assigned: int = 0      # Actual cores assigned


# =============================================================================
# Status Updates and Reporting
# =============================================================================

@dataclass(slots=True)
class StepStats(Message):
    """
    Statistics for a single workflow step.
    """
    step_name: str               # Step method name
    completed_count: int = 0     # Successful executions
    failed_count: int = 0        # Failed executions
    total_count: int = 0         # Total attempts


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
    """
    job_id: str                  # Parent job
    workflow_id: str             # Workflow instance
    workflow_name: str           # Workflow class name
    status: str                  # WorkflowStatus value
    completed_count: int         # Total actions completed
    failed_count: int            # Total actions failed
    rate_per_second: float       # Current execution rate
    elapsed_seconds: float       # Time since start
    step_stats: list["StepStats"] = field(default_factory=list)
    timestamp: float = 0.0       # Monotonic timestamp
    assigned_cores: list[int] = field(default_factory=list)  # Per-core assignment
    cores_completed: int = 0     # Cores that have finished their portion
    avg_cpu_percent: float = 0.0   # Average CPU utilization
    avg_memory_mb: float = 0.0     # Average memory usage in MB
    vus: int = 0                   # Virtual users (from workflow config)
    worker_workflow_assigned_cores: int = 0
    worker_workflow_completed_cores: int = 0
    worker_available_cores: int = 0 # Available cores for worker.


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
    job_id: str                  # Parent job
    workflow_id: str             # Workflow instance
    workflow_name: str           # Workflow class name
    status: str                  # COMPLETED | FAILED
    results: dict[int, WorkflowResults]               # Cloudpickled dict[int, WorkflowResults]
    context_updates: bytes       # Cloudpickled context dict (for Provide hooks)
    error: str | None = None     # Error message if failed (no traceback)
    worker_id: str = ""          # Worker that executed this workflow
    worker_available_cores: int = 0  # Worker's available cores after completion


@dataclass(slots=True)
class WorkflowResult(Message):
    """
    Simplified workflow result for aggregation (without context).
    
    Used in JobFinalResult for Manager -> Gate communication.
    Context is NOT included because gates don't need it.
    """
    workflow_id: str             # Workflow instance ID
    workflow_name: str           # Workflow class name
    status: str                  # COMPLETED | FAILED
    results: bytes               # Cloudpickled WorkflowStats
    error: str | None = None     # Error message if failed


@dataclass(slots=True)
class JobFinalResult(Message):
    """
    Final result for a job from one datacenter.

    Sent from Manager to Gate (or directly to Client if no gates).
    Contains per-workflow results and aggregated stats.
    """
    job_id: str                  # Job identifier
    datacenter: str              # Reporting datacenter
    status: str                  # COMPLETED | FAILED | PARTIAL
    workflow_results: list["WorkflowResult"] = field(default_factory=list)
    total_completed: int = 0     # Total successful actions
    total_failed: int = 0        # Total failed actions
    errors: list[str] = field(default_factory=list)  # All error messages
    elapsed_seconds: float = 0.0 # Max elapsed across workflows
    fence_token: int = 0         # Fencing token for at-most-once semantics


@dataclass(slots=True)
class AggregatedJobStats(Message):
    """
    Aggregated statistics across all datacenters.
    
    Part of GlobalJobResult for cross-DC aggregation.
    """
    total_requests: int = 0      # Total actions across all DCs
    successful_requests: int = 0 # Successful actions
    failed_requests: int = 0     # Failed actions
    overall_rate: float = 0.0    # Combined rate (requests/sec)
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
    job_id: str                  # Job identifier
    status: str                  # COMPLETED | FAILED | PARTIAL
    # Per-datacenter breakdown
    per_datacenter_results: list["JobFinalResult"] = field(default_factory=list)
    # Cross-DC aggregated stats
    aggregated: "AggregatedJobStats" = field(default_factory=AggregatedJobStats)
    # Summary
    total_completed: int = 0     # Sum across all DCs
    total_failed: int = 0        # Sum across all DCs
    successful_datacenters: int = 0
    failed_datacenters: int = 0
    errors: list[str] = field(default_factory=list)  # All errors from all DCs
    elapsed_seconds: float = 0.0 # Max elapsed across all DCs


@dataclass(slots=True)
class JobProgress(Message):
    """
    Aggregated job progress from manager to gate.

    Contains summary of all workflows in the job.
    """
    job_id: str                  # Job identifier
    datacenter: str              # Reporting datacenter
    status: str                  # JobStatus value
    workflows: list["WorkflowProgress"] = field(default_factory=list)
    total_completed: int = 0     # Total actions completed
    total_failed: int = 0        # Total actions failed
    overall_rate: float = 0.0    # Aggregate rate
    elapsed_seconds: float = 0.0 # Time since job start
    timestamp: float = 0.0       # Monotonic timestamp
    # Aggregated step stats across all workflows in the job
    step_stats: list["StepStats"] = field(default_factory=list)
    fence_token: int = 0         # Fencing token for at-most-once semantics


@dataclass(slots=True)
class GlobalJobStatus(Message):
    """
    Global job status aggregated by gate across datacenters.

    This is what gets returned to the client.
    """
    job_id: str                  # Job identifier
    status: str                  # JobStatus value
    datacenters: list["JobProgress"] = field(default_factory=list)
    total_completed: int = 0     # Global total completed
    total_failed: int = 0        # Global total failed
    overall_rate: float = 0.0    # Global aggregate rate
    elapsed_seconds: float = 0.0 # Time since submission
    completed_datacenters: int = 0  # DCs finished
    failed_datacenters: int = 0  # DCs failed
    timestamp: float = 0.0       # Monotonic time when job was submitted


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
    job_id: str                  # Job being led
    leader_id: str               # Node ID of the job leader
    leader_host: str             # Host of the job leader
    leader_tcp_port: int         # TCP port of the job leader
    term: int                    # Cluster term when job was accepted
    workflow_count: int = 0      # Number of workflows in job
    timestamp: float = 0.0       # When job was accepted
    # Workflow names for query support (non-leaders can track job contents)
    workflow_names: list[str] = field(default_factory=list)


@dataclass(slots=True)
class JobLeadershipAck(Message):
    """
    Acknowledgment of job leadership announcement.
    """
    job_id: str                  # Job being acknowledged
    accepted: bool               # Whether announcement was accepted
    responder_id: str            # Node ID of responder


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
    leader_id: str               # Node ID of the job leader
    job_id: str                  # Job identifier
    status: str                  # Current JobStatus value
    fencing_token: int           # Current fencing token for consistency
    workflows_total: int         # Total workflows in job
    workflows_completed: int     # Completed workflow count
    workflows_failed: int        # Failed workflow count
    workflow_statuses: dict[str, str] = field(default_factory=dict)  # workflow_id -> status
    elapsed_seconds: float = 0.0  # Time since job started
    timestamp: float = 0.0       # When this sync was generated


@dataclass(slots=True)
class JobStateSyncAck(Message):
    """
    Acknowledgment of job state sync.
    """
    job_id: str                  # Job being acknowledged
    responder_id: str            # Node ID of responder
    accepted: bool = True        # Whether sync was applied


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
    job_id: str                  # Job identifier
    status: str                  # JobStatus value
    message: str                 # Human-readable status message
    total_completed: int = 0     # Completed count (aggregated across all DCs)
    total_failed: int = 0        # Failed count (aggregated across all DCs)
    overall_rate: float = 0.0    # Current rate (aggregated across all DCs)
    elapsed_seconds: float = 0.0 # Time since submission
    is_final: bool = False       # True if job is complete (no more updates)
    # Per-datacenter breakdown (for clients that want granular visibility)
    per_dc_stats: list["DCStats"] = field(default_factory=list)
    fence_token: int = 0         # Fencing token for at-most-once semantics


@dataclass(slots=True)
class DCStats(Message):
    """
    Per-datacenter statistics for real-time status updates.
    
    Used in JobStatusPush to provide per-DC visibility without
    the full detail of JobProgress (which includes workflow-level stats).
    """
    datacenter: str              # Datacenter identifier
    status: str                  # DC-specific status
    completed: int = 0           # Completed in this DC
    failed: int = 0              # Failed in this DC
    rate: float = 0.0            # Rate in this DC


@dataclass(slots=True)
class JobBatchPush(Message):
    """
    Batched statistics push notification.
    
    Sent periodically (Tier 2) with aggregated progress data.
    Contains step-level statistics and detailed progress.
    Includes per-DC breakdown for granular visibility.
    """
    job_id: str                  # Job identifier
    status: str                  # Current JobStatus
    step_stats: list["StepStats"] = field(default_factory=list)
    total_completed: int = 0     # Aggregated across all DCs
    total_failed: int = 0        # Aggregated across all DCs
    overall_rate: float = 0.0    # Aggregated across all DCs
    elapsed_seconds: float = 0.0
    # Per-datacenter breakdown (for clients that want granular visibility)
    per_dc_stats: list["DCStats"] = field(default_factory=list)


# =============================================================================
# State Synchronization
# =============================================================================

@dataclass(slots=True)
class WorkerStateSnapshot(Message):
    """
    Complete state snapshot from a worker.

    Used for state sync when a new manager becomes leader.
    """
    node_id: str                 # Worker identifier
    state: str                   # WorkerState value
    total_cores: int             # Total cores
    available_cores: int         # Free cores
    version: int                 # State version
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
    node_id: str                 # Manager identifier
    datacenter: str              # Datacenter
    is_leader: bool              # Leadership status
    term: int                    # Current term
    version: int                 # State version
    workers: list["WorkerStateSnapshot"] = field(default_factory=list)
    jobs: dict[str, "JobProgress"] = field(default_factory=dict)
    # Context consistency protocol state
    job_leaders: dict[str, str] = field(default_factory=dict)  # job_id -> leader_node_id
    job_leader_addrs: dict[str, tuple[str, int]] = field(default_factory=dict)  # job_id -> (host, tcp_port)
    job_layer_versions: dict[str, int] = field(default_factory=dict)  # job_id -> layer version
    job_contexts: bytes = b''  # Serialized contexts (cloudpickle)


@dataclass(slots=True)
class GateStateSnapshot(Message):
    """
    Complete state snapshot from a gate.
    
    Used for state sync between gates when a new leader is elected.
    Contains global job state and datacenter status.
    """
    node_id: str                 # Gate identifier
    is_leader: bool              # Leadership status
    term: int                    # Current term
    version: int                 # State version
    jobs: dict[str, "GlobalJobStatus"] = field(default_factory=dict)
    datacenter_status: dict[str, "DatacenterStatus"] = field(default_factory=dict)
    leases: dict[str, "DatacenterLease"] = field(default_factory=dict)
    # Manager discovery - shared between gates
    datacenter_managers: dict[str, list[tuple[str, int]]] = field(default_factory=dict)
    datacenter_manager_udp: dict[str, list[tuple[str, int]]] = field(default_factory=dict)


@dataclass(slots=True)
class StateSyncRequest(Message):
    """
    Request for state synchronization.
    
    Sent by new leader to gather current state.
    """
    requester_id: str            # Requesting node
    requester_role: str          # NodeRole value
    since_version: int = 0       # Only send updates after this version


@dataclass(slots=True)
class StateSyncResponse(Message):
    """
    Response to state sync request.

    The responder_ready field indicates whether the responder has completed
    its own startup and is ready to serve authoritative state. If False,
    the requester should retry after a delay.
    """
    responder_id: str            # Responding node
    current_version: int         # Current state version
    responder_ready: bool = True # Whether responder has completed startup
    # One of these will be set based on node type
    worker_state: "WorkerStateSnapshot | None" = None
    manager_state: "ManagerStateSnapshot | None" = None
    gate_state: "GateStateSnapshot | None" = None


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
    job_id: str                  # Job identifier
    workflow_id: str             # Source workflow
    context_updates: bytes       # Serialized Dict[key, value]
    context_timestamps: bytes    # Serialized Dict[key, lamport_clock]
    source_manager: str          # Manager node_id that received from worker


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
    job_id: str                  # Job identifier
    layer_version: int           # Monotonically increasing per job
    context_snapshot: bytes      # Full context as cloudpickle.dumps(context.dict())
    source_node_id: str          # Job leader's node_id


@dataclass(slots=True)
class ContextLayerSyncAck(Message):
    """
    Peer manager confirms receipt of context layer sync.
    
    Job leader waits for quorum of these before advancing to next layer.
    """
    job_id: str                  # Job identifier
    layer_version: int           # Echoed back for correlation
    applied: bool                # True if applied, False if stale/rejected
    responder_id: str            # Responding manager's node_id


# =============================================================================
# Quorum and Confirmation
# =============================================================================

@dataclass(slots=True)
class ProvisionRequest(Message):
    """
    Request to provision a workflow across the cluster.
    
    Sent from leader manager to all managers for quorum confirmation.
    """
    job_id: str                  # Job identifier
    workflow_id: str             # Workflow to provision
    target_worker: str           # Selected worker node_id
    cores_required: int          # Cores needed
    fence_token: int             # Fencing token
    version: int                 # State version for this decision


@dataclass(slots=True)
class ProvisionConfirm(Message):
    """
    Confirmation of provision request.
    
    Manager acknowledges the provisioning decision.
    """
    job_id: str                  # Job identifier
    workflow_id: str             # Workflow
    confirming_node: str         # Node confirming
    confirmed: bool              # Whether confirmed
    version: int                 # Node's current version
    error: str | None = None     # Error if not confirmed


@dataclass(slots=True)
class ProvisionCommit(Message):
    """
    Commit message after quorum achieved.
    
    Tells all managers the provisioning is final.
    """
    job_id: str                  # Job identifier
    workflow_id: str             # Workflow
    target_worker: str           # Worker receiving the workflow
    cores_assigned: int          # Cores allocated
    fence_token: int             # Fencing token
    committed_version: int       # Version at commit time


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
    job_id: str                  # Job to cancel
    reason: str = ""             # Cancellation reason
    fence_token: int = 0         # Fencing token for validation


@dataclass(slots=True)
class CancelAck(Message):
    """
    Acknowledgment of cancellation.
    """
    job_id: str                  # Job identifier
    cancelled: bool              # Whether successfully cancelled
    workflows_cancelled: int = 0 # Number of workflows stopped
    error: str | None = None     # Error if cancellation failed


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
    job_id: str                  # Job identifier
    datacenter: str              # Datacenter holding lease
    lease_holder: str            # Gate node_id holding lease
    fence_token: int             # Fencing token
    expires_at: float            # Monotonic expiration time
    version: int                 # Lease version


@dataclass(slots=True)
class LeaseTransfer(Message):
    """
    Transfer a lease to another gate (during scaling).
    """
    job_id: str                  # Job identifier
    datacenter: str              # Datacenter
    from_gate: str               # Current holder
    to_gate: str                 # New holder
    new_fence_token: int         # New fencing token
    version: int                 # Transfer version


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
    dc_id: str                       # Datacenter identifier
    health: str                      # DatacenterHealth value
    available_capacity: int = 0      # Estimated available cores
    queue_depth: int = 0             # Jobs waiting
    manager_count: int = 0           # Responding managers (via SWIM)
    worker_count: int = 0            # Available workers
    last_update: float = 0.0         # Timestamp of last status update


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
    request_id: str                  # Unique request identifier


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
    worker_id: str                   # Worker's node_id
    state: str                       # WorkerState value (as string for wire)
    available_cores: int = 0         # Currently available cores
    total_cores: int = 0             # Total cores on worker
    queue_depth: int = 0             # Pending workflows
    cpu_percent: float = 0.0         # CPU utilization
    memory_percent: float = 0.0      # Memory utilization
    # Manager-internal tracking fields (not used in wire protocol)
    registration: "WorkerRegistration | None" = None  # Full registration info
    heartbeat: "WorkerHeartbeat | None" = None        # Last heartbeat received
    last_seen: float = 0.0                            # Monotonic time of last contact
    reserved_cores: int = 0                           # Cores reserved but not confirmed

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
        object.__setattr__(self, 'state', value.value)

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
    request_id: str                  # Echoed from request
    manager_id: str                  # Manager's node_id
    datacenter: str                  # Datacenter identifier
    host: str                        # Manager TCP host
    port: int                        # Manager TCP port
    is_leader: bool                  # Whether this manager is the DC leader
    state: str                       # ManagerState value
    term: int                        # Current leadership term
    # Capacity
    total_cores: int = 0             # Total cores across all workers
    available_cores: int = 0         # Available cores (healthy workers only)
    # Workers
    worker_count: int = 0            # Total registered workers
    healthy_worker_count: int = 0    # Workers responding to SWIM
    workers: list[WorkerStatus] = field(default_factory=list)  # Per-worker status
    # Jobs
    active_job_ids: list[str] = field(default_factory=list)  # Currently active jobs
    active_job_count: int = 0        # Number of active jobs
    active_workflow_count: int = 0   # Number of active workflows
    # Cluster info
    peer_managers: list[tuple[str, int]] = field(default_factory=list)  # Known peer manager addrs


@dataclass(slots=True, kw_only=True)
class DatacenterInfo(Message):
    """
    Information about a datacenter as seen by a gate.

    Used in GatePingResponse to report per-DC status.
    """
    dc_id: str                       # Datacenter identifier
    health: str                      # DatacenterHealth value
    leader_addr: tuple[str, int] | None = None  # DC leader's TCP address
    available_cores: int = 0         # Available cores in DC
    manager_count: int = 0           # Managers in DC
    worker_count: int = 0            # Workers in DC


@dataclass(slots=True, kw_only=True)
class GatePingResponse(Message):
    """
    Ping response from a gate.

    Contains gate status and datacenter health info.
    """
    request_id: str                  # Echoed from request
    gate_id: str                     # Gate's node_id
    datacenter: str                  # Gate's home datacenter
    host: str                        # Gate TCP host
    port: int                        # Gate TCP port
    is_leader: bool                  # Whether this gate is the gate cluster leader
    state: str                       # GateState value
    term: int                        # Current leadership term
    # Datacenters
    datacenters: list[DatacenterInfo] = field(default_factory=list)  # Per-DC status
    active_datacenter_count: int = 0 # Number of active datacenters
    # Jobs
    active_job_ids: list[str] = field(default_factory=list)  # Currently active jobs
    active_job_count: int = 0        # Number of active jobs
    # Cluster info
    peer_gates: list[tuple[str, int]] = field(default_factory=list)  # Known peer gate addrs


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
    request_id: str                      # Unique request identifier
    workflow_names: list[str]            # Workflow class names to query
    job_id: str | None = None            # Optional: filter to specific job


@dataclass(slots=True, kw_only=True)
class WorkflowStatusInfo(Message):
    """
    Status information for a single workflow.

    Returned as part of WorkflowQueryResponse.
    """
    workflow_name: str                   # Workflow class name
    workflow_id: str                     # Unique workflow instance ID
    job_id: str                          # Parent job ID
    status: str                          # WorkflowStatus value
    # Provisioning info
    provisioned_cores: int = 0           # Cores allocated to this workflow
    vus: int = 0                         # Virtual users (from workflow config)
    # Progress info
    completed_count: int = 0             # Actions completed
    failed_count: int = 0                # Actions failed
    rate_per_second: float = 0.0         # Current execution rate
    elapsed_seconds: float = 0.0         # Time since start
    # Queue info
    is_enqueued: bool = False            # True if waiting for cores
    queue_position: int = 0              # Position in queue (0 if not queued)
    # Worker assignment
    assigned_workers: list[str] = field(default_factory=list)  # Worker IDs


@dataclass(slots=True, kw_only=True)
class WorkflowQueryResponse(Message):
    """
    Response to workflow query from a manager.

    Contains status for all matching workflows.
    """
    request_id: str                      # Echoed from request
    manager_id: str                       # Responding manager's node_id
    datacenter: str                      # Manager's datacenter
    workflows: list[WorkflowStatusInfo] = field(default_factory=list)


@dataclass(slots=True, kw_only=True)
class DatacenterWorkflowStatus(Message):
    """
    Workflow status for a single datacenter.

    Used in GateWorkflowQueryResponse to group results by DC.
    """
    dc_id: str                           # Datacenter identifier
    workflows: list[WorkflowStatusInfo] = field(default_factory=list)


@dataclass(slots=True, kw_only=True)
class GateWorkflowQueryResponse(Message):
    """
    Response to workflow query from a gate.

    Contains status grouped by datacenter.
    """
    request_id: str                      # Echoed from request
    gate_id: str                         # Responding gate's node_id
    datacenters: list[DatacenterWorkflowStatus] = field(default_factory=list)


@dataclass
class EagerWorkflowEntry:
    """
    Tracking entry for a workflow pending eager dispatch.

    Contains all information needed to dispatch the workflow once
    its dependencies are met and cores are available.
    """
    job_id: str                          # Parent job ID
    workflow_name: str                   # Workflow name (graph node)
    workflow_idx: int                    # Index in job's workflow list
    workflow: Any                        # The workflow instance
    vus: int                             # Virtual users for this workflow
    priority: "StagePriority"            # Workflow priority
    is_test: bool                        # Whether this is a test workflow
    dependencies: set[str]               # Set of workflow names this depends on
    completed_dependencies: set[str] = field(default_factory=set)  # Dependencies that have completed
    dispatched: bool = False             # Whether this workflow has been dispatched
    cores_allocated: int = 0             # Cores allocated (set at dispatch time)