"""
Distributed system message types for Gate, Manager, and Worker nodes.

These dataclasses define the wire format for all TCP communication
in the distributed Hyperscale architecture.
"""

from dataclasses import dataclass, field
from enum import Enum
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
    ASSIGNED = "assigned"        # Assigned to worker(s)
    RUNNING = "running"          # Executing
    COMPLETED = "completed"      # Finished successfully
    FAILED = "failed"            # Failed
    CANCELLED = "cancelled"      # Cancelled


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


@dataclass(slots=True)
class ManagerHeartbeat(Message):
    """
    Periodic heartbeat from manager to gates (if gates present).
    
    Contains datacenter-level job status summary.
    """
    node_id: str                 # Manager identifier
    datacenter: str              # Datacenter identifier
    is_leader: bool              # Is this the leader manager?
    term: int                    # Leadership term
    version: int                 # State version
    active_jobs: int             # Number of active jobs
    active_workflows: int        # Number of active workflows
    worker_count: int            # Number of registered workers
    available_cores: int         # Total available cores across workers
    state: str = "active"        # ManagerState value (syncing/active/draining)


# =============================================================================
# Job Submission and Dispatch
# =============================================================================

@dataclass(slots=True)
class JobSubmission(Message):
    """
    Job submission from client to gate or manager.
    
    A job contains one or more workflow classes to execute.
    """
    job_id: str                  # Unique job identifier
    workflows: bytes             # Cloudpickled list of Workflow classes
    vus: int                     # Virtual users (cores to use per workflow)
    timeout_seconds: float       # Maximum execution time
    datacenter_count: int = 1    # Number of DCs to run in (gates only)
    datacenters: list[str] = field(default_factory=list)


@dataclass(slots=True)
class JobAck(Message):
    """
    Acknowledgment of job submission.
    
    Returned immediately after job is accepted for processing.
    """
    job_id: str                  # Job identifier
    accepted: bool               # Whether job was accepted
    error: str | None = None     # Error message if rejected
    queued_position: int = 0     # Position in queue (if queued)


@dataclass(slots=True)
class WorkflowDispatch(Message):
    """
    Dispatch a single workflow to a worker.
    
    Sent from manager to worker for execution.
    """
    job_id: str                  # Parent job identifier
    workflow_id: str             # Unique workflow instance ID
    workflow: bytes              # Cloudpickled Workflow class
    context: bytes               # Cloudpickled context dict
    vus: int                     # Cores to use
    timeout_seconds: float       # Execution timeout
    fence_token: int             # Fencing token for at-most-once


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
    """
    responder_id: str            # Responding node
    current_version: int         # Current state version
    # One of these will be set based on node type
    worker_state: "WorkerStateSnapshot | None" = None
    manager_state: "ManagerStateSnapshot | None" = None
    gate_state: "GateStateSnapshot | None" = None


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
