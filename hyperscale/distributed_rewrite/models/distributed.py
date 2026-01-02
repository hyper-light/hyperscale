"""
Distributed system message types for Gate, Manager, and Worker nodes.

These msgspec Structs define the wire format for all TCP communication
in the distributed Hyperscale architecture.
"""

from enum import Enum
from typing import Literal
import msgspec


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


# =============================================================================
# Node Identity and Registration
# =============================================================================

class NodeInfo(msgspec.Struct, kw_only=True):
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


class WorkerRegistration(msgspec.Struct, kw_only=True):
    """
    Worker registration message sent to managers.
    
    Contains worker identity and capacity information.
    """
    node: NodeInfo               # Worker identity
    total_cores: int             # Total CPU cores available
    available_cores: int         # Currently free cores
    memory_mb: int               # Total memory in MB
    available_memory_mb: int     # Currently free memory


class WorkerHeartbeat(msgspec.Struct, kw_only=True):
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
    active_workflows: dict[str, str] = {}  # workflow_id -> WorkflowStatus


class ManagerHeartbeat(msgspec.Struct, kw_only=True):
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


# =============================================================================
# Job Submission and Dispatch
# =============================================================================

class JobSubmission(msgspec.Struct, kw_only=True):
    """
    Job submission from client to gate or manager.
    
    A job contains one or more workflow classes to execute.
    """
    job_id: str                  # Unique job identifier
    workflows: bytes             # Cloudpickled list of Workflow classes
    vus: int                     # Virtual users (cores to use per workflow)
    timeout_seconds: float       # Maximum execution time
    datacenter_count: int = 1    # Number of DCs to run in (gates only)
    datacenters: list[str] = []  # Specific DCs (empty = auto-select)


class JobAck(msgspec.Struct, kw_only=True):
    """
    Acknowledgment of job submission.
    
    Returned immediately after job is accepted for processing.
    """
    job_id: str                  # Job identifier
    accepted: bool               # Whether job was accepted
    error: str | None = None     # Error message if rejected
    queued_position: int = 0     # Position in queue (if queued)


class WorkflowDispatch(msgspec.Struct, kw_only=True):
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


class WorkflowDispatchAck(msgspec.Struct, kw_only=True):
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

class StepStats(msgspec.Struct, kw_only=True):
    """
    Statistics for a single workflow step.
    """
    step_name: str               # Step method name
    completed_count: int = 0     # Successful executions
    failed_count: int = 0        # Failed executions
    total_count: int = 0         # Total attempts


class WorkflowProgress(msgspec.Struct, kw_only=True):
    """
    Progress update for a running workflow.
    
    Sent from worker to manager during execution.
    """
    job_id: str                  # Parent job
    workflow_id: str             # Workflow instance
    workflow_name: str           # Workflow class name
    status: str                  # WorkflowStatus value
    completed_count: int         # Total actions completed
    failed_count: int            # Total actions failed
    rate_per_second: float       # Current execution rate
    elapsed_seconds: float       # Time since start
    step_stats: list[StepStats] = []  # Per-step breakdown
    timestamp: float = 0.0       # Monotonic timestamp


class JobProgress(msgspec.Struct, kw_only=True):
    """
    Aggregated job progress from manager to gate.
    
    Contains summary of all workflows in the job.
    """
    job_id: str                  # Job identifier
    datacenter: str              # Reporting datacenter
    status: str                  # JobStatus value
    workflows: list[WorkflowProgress] = []  # Per-workflow progress
    total_completed: int = 0     # Total actions completed
    total_failed: int = 0        # Total actions failed
    overall_rate: float = 0.0    # Aggregate rate
    elapsed_seconds: float = 0.0 # Time since job start
    timestamp: float = 0.0       # Monotonic timestamp


class GlobalJobStatus(msgspec.Struct, kw_only=True):
    """
    Global job status aggregated by gate across datacenters.
    
    This is what gets returned to the client.
    """
    job_id: str                  # Job identifier
    status: str                  # JobStatus value
    datacenters: list[JobProgress] = []  # Per-DC progress
    total_completed: int = 0     # Global total completed
    total_failed: int = 0        # Global total failed
    overall_rate: float = 0.0    # Global aggregate rate
    elapsed_seconds: float = 0.0 # Time since submission
    completed_datacenters: int = 0  # DCs finished
    failed_datacenters: int = 0  # DCs failed


# =============================================================================
# State Synchronization
# =============================================================================

class WorkerStateSnapshot(msgspec.Struct, kw_only=True):
    """
    Complete state snapshot from a worker.
    
    Used for state sync when a new manager becomes leader.
    """
    node_id: str                 # Worker identifier
    state: str                   # WorkerState value
    total_cores: int             # Total cores
    available_cores: int         # Free cores
    version: int                 # State version
    active_workflows: dict[str, WorkflowProgress] = {}  # workflow_id -> progress


class ManagerStateSnapshot(msgspec.Struct, kw_only=True):
    """
    Complete state snapshot from a manager.
    
    Used for state sync between managers.
    """
    node_id: str                 # Manager identifier
    datacenter: str              # Datacenter
    is_leader: bool              # Leadership status
    term: int                    # Current term
    version: int                 # State version
    workers: list[WorkerStateSnapshot] = []  # Registered workers
    jobs: dict[str, JobProgress] = {}  # Active jobs


class StateSyncRequest(msgspec.Struct, kw_only=True):
    """
    Request for state synchronization.
    
    Sent by new leader to gather current state.
    """
    requester_id: str            # Requesting node
    requester_role: str          # NodeRole value
    since_version: int = 0       # Only send updates after this version


class StateSyncResponse(msgspec.Struct, kw_only=True):
    """
    Response to state sync request.
    """
    responder_id: str            # Responding node
    current_version: int         # Current state version
    # One of these will be set based on node type
    worker_state: WorkerStateSnapshot | None = None
    manager_state: ManagerStateSnapshot | None = None


# =============================================================================
# Quorum and Confirmation
# =============================================================================

class ProvisionRequest(msgspec.Struct, kw_only=True):
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


class ProvisionConfirm(msgspec.Struct, kw_only=True):
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


class ProvisionCommit(msgspec.Struct, kw_only=True):
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

class CancelJob(msgspec.Struct, kw_only=True):
    """
    Request to cancel a job.
    
    Flows: client -> gate -> manager -> worker
           or: client -> manager -> worker
    """
    job_id: str                  # Job to cancel
    reason: str = ""             # Cancellation reason
    fence_token: int = 0         # Fencing token for validation


class CancelAck(msgspec.Struct, kw_only=True):
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

class DatacenterLease(msgspec.Struct, kw_only=True):
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


class LeaseTransfer(msgspec.Struct, kw_only=True):
    """
    Transfer a lease to another gate (during scaling).
    """
    job_id: str                  # Job identifier
    datacenter: str              # Datacenter
    from_gate: str               # Current holder
    to_gate: str                 # New holder
    new_fence_token: int         # New fencing token
    version: int                 # Transfer version

