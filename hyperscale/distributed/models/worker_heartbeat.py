"""Wire model ``WorkerHeartbeat`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from typing import TYPE_CHECKING
from dataclasses import dataclass, field
from .message import Message

if TYPE_CHECKING:
    from hyperscale.distributed.models.coordinates import NetworkCoordinate


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
    # Total configured cores. Multiple consumers (manager health monitor,
    # datacenter capacity aggregator, datacenter health manager) need to
    # know the worker's full core budget alongside available_cores; without
    # this field they raised AttributeError, dropping every heartbeat.
    total_cores: int = 0
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
    # Phase F1 — workflow id of the snapshot the extension request
    # describes. Required for the manager's H5 multi-witness routing
    # path; absent means the manager falls back to the worker-level
    # legacy path. Empty string when no workflow-scoped extension is
    # being requested.
    extension_workflow_id: str = ""
    # Phase H3 — multi-dimensional WorkflowProgressSnapshot fields.
    # ``extension_completed_items`` is the primary (cores_completed)
    # signal; the two below are the secondary and tertiary signals
    # that AD-26 H5 multi-witness decision uses for tamper-resistant
    # progress validation. All three counters are integer monotonic
    # on the worker side; the manager rejects extension requests
    # where any dimension regresses.
    extension_step_transitions: int = 0  # AD-54 step-state transitions since dispatch
    extension_actions_completed: int = 0  # Sum of StepStats.completed_count across active steps
    extension_snapshot_time: float = 0.0  # time.monotonic() on worker when snapshot constructed
    # AD-19 addendum (Phase D): uniform LHM gossip across all heartbeat
    # tiers. Worker reports its raw LocalHealthMultiplier.score (0-8)
    # so cross_dc_correlation can correlate worker stress against
    # manager/gate stress and distinguish systemic load (multiple tiers
    # elevated) from isolated failures (one tier elevated).
    lhm_score: int = 0  # Local Health Multiplier score (0-8)
    # The worker's core availability version as of available_cores: orders
    # this report against its progress reports, results and dispatch acks.
    cores_version: int = 0
