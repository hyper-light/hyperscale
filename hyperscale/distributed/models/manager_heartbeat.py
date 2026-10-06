"""Wire model ``ManagerHeartbeat`` -- pickled under the wire namespace
``hyperscale.distributed.models.distributed`` (see that module)."""

from typing import TYPE_CHECKING
from dataclasses import dataclass, field
from .message import Message

if TYPE_CHECKING:
    from hyperscale.distributed.models.coordinates import NetworkCoordinate
    from hyperscale.distributed.resources.manager_resource_report import ManagerResourceReport


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
    # AD-19 addendum (Phase D): worker-tier LHM aggregated by the
    # manager from incoming WorkerHeartbeats. Gates consume this to
    # see worker-tier stress in their cross_dc_correlation alongside
    # the manager-tier ``lhm_score``. Carries the **max** observed
    # across reporting workers — any worker saturating raises the
    # DC-wide signal. Zero when no workers are currently registered
    # or none are reporting.
    worker_max_lhm_score: int = 0
    # Datacenter capacity inputs consumed by ``DataCenterCapacity.from_heartbeats``.
    # Without these, the gate's submission path raises
    # ``'ManagerHeartbeat' object has no attribute 'pending_workflow_count'``
    # when computing DC capacity, which surfaces as
    # ``Job rejected: '...'`` at the client and the workflow never
    # reaches RUNNING. Neutral defaults preserve back-compat with
    # peers running an older heartbeat schema.
    pending_workflow_count: int = 0
    pending_duration_seconds: float = 0.0
    active_remaining_seconds: float = 0.0
    # AD-43 Part 4: when the cores this manager's executing workflows hold
    # come free -- ``(seconds after this report, cores)``, soonest first.
    # A gate walks it to see when a job's cores will be free; the backlog
    # sums above only tell it how long the work ahead takes to drain.
    cores_freeing_schedule: list[tuple[float, int]] = field(default_factory=list)
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
    # AD-42 Phase E: per-DC SLO summary disseminated manager → gate.
    # All fields default to a neutral baseline so back-compat with
    # peers that pre-date Phase E is preserved (a zero-sample summary
    # gives compliance_score=1.0, routing_factor=1.0).
    slo_p50_ms: float = 0.0
    slo_p95_ms: float = 0.0
    slo_p99_ms: float = 0.0
    slo_sample_count: int = 0
    slo_compliance_score: float = 1.0
    slo_routing_factor: float = 1.0
    slo_updated_at: float = 0.0
    # AD-41: this manager's resource report for gates. Only the TCP
    # status update to gates carries it (it would push the SWIM-embedded
    # heartbeat further past the datagram budget); None elsewhere, and
    # gates keep the last report a manager sent rather than clearing it.
    resource_report: "ManagerResourceReport | None" = None
    # Whether this manager's durable storage (its job ledger) last proved
    # writable. A manager that cannot write durably cannot accept jobs:
    # gates classify a datacenter whose authoritative manager reports
    # False as UNHEALTHY, so placement routes around a full or failing
    # disk. Managers without a durable tier always report True.
    storage_writable: bool = True
