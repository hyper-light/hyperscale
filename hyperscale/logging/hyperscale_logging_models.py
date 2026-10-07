from .models import Entry, LogLevel


class TestTrace(Entry, kw_only=True):
    test: str
    runner_type: str
    workflows: list[str]
    workers: int
    level: LogLevel = LogLevel.TRACE


class TestDebug(Entry, kw_only=True):
    test: str
    runner_type: str
    workflows: list[str]
    workers: int
    level: LogLevel = LogLevel.DEBUG


class TestFatal(Entry, kw_only=True):
    test: str
    runner_type: str
    workflows: list[str]
    workers: int
    level: LogLevel = LogLevel.FATAL


class TestError(Entry, kw_only=True):
    test: str
    runner_type: str
    workflows: list[str]
    workers: int
    level: LogLevel = LogLevel.ERROR


class TestInfo(Entry, kw_only=True):
    test: str
    runner_type: str
    workflows: list[str]
    workers: int
    level: LogLevel = LogLevel.INFO


class RemoteManagerInfo(Entry, kw_only=True):
    host: str
    port: int
    with_ssl: bool
    level: LogLevel = LogLevel.INFO


class GraphDebug(Entry, kw_only=True):
    graph: str
    workflows: list[str]
    workers: int
    level: LogLevel = LogLevel.DEBUG


class WorkflowTrace(Entry, kw_only=True):
    workflow: str
    duration: str
    run_id: int
    workflow_vus: int
    workers: int
    level: LogLevel = LogLevel.TRACE


class WorkflowDebug(Entry, kw_only=True):
    workflow: str
    duration: str
    run_id: int
    workflow_vus: int
    workers: int
    level: LogLevel = LogLevel.DEBUG


class WorkflowInfo(Entry, kw_only=True):
    workflow: str
    duration: str
    run_id: int
    workflow_vus: int
    workers: int
    level: LogLevel = LogLevel.INFO


class WorkflowError(Entry, kw_only=True):
    workflow: str
    duration: str
    run_id: int
    workflow_vus: int
    workers: int
    level: LogLevel = LogLevel.ERROR


class WorkflowFatal(Entry, kw_only=True):
    workflow: str
    duration: str
    run_id: int
    workflow_vus: int
    workers: int
    level: LogLevel = LogLevel.FATAL


class RunTrace(Entry, kw_only=True):
    node_id: str
    workflow: str
    duration: str
    run_id: int
    workflow_vus: int
    level: LogLevel = LogLevel.TRACE


class RunDebug(Entry, kw_only=True):
    node_id: str
    workflow: str
    duration: str
    run_id: int
    workflow_vus: int
    level: LogLevel = LogLevel.DEBUG


class RunInfo(Entry, kw_only=True):
    node_id: str
    workflow: str
    duration: str
    run_id: int
    workflow_vus: int
    level: LogLevel = LogLevel.INFO


class RunError(Entry, kw_only=True):
    node_id: str
    workflow: str
    duration: str
    run_id: int
    workflow_vus: int
    level: LogLevel = LogLevel.ERROR


class RunFatal(Entry, kw_only=True):
    node_id: str
    workflow: str
    duration: str
    run_id: int
    workflow_vus: int
    level: LogLevel = LogLevel.FATAL


class ServerTrace(Entry, kw_only=True):
    node_id: str
    node_host: str
    node_port: int
    level: LogLevel = LogLevel.TRACE


class ServerDebug(Entry, kw_only=True):
    node_id: str
    node_host: str
    node_port: int
    level: LogLevel = LogLevel.DEBUG


class ServerInfo(Entry, kw_only=True):
    node_id: str
    node_host: str
    node_port: int
    level: LogLevel = LogLevel.INFO


class ServerWarning(Entry, kw_only=True):
    node_id: str
    node_host: str
    node_port: int
    level: LogLevel = LogLevel.WARN


class ServerError(Entry, kw_only=True):
    node_id: str
    node_host: str
    node_port: int
    level: LogLevel = LogLevel.ERROR


class ServerFatal(Entry, kw_only=True):
    node_id: str
    node_host: str
    node_port: int
    level: LogLevel = LogLevel.FATAL


class StatusUpdate(Entry, kw_only=True):
    node_id: str
    node_host: str
    node_port: int
    completed_count: int
    failed_count: int
    avg_cpu: float
    avg_mem_mb: float
    level: LogLevel = LogLevel.TRACE  # TRACE level since this fires every 100ms


class SilentDropStats(Entry, kw_only=True):
    """Periodic summary of silently dropped messages for security monitoring."""

    node_id: str
    node_host: str
    node_port: int
    protocol: str  # "tcp" or "udp"
    rate_limited_count: int
    message_too_large_count: int
    decompression_too_large_count: int
    decryption_failed_count: int
    malformed_message_count: int
    replay_detected_count: int
    load_shed_count: int = (
        0  # AD-32: Messages dropped due to priority-based load shedding
    )
    # Log records lost because the logger's own write failed.
    log_write_failed_count: int
    total_dropped: int
    interval_seconds: float
    level: LogLevel = LogLevel.WARN


class IdempotencyInfo(Entry, kw_only=True):
    component: str
    idempotency_key: str | None = None
    job_id: str | None = None
    level: LogLevel = LogLevel.INFO


class IdempotencyWarning(Entry, kw_only=True):
    component: str
    idempotency_key: str | None = None
    job_id: str | None = None
    level: LogLevel = LogLevel.WARN


class IdempotencyError(Entry, kw_only=True):
    component: str
    idempotency_key: str | None = None
    job_id: str | None = None
    level: LogLevel = LogLevel.ERROR


class WALDebug(Entry, kw_only=True):
    path: str
    level: LogLevel = LogLevel.DEBUG


class WALInfo(Entry, kw_only=True):
    path: str
    level: LogLevel = LogLevel.INFO


class WALWarning(Entry, kw_only=True):
    path: str
    error_type: str | None = None
    level: LogLevel = LogLevel.WARN


class WALError(Entry, kw_only=True):
    path: str
    error_type: str
    level: LogLevel = LogLevel.ERROR


class CheckpointInfo(Entry, kw_only=True):
    path: str
    checkpoint_lsn: int
    compacted_entries: int
    active_jobs: int
    level: LogLevel = LogLevel.INFO


class CheckpointError(Entry, kw_only=True):
    path: str
    error_type: str
    pending_entries: int
    level: LogLevel = LogLevel.ERROR


class CheckpointRetentionError(Entry, kw_only=True):
    path: str
    error_type: str
    level: LogLevel = LogLevel.ERROR


class ArchiveInfo(Entry, kw_only=True):
    path: str
    job_id: str
    level: LogLevel = LogLevel.INFO


class ArchiveError(Entry, kw_only=True):
    path: str
    job_id: str
    error_type: str
    level: LogLevel = LogLevel.ERROR


class StorageFormatUnrecognized(Entry, kw_only=True):
    path: str
    set_aside_path: str
    reason: str
    level: LogLevel = LogLevel.ERROR


class SystemicEvictionHeld(Entry, kw_only=True):
    node_id: str
    held_count: int
    population: int
    level: LogLevel = LogLevel.WARN


class SystemicEvictionReleased(Entry, kw_only=True):
    node_id: str
    population: int
    level: LogLevel = LogLevel.INFO


class ClockFenced(Entry, kw_only=True):
    node_id: str
    peers_beyond: int
    peers_measured: int
    quorum: int
    hlc_lead_ms: int
    threshold_ms: int
    level: LogLevel = LogLevel.ERROR


class ClockUnfenced(Entry, kw_only=True):
    node_id: str
    peers_beyond: int
    peers_measured: int
    quorum: int
    hlc_lead_ms: int
    threshold_ms: int
    level: LogLevel = LogLevel.INFO


class ClockOffsetProbeFailed(Entry, kw_only=True):
    node_id: str
    peer_id: str
    error_type: str
    level: LogLevel = LogLevel.DEBUG


class WALTailDiscarded(Entry, kw_only=True):
    path: str
    preserved_path: str
    discarded_bytes: int
    recovered_entries: int
    level: LogLevel = LogLevel.WARN


class WALUntrustworthy(Entry, kw_only=True):
    """AD-38 Part 3.2: a job WAL is damaged at ``damage_offset`` with
    ``bytes_after_damage`` written bytes after it -- not a torn tail -- so
    the node refuses to start; the file is left as found."""

    path: str
    damage_offset: int
    bytes_after_damage: int
    recovered_entries: int
    level: LogLevel = LogLevel.CRITICAL


class WorkerStarted(Entry, kw_only=True):
    node_id: str
    node_host: str
    node_port: int
    manager_host: str | None = None
    manager_port: int | None = None
    level: LogLevel = LogLevel.INFO


class WorkerStopping(Entry, kw_only=True):
    node_id: str
    node_host: str
    node_port: int
    reason: str | None = None
    level: LogLevel = LogLevel.INFO


class WorkerJobReceived(Entry, kw_only=True):
    node_id: str
    node_host: str
    node_port: int
    job_id: str
    workflow_id: str
    source_manager_host: str
    source_manager_port: int
    level: LogLevel = LogLevel.INFO


class WorkerJobStarted(Entry, kw_only=True):
    node_id: str
    node_host: str
    node_port: int
    job_id: str
    workflow_id: str
    allocated_vus: int
    allocated_cores: int
    level: LogLevel = LogLevel.INFO


class WorkerJobCompleted(Entry, kw_only=True):
    node_id: str
    node_host: str
    node_port: int
    job_id: str
    workflow_id: str
    elapsed_seconds: float
    completed_count: int
    failed_count: int
    level: LogLevel = LogLevel.INFO


class WorkerJobFailed(Entry, kw_only=True):
    node_id: str
    node_host: str
    node_port: int
    job_id: str
    workflow_id: str
    elapsed_seconds: float
    error_message: str | None
    error_type: str | None
    level: LogLevel = LogLevel.ERROR


class WorkerActionStarted(Entry, kw_only=True):
    node_id: str
    node_host: str
    node_port: int
    job_id: str
    action_name: str
    level: LogLevel = LogLevel.TRACE


class WorkerActionCompleted(Entry, kw_only=True):
    node_id: str
    node_host: str
    node_port: int
    job_id: str
    action_name: str
    duration_ms: float
    level: LogLevel = LogLevel.TRACE


class WorkerActionFailed(Entry, kw_only=True):
    node_id: str
    node_host: str
    node_port: int
    job_id: str
    action_name: str
    error_type: str
    duration_ms: float
    level: LogLevel = LogLevel.WARN


class WorkerHealthcheckReceived(Entry, kw_only=True):
    node_id: str
    node_host: str
    node_port: int
    source_host: str
    source_port: int
    level: LogLevel = LogLevel.TRACE


class WorkerExtensionRequested(Entry, kw_only=True):
    node_id: str
    node_host: str
    node_port: int
    reason: str
    estimated_completion_seconds: float
    active_workflow_count: int
    level: LogLevel = LogLevel.DEBUG


class WorkerExtensionDecision(Entry, kw_only=True):
    node_id: str
    node_host: str
    node_port: int
    granted: bool
    extension_seconds: float
    denial_reason: str
    level: LogLevel = LogLevel.DEBUG


class WorkflowLifecycleTransitionTaken(Entry, kw_only=True):
    manager_id: str
    datacenter: str
    job_id: str
    workflow_id: str
    from_state: str
    to_state: str
    reason: str
    level: LogLevel = LogLevel.DEBUG


class WorkflowLifecycleTransitionRefused(Entry, kw_only=True):
    manager_id: str
    datacenter: str
    job_id: str
    workflow_id: str
    from_state: str
    to_state: str
    reason: str
    level: LogLevel = LogLevel.WARN


class WorkflowLifecycleCallbackFailed(Entry, kw_only=True):
    manager_id: str
    datacenter: str
    job_id: str
    workflow_id: str
    error_type: str
    level: LogLevel = LogLevel.ERROR


class ClusterMembershipEvent(Entry, kw_only=True):
    """A significant change in a cluster's membership, as one member applied
    it (AD-52 section 18): ``event`` is ``member_added`` (as a learner),
    ``member_promoted``, ``member_removed``, ``member_resumed`` (from its disk), ``leader_elected``,
    ``cluster_formed``, ``mode_changed`` or ``cohort_resized``; ``subject``
    names the member, mode or cohort. Together they let a membership
    decision be reconstructed after the fact."""

    node_id: str
    cluster_uuid: str
    event: str
    subject: str
    level: LogLevel = LogLevel.INFO


class ClusterWatchConnectivityChanged(Entry, kw_only=True):
    """AD-52 section 10: a node's watch of a cluster's membership lost the
    cluster (``disconnected``: its view grew staler than a healthy watch
    lets it, and is still served, aging) or reached it again. ``watched``
    names what is watched -- a datacenter's managers, or this node's own
    datacenter."""

    node_id: str
    watched: str
    disconnected: bool
    staleness_seconds: float
    level: LogLevel = LogLevel.WARN


class DatacenterRegenerated(Entry, kw_only=True):
    """AD-52 section 10: a datacenter's managers answer as a different
    cluster than before -- it was founded again -- so what this gate
    learned of its old incarnation (observed latency, SLO violations under
    way) is forgotten."""

    node_id: str
    datacenter_id: str
    previous_cluster_uuid: str
    cluster_uuid: str
    level: LogLevel = LogLevel.WARN


class RaftStoreOpened(Entry, kw_only=True):
    """D1: a node opened its Raft store -- resumed under the identity on
    its disk (``resumed``), or made a new one -- with how many groups it
    recovered and how many bytes of a torn last record it dropped."""

    node_id: str
    path: str
    resumed: bool
    groups_recovered: int
    torn_bytes_dropped: int
    level: LogLevel = LogLevel.INFO


class RaftStoreSetAside(Entry, kw_only=True):
    """D1: a node's Raft store could not be trusted -- ``reason`` says why
    -- and was moved to ``set_aside_path`` unread; the node starts under a
    new identity, as a new member."""

    node_id: str
    path: str
    set_aside_path: str
    reason: str
    level: LogLevel = LogLevel.ERROR


class RaftStoreCompacted(Entry, kw_only=True):
    """D1: a node's Raft store was rewritten with only what its live
    groups need."""

    node_id: str
    path: str
    bytes_reclaimed: int
    live_bytes: int
    level: LogLevel = LogLevel.DEBUG


class RaftStoreCompactionFailed(Entry, kw_only=True):
    """D1: a rewrite of a node's Raft store failed; the store is unchanged
    (the rewrite is atomic) and the next compaction retries it."""

    node_id: str
    path: str
    error: str
    level: LogLevel = LogLevel.ERROR


class ObservedLatencyRecorded(Entry, kw_only=True):
    """AD-45: a datacenter's time to accept a dispatch, folded into the
    gate's observed latency for it."""

    datacenter_id: str
    latency_ms: float
    observed_latency_ms: float
    sample_count: int
    level: LogLevel = LogLevel.DEBUG


class StaleObservationsDecayed(Entry, kw_only=True):
    """AD-45: datacenters whose observed latency decayed to no confidence
    (no sample within the staleness bound) and were forgotten -- routing
    falls back to prediction alone for them."""

    datacenter_ids: list[str]
    level: LogLevel = LogLevel.INFO


class DiscoveryDnsLookupFailed(Entry, kw_only=True):
    """AD-28: a configured discovery DNS name failed to resolve. The peers it
    answered before are kept (a DNS outage is not a departure); the lookup is
    retried on the next discovery pass."""

    dns_name: str
    node_role: str
    cluster_id: str
    error: str
    level: LogLevel = LogLevel.WARN
