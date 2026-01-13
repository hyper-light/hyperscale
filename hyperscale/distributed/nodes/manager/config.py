"""
Manager configuration for ManagerServer.

Loads environment settings, defines constants, and provides configuration
for timeouts, intervals, retry policies, and protocol negotiation.
"""

from dataclasses import dataclass, field
from pathlib import Path

from hyperscale.distributed.env import Env


@dataclass(slots=True)
class ManagerConfig:
    """
    Configuration for ManagerServer.

    Combines environment variables, derived constants, and default settings
    for manager operation. All time values are in seconds unless noted.
    """

    # Network configuration
    host: str
    tcp_port: int
    udp_port: int
    datacenter_id: str = "default"

    # Gate configuration (optional)
    seed_gates: list[tuple[str, int]] = field(default_factory=list)
    gate_udp_addrs: list[tuple[str, int]] = field(default_factory=list)

    # Peer manager configuration
    seed_managers: list[tuple[str, int]] = field(default_factory=list)
    manager_udp_peers: list[tuple[str, int]] = field(default_factory=list)

    # Quorum settings
    quorum_timeout_seconds: float = 5.0

    # Workflow execution settings
    max_workflow_retries: int = 3
    workflow_timeout_seconds: float = 300.0

    # Dead node reaping intervals (from env)
    dead_worker_reap_interval_seconds: float = 60.0
    dead_peer_reap_interval_seconds: float = 120.0
    dead_gate_reap_interval_seconds: float = 120.0

    # Orphan scan settings (from env)
    orphan_scan_interval_seconds: float = 30.0
    orphan_scan_worker_timeout_seconds: float = 10.0

    # Cancelled workflow cleanup (from env)
    cancelled_workflow_ttl_seconds: float = 300.0
    cancelled_workflow_cleanup_interval_seconds: float = 60.0

    # Recovery settings (from env)
    recovery_max_concurrent: int = 20
    recovery_jitter_min_seconds: float = 0.1
    recovery_jitter_max_seconds: float = 1.0

    # Dispatch settings (from env)
    dispatch_max_concurrent_per_worker: int = 10

    # Job cleanup settings (from env)
    completed_job_max_age_seconds: float = 3600.0
    failed_job_max_age_seconds: float = 7200.0
    job_cleanup_interval_seconds: float = 60.0

    # Node check intervals (from env)
    dead_node_check_interval_seconds: float = 10.0
    rate_limit_cleanup_interval_seconds: float = 300.0

    # Rate limiting settings (AD-24, from env)
    rate_limit_default_max_requests: int = 100
    rate_limit_default_window_seconds: float = 10.0

    # TCP timeout settings (from env)
    tcp_timeout_short_seconds: float = 2.0
    tcp_timeout_standard_seconds: float = 5.0

    # Batch stats push interval (from env)
    batch_push_interval_seconds: float = 1.0

    # Job responsiveness (AD-30, from env)
    job_responsiveness_threshold_seconds: float = 30.0
    job_responsiveness_check_interval_seconds: float = 5.0

    # Discovery failure decay (from env)
    discovery_failure_decay_interval_seconds: float = 60.0

    # Stats window settings (from env)
    stats_window_size_ms: int = 1000
    stats_drift_tolerance_ms: int = 100
    stats_max_window_age_ms: int = 5000

    # Stats buffer settings (AD-23, from env)
    stats_hot_max_entries: int = 10000
    stats_throttle_threshold: float = 0.7
    stats_batch_threshold: float = 0.85
    stats_reject_threshold: float = 0.95

    # Stats push interval (from env)
    stats_push_interval_ms: int = 1000

    # Cluster identity (from env)
    cluster_id: str = "hyperscale"
    environment_id: str = "default"
    mtls_strict_mode: bool = False

    # State sync settings (from env)
    state_sync_retries: int = 3
    state_sync_timeout_seconds: float = 10.0

    # Leader election settings (from env)
    leader_election_jitter_max_seconds: float = 0.5
    startup_sync_delay_seconds: float = 1.0

    # Cluster stabilization (from env)
    cluster_stabilization_timeout_seconds: float = 30.0
    cluster_stabilization_poll_interval_seconds: float = 0.5

    # Heartbeat settings (from env)
    heartbeat_interval_seconds: float = 5.0
    gate_heartbeat_interval_seconds: float = 10.0

    # Peer sync settings (from env)
    peer_sync_interval_seconds: float = 30.0
    peer_job_sync_interval_seconds: float = 15.0

    # Throughput tracking (from env)
    throughput_interval_seconds: float = 10.0

    # Job timeout settings (AD-34)
    job_timeout_check_interval_seconds: float = 30.0
    job_retention_seconds: float = 3600.0

    # Aggregate health alert thresholds
    health_alert_overloaded_ratio: float = 0.5
    health_alert_non_healthy_ratio: float = 0.8

    # WAL configuration (AD-38)
    wal_data_dir: Path | None = None


def create_manager_config_from_env(
    host: str,
    tcp_port: int,
    udp_port: int,
    env: Env,
    datacenter_id: str = "default",
    seed_gates: list[tuple[str, int]] | None = None,
    gate_udp_addrs: list[tuple[str, int]] | None = None,
    seed_managers: list[tuple[str, int]] | None = None,
    manager_udp_peers: list[tuple[str, int]] | None = None,
    quorum_timeout: float = 5.0,
    max_workflow_retries: int = 3,
    workflow_timeout: float = 300.0,
    wal_data_dir: Path | None = None,
) -> ManagerConfig:
    """
    Create manager configuration from environment variables.

    Args:
        host: Manager host address
        tcp_port: Manager TCP port
        udp_port: Manager UDP port
        env: Environment configuration instance
        datacenter_id: Datacenter identifier
        seed_gates: Initial gate addresses for discovery
        gate_udp_addrs: Gate UDP addresses for SWIM
        seed_managers: Initial manager addresses for peer discovery
        manager_udp_peers: Manager UDP addresses for SWIM cluster
        quorum_timeout: Timeout for quorum operations
        max_workflow_retries: Maximum retry attempts per workflow
        workflow_timeout: Workflow execution timeout

    Returns:
        ManagerConfig instance populated from environment
    """
    return ManagerConfig(
        host=host,
        tcp_port=tcp_port,
        udp_port=udp_port,
        datacenter_id=datacenter_id,
        seed_gates=seed_gates or [],
        gate_udp_addrs=gate_udp_addrs or [],
        seed_managers=seed_managers or [],
        manager_udp_peers=manager_udp_peers or [],
        quorum_timeout_seconds=quorum_timeout,
        max_workflow_retries=max_workflow_retries,
        workflow_timeout_seconds=workflow_timeout,
        # From env
        dead_worker_reap_interval_seconds=env.MANAGER_DEAD_WORKER_REAP_INTERVAL,
        dead_peer_reap_interval_seconds=env.MANAGER_DEAD_PEER_REAP_INTERVAL,
        dead_gate_reap_interval_seconds=env.MANAGER_DEAD_GATE_REAP_INTERVAL,
        orphan_scan_interval_seconds=env.ORPHAN_SCAN_INTERVAL,
        orphan_scan_worker_timeout_seconds=env.ORPHAN_SCAN_WORKER_TIMEOUT,
        cancelled_workflow_ttl_seconds=env.CANCELLED_WORKFLOW_TTL,
        cancelled_workflow_cleanup_interval_seconds=env.CANCELLED_WORKFLOW_CLEANUP_INTERVAL,
        recovery_max_concurrent=env.RECOVERY_MAX_CONCURRENT,
        recovery_jitter_min_seconds=env.RECOVERY_JITTER_MIN,
        recovery_jitter_max_seconds=env.RECOVERY_JITTER_MAX,
        dispatch_max_concurrent_per_worker=env.DISPATCH_MAX_CONCURRENT_PER_WORKER,
        completed_job_max_age_seconds=env.COMPLETED_JOB_MAX_AGE,
        failed_job_max_age_seconds=env.FAILED_JOB_MAX_AGE,
        job_cleanup_interval_seconds=env.JOB_CLEANUP_INTERVAL,
        dead_node_check_interval_seconds=env.MANAGER_DEAD_NODE_CHECK_INTERVAL,
        rate_limit_cleanup_interval_seconds=env.MANAGER_RATE_LIMIT_CLEANUP_INTERVAL,
        rate_limit_default_max_requests=getattr(
            env, "MANAGER_RATE_LIMIT_DEFAULT_MAX_REQUESTS", 100
        ),
        rate_limit_default_window_seconds=getattr(
            env, "MANAGER_RATE_LIMIT_DEFAULT_WINDOW_SECONDS", 10.0
        ),
        tcp_timeout_short_seconds=env.MANAGER_TCP_TIMEOUT_SHORT,
        tcp_timeout_standard_seconds=env.MANAGER_TCP_TIMEOUT_STANDARD,
        batch_push_interval_seconds=env.MANAGER_BATCH_PUSH_INTERVAL,
        job_responsiveness_threshold_seconds=env.JOB_RESPONSIVENESS_THRESHOLD,
        job_responsiveness_check_interval_seconds=env.JOB_RESPONSIVENESS_CHECK_INTERVAL,
        discovery_failure_decay_interval_seconds=env.DISCOVERY_FAILURE_DECAY_INTERVAL,
        stats_window_size_ms=env.STATS_WINDOW_SIZE_MS,
        stats_drift_tolerance_ms=env.STATS_DRIFT_TOLERANCE_MS,
        stats_max_window_age_ms=env.STATS_MAX_WINDOW_AGE_MS,
        stats_hot_max_entries=env.MANAGER_STATS_HOT_MAX_ENTRIES,
        stats_throttle_threshold=env.MANAGER_STATS_THROTTLE_THRESHOLD,
        stats_batch_threshold=env.MANAGER_STATS_BATCH_THRESHOLD,
        stats_reject_threshold=env.MANAGER_STATS_REJECT_THRESHOLD,
        stats_push_interval_ms=env.STATS_PUSH_INTERVAL_MS,
        cluster_id=env.get("CLUSTER_ID", "hyperscale"),
        environment_id=env.get("ENVIRONMENT_ID", "default"),
        mtls_strict_mode=env.get("MTLS_STRICT_MODE", "false").lower() == "true",
        state_sync_retries=env.MANAGER_STATE_SYNC_RETRIES,
        state_sync_timeout_seconds=env.MANAGER_STATE_SYNC_TIMEOUT,
        leader_election_jitter_max_seconds=env.LEADER_ELECTION_JITTER_MAX,
        startup_sync_delay_seconds=env.MANAGER_STARTUP_SYNC_DELAY,
        cluster_stabilization_timeout_seconds=env.CLUSTER_STABILIZATION_TIMEOUT,
        cluster_stabilization_poll_interval_seconds=env.CLUSTER_STABILIZATION_POLL_INTERVAL,
        heartbeat_interval_seconds=env.MANAGER_HEARTBEAT_INTERVAL,
        gate_heartbeat_interval_seconds=getattr(
            env, "MANAGER_GATE_HEARTBEAT_INTERVAL", 10.0
        ),
        peer_sync_interval_seconds=env.MANAGER_PEER_SYNC_INTERVAL,
        peer_job_sync_interval_seconds=getattr(
            env, "MANAGER_PEER_JOB_SYNC_INTERVAL", 15.0
        ),
        throughput_interval_seconds=getattr(
            env, "MANAGER_THROUGHPUT_INTERVAL_SECONDS", 10.0
        ),
        job_timeout_check_interval_seconds=getattr(
            env, "JOB_TIMEOUT_CHECK_INTERVAL", 30.0
        ),
        job_retention_seconds=getattr(env, "JOB_RETENTION_SECONDS", 3600.0),
        health_alert_overloaded_ratio=env.MANAGER_HEALTH_ALERT_OVERLOADED_RATIO,
        health_alert_non_healthy_ratio=env.MANAGER_HEALTH_ALERT_NON_HEALTHY_RATIO,
        wal_data_dir=wal_data_dir,
    )
