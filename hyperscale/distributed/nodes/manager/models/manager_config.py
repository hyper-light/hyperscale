"""
``ManagerConfig``: configuration for ManagerServer (built by
``nodes/manager/config.py`` ``create_manager_config_from_env``).
"""

from dataclasses import dataclass, field
from pathlib import Path


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

    # Workflow execution settings (a workflow's retries are bounded by its
    # job's AD-44 retry budget)
    workflow_timeout_seconds: float = 300.0

    # Dead node reaping intervals (from env)
    dead_worker_reap_interval_seconds: float = 60.0
    dead_peer_reap_interval_seconds: float = 120.0
    dead_gate_reap_interval_seconds: float = 120.0

    # Worker eviction notice re-send pacing (two-sided deregistration).
    # The base interval doubles per attempt up to the cap so a wedged
    # worker that recovers minutes later still gets told promptly-ish
    # without the manager hammering a dead address.
    eviction_notice_base_interval_seconds: float = 5.0
    eviction_notice_max_interval_seconds: float = 60.0

    # Completion-notice obligation re-send pacing (manager -> origin
    # gate). A job's durable terminal OWES its gate a JobFinalResult
    # until the gate acks: one un-retried send turned completed work
    # into a client-observed timeout whenever a partition covered the
    # completion instant. Same capped-exponential shape as eviction
    # notices; the age ceiling bounds how long an obligation to a
    # gone-forever gate is carried (the gate's own AD-34 tracker will
    # have terminal-resolved the job long before this expires — a
    # delivery after that is a no-op duplicate, not a correction).
    completion_notice_base_interval_seconds: float = 5.0
    completion_notice_max_interval_seconds: float = 60.0
    completion_notice_max_age_seconds: float = 1800.0

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
    dispatch_max_concurrent_workers: int = 16
    dispatch_routing_failure_base_cooldown_seconds: float = 0.25
    dispatch_routing_failure_max_cooldown_seconds: float = 5.0
    dispatch_routing_readiness_cooldown_seconds: float = 0.5

    # Job cleanup settings (from env)
    completed_job_max_age_seconds: float = 3600.0
    failed_job_max_age_seconds: float = 7200.0
    job_cleanup_interval_seconds: float = 60.0

    # Node check intervals (from env)
    dead_node_check_interval_seconds: float = 10.0
    rate_limit_cleanup_interval_seconds: float = 300.0

    # TCP timeout settings (from env)
    tcp_timeout_short_seconds: float = 2.0
    tcp_timeout_standard_seconds: float = 5.0

    # Batch stats push interval (from env)
    batch_push_interval_seconds: float = 1.0

    # Job responsiveness (AD-30, from env)
    job_responsiveness_threshold_seconds: float = 30.0
    job_responsiveness_check_interval_seconds: float = 5.0

    # Stats window settings (from env)
    stats_window_size_ms: int = 1000
    stats_drift_tolerance_ms: int = 100
    stats_max_window_age_ms: int = 5000

    # Stats buffer settings (AD-23, from env)
    stats_hot_max_entries: int = 10000
    stats_throttle_threshold: float = 0.7
    stats_batch_threshold: float = 0.85
    stats_reject_threshold: float = 0.95
    stats_buffer_high_watermark: int = 1000
    stats_buffer_critical_watermark: int = 5000
    stats_buffer_reject_watermark: int = 10000
    progress_normal_ratio: float = 0.8
    progress_slow_ratio: float = 0.5
    progress_degraded_ratio: float = 0.2

    # Stats push interval (from env)
    stats_push_interval_ms: int = 1000

    # Cluster identity (from env)
    cluster_id: str = "hyperscale"
    environment_id: str = "default"
    mtls_strict_mode: bool = False

    # Per-link circuit breaker (from env: CIRCUIT_BREAKER_*)
    circuit_breaker_max_errors: int = 3
    circuit_breaker_window_seconds: float = 30.0
    circuit_breaker_half_open_after_seconds: float = 10.0

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
    max_workers_per_manager: int | None = None

    # Peer sync settings (from env)
    peer_sync_interval_seconds: float = 30.0
    peer_job_sync_interval_seconds: float = 15.0

    # Throughput tracking (from env)
    throughput_interval_seconds: float = 10.0

    # Job timeout settings (AD-34)
    job_timeout_check_interval_seconds: float = 30.0

    # Aggregate health alert thresholds
    health_alert_overloaded_ratio: float = 0.5
    health_alert_non_healthy_ratio: float = 0.8

    # WAL configuration (AD-38)
    wal_data_dir: Path | None = None
