"""
Manager configuration for ManagerServer.

Loads environment settings, defines constants, and provides configuration
for timeouts, intervals, retry policies, and protocol negotiation.
"""

from pathlib import Path

from hyperscale.distributed.env import Env
from hyperscale.distributed.nodes.manager.models.manager_config import ManagerConfig


def _addresses_or_empty(addresses: list[tuple[str, int]] | None) -> list[tuple[str, int]]:
    """The given address list, or a fresh empty one when none (or an empty one) was given."""
    return addresses or []


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
        workflow_timeout: Workflow execution timeout

    Returns:
        ManagerConfig instance populated from environment
    """
    return ManagerConfig(
        host=host,
        tcp_port=tcp_port,
        udp_port=udp_port,
        datacenter_id=datacenter_id,
        seed_gates=_addresses_or_empty(seed_gates),
        gate_udp_addrs=_addresses_or_empty(gate_udp_addrs),
        seed_managers=_addresses_or_empty(seed_managers),
        manager_udp_peers=_addresses_or_empty(manager_udp_peers),
        quorum_timeout_seconds=quorum_timeout,
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
        dispatch_max_concurrent_workers=env.DISPATCH_MAX_CONCURRENT_WORKERS,
        dispatch_routing_failure_base_cooldown_seconds=(
            env.DISPATCH_ROUTING_FAILURE_BASE_COOLDOWN
        ),
        dispatch_routing_failure_max_cooldown_seconds=(
            env.DISPATCH_ROUTING_FAILURE_MAX_COOLDOWN
        ),
        dispatch_routing_readiness_cooldown_seconds=(
            env.DISPATCH_ROUTING_READINESS_COOLDOWN
        ),
        completed_job_max_age_seconds=env.COMPLETED_JOB_MAX_AGE,
        failed_job_max_age_seconds=env.FAILED_JOB_MAX_AGE,
        job_cleanup_interval_seconds=env.JOB_CLEANUP_INTERVAL,
        dead_node_check_interval_seconds=env.MANAGER_DEAD_NODE_CHECK_INTERVAL,
        rate_limit_cleanup_interval_seconds=env.MANAGER_RATE_LIMIT_CLEANUP_INTERVAL,
        tcp_timeout_short_seconds=env.MANAGER_TCP_TIMEOUT_SHORT,
        tcp_timeout_standard_seconds=env.MANAGER_TCP_TIMEOUT_STANDARD,
        batch_push_interval_seconds=env.MANAGER_BATCH_PUSH_INTERVAL,
        job_responsiveness_threshold_seconds=env.JOB_RESPONSIVENESS_THRESHOLD,
        job_responsiveness_check_interval_seconds=env.JOB_RESPONSIVENESS_CHECK_INTERVAL,
        stats_window_size_ms=env.STATS_WINDOW_SIZE_MS,
        stats_drift_tolerance_ms=env.STATS_DRIFT_TOLERANCE_MS,
        stats_max_window_age_ms=env.STATS_MAX_WINDOW_AGE_MS,
        stats_hot_max_entries=env.MANAGER_STATS_HOT_MAX_ENTRIES,
        stats_throttle_threshold=env.MANAGER_STATS_THROTTLE_THRESHOLD,
        stats_batch_threshold=env.MANAGER_STATS_BATCH_THRESHOLD,
        stats_reject_threshold=env.MANAGER_STATS_REJECT_THRESHOLD,
        stats_buffer_high_watermark=env.MANAGER_STATS_BUFFER_HIGH_WATERMARK,
        stats_buffer_critical_watermark=env.MANAGER_STATS_BUFFER_CRITICAL_WATERMARK,
        stats_buffer_reject_watermark=env.MANAGER_STATS_BUFFER_REJECT_WATERMARK,
        progress_normal_ratio=env.MANAGER_PROGRESS_NORMAL_RATIO,
        progress_slow_ratio=env.MANAGER_PROGRESS_SLOW_RATIO,
        progress_degraded_ratio=env.MANAGER_PROGRESS_DEGRADED_RATIO,
        stats_push_interval_ms=env.STATS_PUSH_INTERVAL_MS,
        cluster_id=env.CLUSTER_ID,
        environment_id=env.ENVIRONMENT_ID,
        mtls_strict_mode=env.MTLS_STRICT_MODE.lower() == "true",
        circuit_breaker_max_errors=env.CIRCUIT_BREAKER_MAX_ERRORS,
        circuit_breaker_window_seconds=env.CIRCUIT_BREAKER_WINDOW_SECONDS,
        circuit_breaker_half_open_after_seconds=env.CIRCUIT_BREAKER_HALF_OPEN_AFTER,
        state_sync_retries=env.MANAGER_STATE_SYNC_RETRIES,
        state_sync_timeout_seconds=env.MANAGER_STATE_SYNC_TIMEOUT,
        leader_election_jitter_max_seconds=env.LEADER_ELECTION_JITTER_MAX,
        startup_sync_delay_seconds=env.MANAGER_STARTUP_SYNC_DELAY,
        cluster_stabilization_timeout_seconds=env.CLUSTER_STABILIZATION_TIMEOUT,
        cluster_stabilization_poll_interval_seconds=env.CLUSTER_STABILIZATION_POLL_INTERVAL,
        heartbeat_interval_seconds=env.MANAGER_HEARTBEAT_INTERVAL,
        max_workers_per_manager=env.MAX_WORKERS_PER_MANAGER,
        peer_sync_interval_seconds=env.MANAGER_PEER_SYNC_INTERVAL,
        peer_job_sync_interval_seconds=env.MANAGER_PEER_JOB_SYNC_INTERVAL,
        throughput_interval_seconds=env.MANAGER_THROUGHPUT_INTERVAL_SECONDS,
        job_timeout_check_interval_seconds=env.JOB_TIMEOUT_CHECK_INTERVAL,
        health_alert_overloaded_ratio=env.MANAGER_HEALTH_ALERT_OVERLOADED_RATIO,
        health_alert_non_healthy_ratio=env.MANAGER_HEALTH_ALERT_NON_HEALTHY_RATIO,
        wal_data_dir=wal_data_dir,
    )
