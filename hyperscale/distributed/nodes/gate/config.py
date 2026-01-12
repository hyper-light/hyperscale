"""
Gate configuration for GateServer.

Loads environment settings, defines constants, and provides configuration
for timeouts, intervals, retry policies, and protocol negotiation.
"""

from dataclasses import dataclass, field
from pathlib import Path


@dataclass(slots=True)
class GateConfig:
    """
    Configuration for GateServer.

    Combines environment variables, derived constants, and default settings
    for gate operation.
    """

    # Network configuration
    host: str
    tcp_port: int
    udp_port: int
    dc_id: str = "global"  # Gates typically span DCs

    # Datacenter manager addresses
    datacenter_managers: dict[str, list[tuple[str, int]]] = field(
        default_factory=dict
    )  # TCP
    datacenter_managers_udp: dict[str, list[tuple[str, int]]] = field(
        default_factory=dict
    )  # UDP for SWIM

    # Gate peer addresses
    gate_peers: list[tuple[str, int]] = field(default_factory=list)  # TCP
    gate_peers_udp: list[tuple[str, int]] = field(
        default_factory=list
    )  # UDP for SWIM cluster

    # Lease configuration
    lease_timeout_seconds: float = 30.0

    # Heartbeat/health timeouts
    heartbeat_timeout_seconds: float = 30.0
    manager_dispatch_timeout_seconds: float = 5.0
    max_retries_per_dc: int = 2

    # Rate limiting (AD-24)
    rate_limit_inactive_cleanup_seconds: float = 300.0

    # Latency tracking
    latency_sample_max_age_seconds: float = 60.0
    latency_sample_max_count: int = 30

    # Throughput tracking (AD-19)
    throughput_interval_seconds: float = 10.0

    # Orphan job tracking
    orphan_grace_period_seconds: float = 120.0
    orphan_check_interval_seconds: float = 30.0

    # Timeout tracking (AD-34)
    timeout_check_interval_seconds: float = 15.0
    all_dc_stuck_threshold_seconds: float = 180.0

    # Job hash ring configuration
    hash_ring_replicas: int = 150

    # Job forwarding configuration
    forward_timeout_seconds: float = 3.0
    max_forward_attempts: int = 3

    # Stats window configuration
    stats_window_size_ms: float = 1000.0
    stats_drift_tolerance_ms: float = 100.0
    stats_max_window_age_ms: float = 5000.0
    stats_push_interval_ms: float = 1000.0

    # Job lease configuration
    job_lease_duration_seconds: float = 300.0
    job_lease_cleanup_interval_seconds: float = 60.0

    # Recovery configuration
    recovery_max_concurrent: int = 3

    # Circuit breaker configuration
    circuit_breaker_max_errors: int = 5
    circuit_breaker_window_seconds: float = 30.0
    circuit_breaker_half_open_after_seconds: float = 10.0

    # Job ledger configuration (AD-38)
    ledger_data_dir: Path | None = None


def create_gate_config(
    host: str,
    tcp_port: int,
    udp_port: int,
    dc_id: str = "global",
    datacenter_managers: dict[str, list[tuple[str, int]]] | None = None,
    datacenter_managers_udp: dict[str, list[tuple[str, int]]] | None = None,
    gate_peers: list[tuple[str, int]] | None = None,
    gate_peers_udp: list[tuple[str, int]] | None = None,
    lease_timeout: float = 30.0,
) -> GateConfig:
    """
    Create gate configuration with defaults.

    Args:
        host: Gate host address
        tcp_port: Gate TCP port
        udp_port: Gate UDP port for SWIM
        dc_id: Datacenter identifier (default "global" for gates spanning DCs)
        datacenter_managers: DC -> manager TCP addresses mapping
        datacenter_managers_udp: DC -> manager UDP addresses mapping
        gate_peers: List of peer gate TCP addresses
        gate_peers_udp: List of peer gate UDP addresses
        lease_timeout: Lease timeout in seconds

    Returns:
        GateConfig instance
    """
    return GateConfig(
        host=host,
        tcp_port=tcp_port,
        udp_port=udp_port,
        dc_id=dc_id,
        datacenter_managers=datacenter_managers or {},
        datacenter_managers_udp=datacenter_managers_udp or {},
        gate_peers=gate_peers or [],
        gate_peers_udp=gate_peers_udp or [],
        lease_timeout_seconds=lease_timeout,
    )
