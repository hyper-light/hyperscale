"""
Worker configuration for WorkerServer.

Loads environment settings, defines constants, and provides configuration
for timeouts, intervals, retry policies, and health monitoring.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from hyperscale.distributed.env import Env


def _get_os_cpus() -> int:
    """Get OS CPU count."""
    try:
        import psutil

        return psutil.cpu_count(logical=False) or os.cpu_count() or 1
    except ImportError:
        return os.cpu_count() or 1


@dataclass(slots=True)
class WorkerConfig:
    """
    Configuration for WorkerServer.

    Combines environment variables, derived constants, and default settings
    for worker operation.
    """

    # Network configuration
    host: str
    tcp_port: int
    udp_port: int
    datacenter_id: str = "default"

    # Core allocation
    total_cores: int = field(default_factory=_get_os_cpus)
    max_workflow_cores: int | None = None

    # Manager communication timeouts
    tcp_timeout_short_seconds: float = 2.0
    tcp_timeout_standard_seconds: float = 5.0

    # Dead manager tracking
    dead_manager_reap_interval_seconds: float = 60.0
    dead_manager_check_interval_seconds: float = 10.0

    # Discovery settings (AD-28)
    discovery_probe_interval_seconds: float = 30.0
    discovery_failure_decay_interval_seconds: float = 60.0

    # Progress update settings
    progress_update_interval_seconds: float = 1.0
    progress_flush_interval_seconds: float = 0.5

    # Cancellation polling
    cancellation_poll_interval_seconds: float = 5.0

    # Orphan workflow handling (Section 2.7)
    orphan_grace_period_seconds: float = 120.0
    orphan_check_interval_seconds: float = 10.0

    # Pending transfer TTL (Section 8.3)
    pending_transfer_ttl_seconds: float = 60.0

    # Overload detection (AD-18)
    overload_poll_interval_seconds: float = 0.25

    # Throughput tracking (AD-19)
    throughput_interval_seconds: float = 10.0
    completion_times_max_samples: int = 50

    # Recovery coordination
    recovery_jitter_min_seconds: float = 0.0
    recovery_jitter_max_seconds: float = 1.0
    recovery_semaphore_size: int = 5

    # Registration
    registration_max_retries: int = 3
    registration_base_delay_seconds: float = 0.5

    # Event log configuration (AD-47)
    event_log_dir: Path | None = None

    @property
    def progress_update_interval(self) -> float:
        """Alias for progress_update_interval_seconds."""
        return self.progress_update_interval_seconds

    @property
    def progress_flush_interval(self) -> float:
        """Alias for progress_flush_interval_seconds."""
        return self.progress_flush_interval_seconds

    @classmethod
    def from_env(
        cls,
        env: Env,
        host: str,
        tcp_port: int,
        udp_port: int,
        datacenter_id: str = "default",
    ) -> WorkerConfig:
        """
        Create worker configuration from Env object.

        Args:
            env: Env configuration object
            host: Worker host address
            tcp_port: Worker TCP port
            udp_port: Worker UDP port
            datacenter_id: Datacenter identifier

        Returns:
            WorkerConfig instance
        """
        total_cores = getattr(env, "WORKER_MAX_CORES", None)
        if not total_cores:
            total_cores = _get_os_cpus()

        return cls(
            host=host,
            tcp_port=tcp_port,
            udp_port=udp_port,
            datacenter_id=datacenter_id,
            total_cores=total_cores,
            tcp_timeout_short_seconds=getattr(env, "WORKER_TCP_TIMEOUT_SHORT", 2.0),
            tcp_timeout_standard_seconds=getattr(
                env, "WORKER_TCP_TIMEOUT_STANDARD", 5.0
            ),
            dead_manager_reap_interval_seconds=getattr(
                env, "WORKER_DEAD_MANAGER_REAP_INTERVAL", 60.0
            ),
            dead_manager_check_interval_seconds=getattr(
                env, "WORKER_DEAD_MANAGER_CHECK_INTERVAL", 10.0
            ),
            progress_update_interval_seconds=getattr(
                env, "WORKER_PROGRESS_UPDATE_INTERVAL", 1.0
            ),
            progress_flush_interval_seconds=getattr(
                env, "WORKER_PROGRESS_FLUSH_INTERVAL", 0.5
            ),
            cancellation_poll_interval_seconds=getattr(
                env, "WORKER_CANCELLATION_POLL_INTERVAL", 5.0
            ),
            orphan_grace_period_seconds=getattr(
                env, "WORKER_ORPHAN_GRACE_PERIOD", 120.0
            ),
            orphan_check_interval_seconds=getattr(
                env, "WORKER_ORPHAN_CHECK_INTERVAL", 10.0
            ),
            pending_transfer_ttl_seconds=getattr(
                env, "WORKER_PENDING_TRANSFER_TTL", 60.0
            ),
            overload_poll_interval_seconds=getattr(
                env, "WORKER_OVERLOAD_POLL_INTERVAL", 0.25
            ),
            throughput_interval_seconds=getattr(
                env, "WORKER_THROUGHPUT_INTERVAL_SECONDS", 10.0
            ),
            recovery_jitter_min_seconds=getattr(env, "RECOVERY_JITTER_MIN", 0.0),
            recovery_jitter_max_seconds=getattr(env, "RECOVERY_JITTER_MAX", 1.0),
            recovery_semaphore_size=getattr(env, "RECOVERY_SEMAPHORE_SIZE", 5),
        )


def create_worker_config_from_env(
    host: str,
    tcp_port: int,
    udp_port: int,
    datacenter_id: str = "default",
    seed_managers: list[tuple[str, int]] | None = None,
) -> WorkerConfig:
    """
    Create worker configuration from environment variables.

    Reads environment variables with WORKER_ prefix for configuration.

    Args:
        host: Worker host address
        tcp_port: Worker TCP port
        udp_port: Worker UDP port
        datacenter_id: Datacenter identifier
        seed_managers: Initial list of manager addresses

    Returns:
        WorkerConfig instance
    """
    total_cores = int(os.getenv("WORKER_MAX_CORES", "0"))
    if not total_cores:
        total_cores = _get_os_cpus()

    return WorkerConfig(
        host=host,
        tcp_port=tcp_port,
        udp_port=udp_port,
        datacenter_id=datacenter_id,
        total_cores=total_cores,
        tcp_timeout_short_seconds=float(os.getenv("WORKER_TCP_TIMEOUT_SHORT", "2.0")),
        tcp_timeout_standard_seconds=float(
            os.getenv("WORKER_TCP_TIMEOUT_STANDARD", "5.0")
        ),
        dead_manager_reap_interval_seconds=float(
            os.getenv("WORKER_DEAD_MANAGER_REAP_INTERVAL", "60.0")
        ),
        dead_manager_check_interval_seconds=float(
            os.getenv("WORKER_DEAD_MANAGER_CHECK_INTERVAL", "10.0")
        ),
        progress_update_interval_seconds=float(
            os.getenv("WORKER_PROGRESS_UPDATE_INTERVAL", "1.0")
        ),
        progress_flush_interval_seconds=float(
            os.getenv("WORKER_PROGRESS_FLUSH_INTERVAL", "0.5")
        ),
        cancellation_poll_interval_seconds=float(
            os.getenv("WORKER_CANCELLATION_POLL_INTERVAL", "5.0")
        ),
        orphan_grace_period_seconds=float(
            os.getenv("WORKER_ORPHAN_GRACE_PERIOD", "120.0")
        ),
        orphan_check_interval_seconds=float(
            os.getenv("WORKER_ORPHAN_CHECK_INTERVAL", "10.0")
        ),
        pending_transfer_ttl_seconds=float(
            os.getenv("WORKER_PENDING_TRANSFER_TTL", "60.0")
        ),
        overload_poll_interval_seconds=float(
            os.getenv("WORKER_OVERLOAD_POLL_INTERVAL", "0.25")
        ),
        throughput_interval_seconds=float(
            os.getenv("WORKER_THROUGHPUT_INTERVAL_SECONDS", "10.0")
        ),
    )
