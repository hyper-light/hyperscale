"""
``WorkerConfig``: configuration for WorkerServer (built by
``nodes/worker/config.py`` ``create_worker_config_from_env``).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from hyperscale.distributed.env import Env
from hyperscale.distributed.nodes.worker.worker_config_derivation import (
    _default_env_value,
    _get_os_cpus,
    _resolve_total_cores,
    derive_orphan_grace_seconds,
)


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
    progress_send_timeout_seconds: float = 1.0
    heartbeat_send_timeout_seconds: float = 1.0

    # Workflow execution and cancellation
    execution_update_wait_seconds: float = 0.5
    workflow_cancel_timeout_seconds: float = 5.0

    # Final results awaiting a job leader's acknowledgement
    pending_result_limit: int = 1000
    result_max_retries: int = 10
    result_retry_base_delay_seconds: float = 5.0
    result_retry_max_delay_seconds: float = 60.0

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
    orphan_extension_min_grant_seconds: float = 1.0
    orphan_extension_max_extensions: int = 5
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
    registration_max_retries: int = field(
        default_factory=lambda: _default_env_value("WORKER_REGISTRATION_MAX_RETRIES")
    )
    registration_base_delay_seconds: float = field(
        default_factory=lambda: _default_env_value("WORKER_REGISTRATION_BASE_DELAY")
    )
    initial_registration_jitter_max_seconds: float = field(
        default_factory=lambda: _default_env_value(
            "WORKER_INITIAL_REGISTRATION_JITTER_MAX"
        )
    )
    manager_rejoin_watch_interval_seconds: float = field(
        default_factory=lambda: _default_env_value(
            "WORKER_MANAGER_REJOIN_WATCH_INTERVAL_SECONDS"
        )
    )

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
        total_cores: int | None = None,
    ) -> WorkerConfig:
        """
        Create worker configuration from Env object.

        Args:
            env: Env configuration object
            host: Worker host address
            tcp_port: Worker TCP port
            udp_port: Worker UDP port
            datacenter_id: Datacenter identifier
            total_cores: Explicit executor core count for this worker.
                ``None`` defers to ``WORKER_MAX_CORES`` and then to the
                machine's physical core count.

        Returns:
            WorkerConfig instance

        Raises:
            ValueError: ``total_cores`` is explicitly less than one — a
                worker with no executor slots can never be dispatched to.
        """
        return cls(
            host=host,
            tcp_port=tcp_port,
            udp_port=udp_port,
            datacenter_id=datacenter_id,
            total_cores=_resolve_total_cores(env, total_cores),
            tcp_timeout_short_seconds=env.WORKER_TCP_TIMEOUT_SHORT,
            tcp_timeout_standard_seconds=env.WORKER_TCP_TIMEOUT_STANDARD,
            progress_send_timeout_seconds=env.WORKER_PROGRESS_SEND_TIMEOUT,
            heartbeat_send_timeout_seconds=env.WORKER_HEARTBEAT_SEND_TIMEOUT,
            execution_update_wait_seconds=env.WORKER_EXECUTION_UPDATE_WAIT,
            workflow_cancel_timeout_seconds=env.WORKER_WORKFLOW_CANCEL_TIMEOUT,
            pending_result_limit=env.WORKER_PENDING_RESULT_LIMIT,
            result_max_retries=env.WORKER_RESULT_MAX_RETRIES,
            result_retry_base_delay_seconds=env.WORKER_RESULT_RETRY_BASE_DELAY,
            # A result never waits between attempts longer than the silence
            # after which its manager suspects this worker of the job (AD-30).
            result_retry_max_delay_seconds=env.JOB_RESPONSIVENESS_THRESHOLD,
            discovery_probe_interval_seconds=env.DISCOVERY_PROBE_INTERVAL,
            discovery_failure_decay_interval_seconds=env.DISCOVERY_FAILURE_DECAY_INTERVAL,
            completion_times_max_samples=env.WORKER_COMPLETION_TIMES_MAX_SAMPLES,
            dead_manager_reap_interval_seconds=env.WORKER_DEAD_MANAGER_REAP_INTERVAL,
            dead_manager_check_interval_seconds=env.WORKER_DEAD_MANAGER_CHECK_INTERVAL,
            progress_update_interval_seconds=env.WORKER_PROGRESS_UPDATE_INTERVAL,
            progress_flush_interval_seconds=env.WORKER_PROGRESS_FLUSH_INTERVAL,
            cancellation_poll_interval_seconds=env.WORKER_CANCELLATION_POLL_INTERVAL,
            orphan_grace_period_seconds=derive_orphan_grace_seconds(env),
            orphan_extension_min_grant_seconds=env.EXTENSION_MIN_GRANT,
            orphan_extension_max_extensions=env.EXTENSION_MAX_EXTENSIONS,
            orphan_check_interval_seconds=env.WORKER_ORPHAN_CHECK_INTERVAL,
            pending_transfer_ttl_seconds=env.WORKER_PENDING_TRANSFER_TTL,
            overload_poll_interval_seconds=env.WORKER_OVERLOAD_POLL_INTERVAL,
            throughput_interval_seconds=env.WORKER_THROUGHPUT_INTERVAL_SECONDS,
            recovery_jitter_min_seconds=env.RECOVERY_JITTER_MIN,
            recovery_jitter_max_seconds=env.RECOVERY_JITTER_MAX,
            recovery_semaphore_size=env.RECOVERY_SEMAPHORE_SIZE,
            registration_max_retries=env.WORKER_REGISTRATION_MAX_RETRIES,
            registration_base_delay_seconds=env.WORKER_REGISTRATION_BASE_DELAY,
            initial_registration_jitter_max_seconds=env.WORKER_INITIAL_REGISTRATION_JITTER_MAX,
            manager_rejoin_watch_interval_seconds=env.WORKER_MANAGER_REJOIN_WATCH_INTERVAL_SECONDS,
        )
