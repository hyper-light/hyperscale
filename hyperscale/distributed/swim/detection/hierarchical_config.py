"""``HierarchicalConfig`` -- pickled under the namespace
``hyperscale.distributed.swim.detection.hierarchical_failure_detector`` (see that module)."""

from dataclasses import dataclass


@dataclass
class HierarchicalConfig:
    """Configuration for hierarchical failure detection."""

    # Global layer config
    global_min_timeout: float = 5.0
    global_max_timeout: float = 30.0
    global_no_witness_timeout: float | None = None
    global_required_confirmations: int = 2

    # Job layer config
    job_min_timeout: float = 1.0
    job_max_timeout: float = 10.0

    # Timing wheel settings (AD-30): coarse_tick_ms=1000, fine_tick_ms=100,
    # fine_wheel_size=10. See ``TimingWheelConfig`` for the invariant.
    coarse_tick_ms: int = 1000
    fine_tick_ms: int = 100

    # Job polling settings
    poll_interval_far_ms: int = 1000
    poll_interval_near_ms: int = 50

    # Reconciliation settings
    reconciliation_interval_s: float = 5.0

    # Resource limits
    max_global_suspicions: int = 10000
    max_job_suspicions_per_job: int = 1000
    max_total_job_suspicions: int = 50000

    # AD-26: Adaptive healthcheck extension settings
    extension_base_deadline: float = 30.0
    extension_min_grant: float = 1.0
    extension_max_extensions: int = 5
    extension_warning_threshold: int = 1
    extension_grace_period: float = 10.0
    max_extension_trackers: int = 10000  # Hard cap to prevent memory exhaustion
