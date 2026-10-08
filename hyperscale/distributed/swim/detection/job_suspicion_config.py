"""``JobSuspicionConfig`` -- pickled under the namespace
``hyperscale.distributed.swim.detection.job_suspicion_manager`` (see that module)."""

from dataclasses import dataclass


@dataclass
class JobSuspicionConfig:
    """Configuration for job-layer suspicion management."""

    # Adaptive polling intervals (ms)
    poll_interval_far_ms: int = 1000  # > 5s remaining
    poll_interval_medium_ms: int = 250  # 1-5s remaining
    poll_interval_near_ms: int = 50  # < 1s remaining

    # Thresholds for interval selection (seconds)
    far_threshold_s: float = 5.0
    near_threshold_s: float = 1.0

    # LHM integration
    max_lhm_backoff_multiplier: float = 3.0  # Max slowdown under load

    # Resource limits
    max_suspicions_per_job: int = 1000
    max_total_suspicions: int = 10000
