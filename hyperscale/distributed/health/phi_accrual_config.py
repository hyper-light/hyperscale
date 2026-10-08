from dataclasses import dataclass

from hyperscale.distributed.env import Env


@dataclass(slots=True, frozen=True)
class PhiAccrualConfig:
    """How a phi-accrual failure detector judges one edge's heartbeats
    (AD-52 section 8): suspected once phi reaches ``threshold``; phi is
    computed over the last ``max_sample_size`` inter-arrival times, their
    deviation never taken below ``min_std_deviation_seconds``, a pause of
    ``acceptable_heartbeat_pause_seconds`` added to the expected interval,
    and -- before any interval is seen -- that interval estimated as
    ``first_heartbeat_estimate_seconds``."""

    threshold: float
    max_sample_size: int
    min_std_deviation_seconds: float
    acceptable_heartbeat_pause_seconds: float
    first_heartbeat_estimate_seconds: float

    @classmethod
    def for_manager_heartbeats(cls, env: Env) -> "PhiAccrualConfig":
        """The gate's detector of each datacenter manager's heartbeats: the
        first interval estimated as the managers' configured heartbeat
        interval."""
        return cls(
            threshold=env.PHI_ACCRUAL_THRESHOLD,
            max_sample_size=env.PHI_ACCRUAL_MAX_SAMPLE_SIZE,
            min_std_deviation_seconds=env.PHI_ACCRUAL_MIN_STD_DEVIATION_SECONDS,
            acceptable_heartbeat_pause_seconds=env.PHI_ACCRUAL_ACCEPTABLE_HEARTBEAT_PAUSE_SECONDS,
            first_heartbeat_estimate_seconds=env.MANAGER_HEARTBEAT_INTERVAL,
        )
