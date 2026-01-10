from dataclasses import dataclass, field
import time


@dataclass(slots=True)
class VivaldiConfig:
    """
    Configuration for Vivaldi coordinate system (AD-35 Part 12.1.7).

    Provides tuning parameters for coordinate updates, RTT estimation,
    and quality assessment.
    """
    # Coordinate dimensions
    dimensions: int = 8

    # Update algorithm parameters
    ce: float = 0.25  # Learning rate for coordinate updates
    error_decay: float = 0.25  # Error decay rate
    gravity: float = 0.01  # Centering gravity
    height_adjustment: float = 0.25  # Height update rate
    adjustment_smoothing: float = 0.05  # Adjustment smoothing factor
    min_error: float = 0.05  # Minimum error bound
    max_error: float = 10.0  # Maximum error bound

    # RTT UCB parameters (AD-35/AD-36)
    k_sigma: float = 2.0  # UCB multiplier for error margin
    rtt_default_ms: float = 100.0  # Default RTT when coordinate unavailable
    sigma_default_ms: float = 50.0  # Default sigma when coordinate unavailable
    sigma_min_ms: float = 1.0  # Minimum sigma bound
    sigma_max_ms: float = 500.0  # Maximum sigma bound
    rtt_min_ms: float = 1.0  # Minimum RTT estimate
    rtt_max_ms: float = 10000.0  # Maximum RTT estimate (10 seconds)

    # Coordinate quality parameters
    min_samples_for_routing: int = 10  # Minimum samples for quality = 1.0
    error_good_ms: float = 20.0  # Error threshold for quality = 1.0
    coord_ttl_seconds: float = 300.0  # Coordinate staleness TTL

    # Convergence thresholds
    convergence_error_threshold: float = 0.5  # Error below which considered converged
    convergence_min_samples: int = 10  # Minimum samples for convergence


@dataclass(slots=True)
class NetworkCoordinate:
    """Network coordinate for RTT estimation."""

    vec: list[float]
    height: float
    adjustment: float
    error: float
    updated_at: float = field(default_factory=time.monotonic)
    sample_count: int = 0
