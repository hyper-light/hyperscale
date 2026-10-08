"""Wire model ``VivaldiConfig`` -- pickled under the wire namespace
``hyperscale.distributed.models.coordinates`` (see that module)."""

from dataclasses import dataclass


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
    # A coordinate's error is the moving average of its absolute prediction
    # error, in seconds (AD-35 Part 7 reads it as ``error_ms``). No floor:
    # error tracks real misprediction down to zero, and the UCB's sigma is
    # bounded below by ``sigma_min_ms`` instead (a 0.05 floor here, a
    # relative-error constant read as seconds, held every UCB 200ms high).
    max_error: float = 10.0  # Maximum error bound (seconds)

    # RTT UCB parameters (AD-35/AD-36)
    k_sigma: float = 2.0  # UCB multiplier for error margin
    sigma_min_ms: float = 1.0  # Minimum sigma bound
    sigma_max_ms: float = 500.0  # Maximum sigma bound
    rtt_min_ms: float = 1.0  # Minimum RTT estimate
    rtt_max_ms: float = 10000.0  # Maximum RTT estimate (10 seconds)

    # Coordinate quality parameters. A coordinate has converged -- its
    # distances are evidence (AD-36 Part 6) -- once it has the samples and
    # the error that earn it full quality.
    min_samples_for_routing: int = 10  # Minimum samples for quality = 1.0
    error_good_ms: float = 20.0  # Error threshold for quality = 1.0
    coord_ttl_seconds: float = 300.0  # Coordinate staleness TTL
