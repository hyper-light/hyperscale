"""
Bootstrap mode for routing when coordinates are immature (AD-36 Part 6).

When local coordinates haven't converged, use coordinate-unaware mode
that ranks by capacity, queue depth, and circuit pressure.
"""

from dataclasses import dataclass

from hyperscale.distributed.routing.candidate_filter import DatacenterCandidate


@dataclass(slots=True)
class BootstrapConfig:
    """Configuration for bootstrap mode."""

    # Thresholds for exiting bootstrap mode
    min_samples_for_routing: int = 10
    max_error_for_routing: float = 0.5  # Coordinate error threshold

    # Conservative defaults in bootstrap mode
    default_rtt_ms: float = 100.0


class BootstrapModeManager:
    """
    Manages coordinate-unaware bootstrap mode (AD-36 Part 6).

    When coordinates are immature:
    - Enter coordinate-unaware mode
    - Rank by capacity, queue depth, circuit pressure
    - Use conservative RTT defaults

    Exit when:
    - sample_count >= MIN_SAMPLES_FOR_ROUTING
    - error <= MAX_ERROR_FOR_ROUTING
    """

    def __init__(self, config: BootstrapConfig | None = None) -> None:
        self._config = config or BootstrapConfig()

    def is_in_bootstrap_mode(
        self,
        local_sample_count: int,
        local_error: float,
    ) -> bool:
        """
        Check if we should be in coordinate-unaware mode.

        Args:
            local_sample_count: Number of samples in local coordinate
            local_error: Local coordinate error

        Returns:
            True if should use bootstrap (coordinate-unaware) mode
        """
        has_enough_samples = local_sample_count >= self._config.min_samples_for_routing
        error_is_acceptable = local_error <= self._config.max_error_for_routing

        return not (has_enough_samples and error_is_acceptable)

    def rank_by_capacity(
        self,
        candidates: list[DatacenterCandidate],
    ) -> list[DatacenterCandidate]:
        """
        Rank candidates by capacity when coordinates unavailable.

        Ranking factors (in order):
        1. Available capacity (higher is better)
        2. Queue depth (lower is better)
        3. Circuit breaker pressure (lower is better)

        Args:
            candidates: List of datacenter candidates

        Returns:
            Candidates sorted by capacity-based ranking (best first)
        """

        def capacity_score(candidate: DatacenterCandidate) -> tuple[float, float, float]:
            # Higher capacity = lower score (negated for sorting)
            capacity_ratio = (
                candidate.available_cores / max(candidate.total_cores, 1)
                if candidate.total_cores > 0
                else 0.0
            )
            capacity_score = -capacity_ratio  # Negate for ascending sort

            # Lower queue depth = lower score
            queue_score = candidate.queue_depth / (candidate.queue_depth + 10.0)

            # Lower circuit pressure = lower score
            circuit_score = candidate.circuit_breaker_pressure

            return (capacity_score, queue_score, circuit_score)

        return sorted(candidates, key=capacity_score)

    def apply_default_rtt(
        self,
        candidates: list[DatacenterCandidate],
    ) -> None:
        """
        Apply conservative default RTT to candidates missing coordinates.

        Modifies candidates in place.
        """
        for candidate in candidates:
            if not candidate.has_coordinate:
                candidate.rtt_ucb_ms = self._config.default_rtt_ms
                candidate.coordinate_quality = 0.0

    def get_bootstrap_status(
        self,
        local_sample_count: int,
        local_error: float,
    ) -> dict:
        """Get bootstrap mode status for observability."""
        is_bootstrap = self.is_in_bootstrap_mode(local_sample_count, local_error)

        samples_needed = max(
            0, self._config.min_samples_for_routing - local_sample_count
        )
        error_improvement_needed = max(
            0.0, local_error - self._config.max_error_for_routing
        )

        return {
            "in_bootstrap_mode": is_bootstrap,
            "local_sample_count": local_sample_count,
            "local_error": local_error,
            "samples_needed": samples_needed,
            "error_improvement_needed": error_improvement_needed,
            "thresholds": {
                "min_samples": self._config.min_samples_for_routing,
                "max_error": self._config.max_error_for_routing,
            },
        }
