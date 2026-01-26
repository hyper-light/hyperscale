"""
Multi-factor scoring for datacenter routing (AD-36 Part 4).

Combines RTT UCB, load factor, and coordinate quality into a single score.
"""

from dataclasses import dataclass

from hyperscale.distributed.routing.candidate_filter import (
    DatacenterCandidate,
    ManagerCandidate,
)
from hyperscale.distributed.routing.routing_state import (
    DatacenterRoutingScore,
)


@dataclass(slots=True)
class ScoringConfig:
    """Configuration for the scoring function."""

    # Load factor weights
    a_util: float = 0.5  # Utilization weight
    a_queue: float = 0.3  # Queue depth weight
    a_cb: float = 0.2  # Circuit breaker weight
    queue_smoothing: float = 10.0
    load_factor_max: float = 5.0

    # Quality penalty weights
    a_quality: float = 0.5
    quality_penalty_max: float = 2.0

    # Preference multiplier (for preferred DCs)
    preference_multiplier: float = 0.9  # 10% bonus


class RoutingScorer:
    """
    Scores datacenter and manager candidates (AD-36 Part 4).

    Score formula:
        score = rtt_ucb_ms * load_factor * quality_penalty * preference_mult

    Lower scores are better.
    """

    def __init__(self, config: ScoringConfig | None = None) -> None:
        self._config = config or ScoringConfig()

    def score_datacenter(
        self,
        candidate: DatacenterCandidate,
        is_preferred: bool = False,
    ) -> DatacenterRoutingScore:
        """
        Score a datacenter candidate.

        Args:
            candidate: Datacenter candidate with metrics
            is_preferred: Whether this DC is in the preferred list

        Returns:
            DatacenterRoutingScore with all components
        """
        if candidate.total_cores > 0:
            utilization = 1.0 - (candidate.available_cores / candidate.total_cores)
        else:
            utilization = 1.0

        return DatacenterRoutingScore.calculate(
            datacenter_id=candidate.datacenter_id,
            health_bucket=candidate.health_bucket,
            rtt_ucb_ms=candidate.rtt_ucb_ms,
            utilization=utilization,
            queue_depth=candidate.queue_depth,
            circuit_breaker_pressure=candidate.circuit_breaker_pressure,
            coordinate_quality=candidate.coordinate_quality,
            is_preferred=is_preferred,
            preference_multiplier=self._config.preference_multiplier,
            health_severity_weight=candidate.health_severity_weight,
        )

    def score_datacenters(
        self,
        candidates: list[DatacenterCandidate],
        preferred_datacenters: set[str] | None = None,
    ) -> list[DatacenterRoutingScore]:
        """
        Score and rank datacenter candidates.

        Args:
            candidates: List of datacenter candidates
            preferred_datacenters: Set of preferred datacenter IDs

        Returns:
            List of scores sorted by score (best first)
        """
        preferred = preferred_datacenters or set()
        scores = [
            self.score_datacenter(c, c.datacenter_id in preferred) for c in candidates
        ]
        return sorted(scores, key=lambda s: s.final_score)

    def score_manager(
        self,
        candidate: ManagerCandidate,
    ) -> float:
        """
        Score a manager candidate within a datacenter.

        Uses similar formula but simpler (no bucket, no preference).

        Args:
            candidate: Manager candidate

        Returns:
            Score (lower is better)
        """
        # Calculate utilization
        if candidate.total_cores > 0:
            utilization = 1.0 - (candidate.available_cores / candidate.total_cores)
        else:
            utilization = 1.0

        # Queue factor
        queue_normalized = candidate.queue_depth / (
            candidate.queue_depth + self._config.queue_smoothing
        )

        # Circuit state penalty
        circuit_penalty = 0.0
        if candidate.circuit_state == "HALF_OPEN":
            circuit_penalty = 0.5

        # Load factor
        load_factor = (
            1.0
            + self._config.a_util * utilization
            + self._config.a_queue * queue_normalized
            + self._config.a_cb * circuit_penalty
        )
        load_factor = min(load_factor, self._config.load_factor_max)

        # Quality penalty
        quality_penalty = 1.0 + self._config.a_quality * (
            1.0 - candidate.coordinate_quality
        )
        quality_penalty = min(quality_penalty, self._config.quality_penalty_max)

        return candidate.rtt_ucb_ms * load_factor * quality_penalty

    def score_managers(
        self,
        candidates: list[ManagerCandidate],
    ) -> list[tuple[ManagerCandidate, float]]:
        """
        Score and rank manager candidates.

        Args:
            candidates: List of manager candidates

        Returns:
            List of (candidate, score) tuples sorted by score (best first)
        """
        scored = [(c, self.score_manager(c)) for c in candidates]
        return sorted(scored, key=lambda x: x[1])
