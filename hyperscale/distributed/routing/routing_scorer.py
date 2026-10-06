"""
Multi-factor scoring for datacenter routing (AD-36 Part 4).
"""

from .datacenter_candidate import DatacenterCandidate
from .datacenter_routing_score import DatacenterRoutingScore
from .scoring_config import ScoringConfig


class RoutingScorer:
    """
    Scores a datacenter for a job; lower is better.

        utilization = 1 - available_cores / total_cores   (1 when unknown)
        queue       = queue_depth / (queue_depth + total_cores)
        load_factor = 1 + weighted sum of utilization, queue and
                      circuit-breaker pressure (ScoringConfig)
        score       = latency_ms * load_factor * health_severity_weight
                      * slo_routing_factor

    The queue is measured against the datacenter's own cores -- queued
    workflows per core of capacity -- so the same backlog weighs more on
    a small datacenter than a large one, with no smoothing constant.
    ``latency_ms`` comes from the latency estimator, which already folds
    coordinate quality and observed latency into one estimate.
    """

    def __init__(self, config: ScoringConfig) -> None:
        self._config = config

    def score_datacenter(
        self,
        candidate: DatacenterCandidate,
        latency_ms: float,
    ) -> DatacenterRoutingScore:
        total_cores = candidate.total_cores
        utilization = (
            1.0 - min(1.0, max(0.0, candidate.available_cores / total_cores))
            if total_cores > 0
            else 1.0
        )
        queue_and_capacity = candidate.queue_depth + total_cores
        queue = (
            candidate.queue_depth / queue_and_capacity
            if queue_and_capacity > 0
            else 0.0
        )
        load_factor = (
            1.0
            + self._config.utilization_weight * utilization
            + self._config.queue_weight * queue
            + self._config.circuit_pressure_weight
            * candidate.circuit_breaker_pressure
        )
        return DatacenterRoutingScore(
            datacenter_id=candidate.datacenter_id,
            latency_ms=latency_ms,
            load_factor=load_factor,
            health_severity_weight=candidate.health_severity_weight,
            slo_routing_factor=candidate.slo_routing_factor,
            final_score=(
                latency_ms
                * load_factor
                * candidate.health_severity_weight
                * candidate.slo_routing_factor
            ),
        )
