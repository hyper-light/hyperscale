"""
Weights of the routing load factor (AD-36 Part 4).
"""

from __future__ import annotations

from dataclasses import dataclass

from hyperscale.distributed.env.env import Env


@dataclass(slots=True, frozen=True)
class ScoringConfig:
    """
    ``load_factor = 1 + utilization_weight * utilization + queue_weight *
    queue + circuit_pressure_weight * circuit_pressure``. Each signal lies
    in [0, 1], so the factor lies in [1, 1 + the weights' sum] without a
    cap.
    """

    utilization_weight: float
    queue_weight: float
    circuit_pressure_weight: float

    @classmethod
    def from_env(cls, env: Env) -> ScoringConfig:
        """
        Create a configuration instance from environment settings.
        """
        return cls(
            utilization_weight=env.ROUTING_UTILIZATION_WEIGHT,
            queue_weight=env.ROUTING_QUEUE_WEIGHT,
            circuit_pressure_weight=env.ROUTING_CIRCUIT_PRESSURE_WEIGHT,
        )
