"""``KSResult`` -- pickled under the namespace
``hyperscale.distributed.health.progress_witness.kolmogorov_smirnov`` (see that module)."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class KSResult:
    """Outcome of a two-sample K-S test."""

    statistic: float  # D = sup |F_1(x) - F_2(x)|
    p_value: float    # P(D >= statistic | null) — small p ⇒ reject null
    n1: int           # First-sample size
    n2: int           # Second-sample size

    def is_stationary(self, alpha: float) -> bool:
        """Return True if we *fail* to reject the null at level
        ``alpha`` — i.e., the two samples are statistically
        indistinguishable. ``alpha`` is the deployment-policy
        false-rejection rate."""
        return self.p_value > alpha
