"""``_RunSuffStats`` -- pickled under the namespace
``hyperscale.distributed.health.progress_witness.bocpd`` (see that module)."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class _RunSuffStats:
    """Welford-style sufficient statistics for one run length.

    Stores the parameters of the per-run Normal-inverse-Gamma
    posterior, derivable from observation count, sample mean, and
    sample SSE. Updated online via Welford's algorithm so we never
    accumulate raw observations.
    """

    n: int = 0
    mean: float = 0.0
    m2: float = 0.0  # sum of squared deviations from running mean

    def update(self, x: float) -> "_RunSuffStats":
        """Welford increment. Returns a new ``_RunSuffStats``."""
        new_n = self.n + 1
        delta = x - self.mean
        new_mean = self.mean + delta / new_n
        delta2 = x - new_mean
        new_m2 = self.m2 + delta * delta2
        return _RunSuffStats(n=new_n, mean=new_mean, m2=new_m2)
