"""``RunLengthPosterior`` -- pickled under the namespace
``hyperscale.distributed.health.progress_witness.bocpd`` (see that module)."""

from __future__ import annotations

from dataclasses import dataclass, field

from .bocpd_shared import _predictive_params_from_suffstats
from .bocpd_config import BOCPDConfig
from ._run_suff_stats import _RunSuffStats


@dataclass(slots=True)
class RunLengthPosterior:
    """The current posterior over run lengths and the per-run suffstats.

    Kept as parallel arrays where ``probabilities[i]`` is
    ``P(r_t = i | x_1:t)`` and ``suffstats[i]`` are the Welford
    statistics for that run.
    """

    probabilities: list[float] = field(default_factory=lambda: [1.0])
    suffstats: list[_RunSuffStats] = field(default_factory=lambda: [_RunSuffStats()])

    def change_point_probability(self) -> float:
        """``P(r_t = 0 | x_1:t)`` — probability that the most recent
        observation triggered a change-point."""
        if not self.probabilities:
            return 0.0
        return self.probabilities[0]

    def maximum_a_posteriori_run_length(self) -> int:
        """The run length with highest posterior mass.

        Useful when downstream consumers want to know "how long has
        the current regime been stable" rather than "did anything
        just change."
        """
        if not self.probabilities:
            return 0
        # ``max`` keeps the first index on ties and replaces only on a
        # strictly greater probability, as the former scan did.
        return max(range(len(self.probabilities)), key=self.probabilities.__getitem__)

    def expected_predictive_mean(
        self, config: BOCPDConfig, mu_0: float
    ) -> float:
        """Posterior-mean prediction of the next observation, marginalised
        over run-length uncertainty.

        Used by the throughput witness to compare "where we expect
        throughput to be" against "what we just observed" — the sign
        of the difference plus the change-point probability gives
        the directional verdict.
        """
        total = 0.0
        for prob, stats in zip(self.probabilities, self.suffstats):
            _, location, _ = _predictive_params_from_suffstats(config, stats, mu_0)
            total += prob * location
        return total
