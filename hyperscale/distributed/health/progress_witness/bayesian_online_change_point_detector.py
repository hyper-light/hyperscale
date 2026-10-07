"""``BayesianOnlineChangePointDetector`` -- pickled under the namespace
``hyperscale.distributed.health.progress_witness.bocpd`` (see that module)."""

from __future__ import annotations

import math
from functools import partial
from operator import lt
from typing import Sequence

from .bocpd_shared import _predictive_params_from_suffstats
from .bocpd_config import BOCPDConfig
from .run_length_posterior import RunLengthPosterior
from ._run_suff_stats import _RunSuffStats


# ``-inf < value``: the entries that contribute to a logsumexp
# (``exp(-inf)`` is 0), tested in C rather than a Python frame per entry.
_ABOVE_NEGATIVE_INFINITY = partial(lt, -math.inf)


def _student_t_log_pdf(
    x: float,
    df: float,
    location: float,
    scale: float,
) -> float:
    """Student-t log probability density at ``x``.

    Closed form:
        log f(x) = lgamma((df+1)/2) - lgamma(df/2)
                   - 0.5 * log(pi * df) - log(scale)
                   - ((df+1)/2) * log(1 + ((x - location)/scale)^2 / df)
    """
    if scale <= 0.0:
        return -math.inf
    z = (x - location) / scale
    log_norm = (
        math.lgamma((df + 1.0) / 2.0)
        - math.lgamma(df / 2.0)
        - 0.5 * math.log(math.pi * df)
        - math.log(scale)
    )
    return log_norm - ((df + 1.0) / 2.0) * math.log(1.0 + (z * z) / df)


class BayesianOnlineChangePointDetector:
    """Online BOCPD over a single observation stream.

    Usage:
        detector = BayesianOnlineChangePointDetector(BOCPDConfig())
        for x in observations:
            posterior = detector.observe(x)
            if posterior.change_point_probability() > alpha_workflow:
                ...

    Thread-safety: NOT thread-safe. The Phase H6 ``ThroughputWitness``
    uses one detector per ``(worker, workflow_id)`` and serialises
    access through the manager's existing per-job lock.
    """

    def __init__(self, config: BOCPDConfig | None = None) -> None:
        self._config: BOCPDConfig = config if config is not None else BOCPDConfig()
        self._posterior: RunLengthPosterior = RunLengthPosterior()
        # Initialise mu_0 lazily on the first observation so the prior
        # never starts catastrophically far from the data — see
        # ``BOCPDConfig.mu_0`` docstring.
        self._mu_0: float | None = None
        self._observation_count: int = 0

    @property
    def posterior(self) -> RunLengthPosterior:
        return self._posterior

    @property
    def observation_count(self) -> int:
        return self._observation_count

    def reset(self) -> None:
        """Discard all run-length state. Used after the witness
        formally accepts a regime change so the new run starts fresh.
        """
        self._posterior = RunLengthPosterior()
        self._mu_0 = None
        self._observation_count = 0

    def observe(self, x: float) -> RunLengthPosterior:
        """Process one observation. Returns the updated posterior.

        Implements Algorithm 1 from Adams & MacKay 2007, with the
        constant-hazard simplification ``H(r) = 1 / hazard_lambda``.
        """
        if self._mu_0 is None:
            self._mu_0 = x
        old_probs = self._posterior.probabilities
        old_stats = self._posterior.suffstats

        log_predictive, new_stats_grow = self._predict_and_grow_runs(x, old_stats)
        log_growth, log_change_total = self._log_growth_and_change(old_probs, log_predictive)
        new_log_unnormalised, new_stats_full = self._assemble_truncated_posterior(
            x, log_change_total, log_growth, new_stats_grow
        )

        # Step 7: normalise into a probability distribution
        log_norm = _logsumexp(new_log_unnormalised)
        new_probs = [math.exp(lp - log_norm) for lp in new_log_unnormalised]

        self._posterior = RunLengthPosterior(
            probabilities=new_probs, suffstats=new_stats_full
        )
        self._observation_count += 1
        return self._posterior

    def _predict_and_grow_runs(
        self, x: float, old_stats: list[_RunSuffStats]
    ) -> tuple[list[float], list[_RunSuffStats]]:
        """Adams & MacKay 2007 Algorithm 1 steps 1-2: each existing run's
        log predictive density at ``x``, and its stats grown by ``x``."""
        cfg = self._config
        # Step 1: predictive probabilities under each existing run
        log_predictive: list[float] = []
        for stats in old_stats:
            df, loc, scale = _predictive_params_from_suffstats(
                cfg, stats, self._mu_0
            )
            log_predictive.append(_student_t_log_pdf(x, df, loc, scale))

        # Step 2: update sufficient stats for each existing run
        new_stats_grow: list[_RunSuffStats] = [
            stats.update(x) for stats in old_stats
        ]
        return log_predictive, new_stats_grow

    def _log_growth_and_change(
        self, old_probs: list[float], log_predictive: list[float]
    ) -> tuple[list[float], float]:
        """Adams & MacKay 2007 Algorithm 1 steps 3-4 under the constant
        hazard ``1 / hazard_lambda``: the growth log-masses and the total
        change-point log-mass."""
        cfg = self._config
        # Step 3: growth probabilities (run length increases by 1)
        # P(r_t = r+1, x_1:t) = P(r_{t-1} = r, x_1:t-1)
        #                       * pi_t^(r) * (1 - H(r))
        hazard = 1.0 / cfg.hazard_lambda
        log_growth = [
            math.log(max(p, cfg.epsilon)) + lp + math.log(1.0 - hazard)
            for p, lp in zip(old_probs, log_predictive)
        ]

        # Step 4: change-point probability (run length resets to 0)
        # P(r_t = 0, x_1:t) = sum_r P(r_{t-1} = r, x_1:t-1)
        #                            * pi_t^(r) * H(r)
        log_change_terms = [
            math.log(max(p, cfg.epsilon)) + lp + math.log(hazard)
            for p, lp in zip(old_probs, log_predictive)
        ]
        return log_growth, _logsumexp(log_change_terms)

    def _assemble_truncated_posterior(
        self,
        x: float,
        log_change_total: float,
        log_growth: list[float],
        new_stats_grow: list[_RunSuffStats],
    ) -> tuple[list[float], list[_RunSuffStats]]:
        """Adams & MacKay 2007 Algorithm 1 steps 5-6: the unnormalised
        posterior, run length 0 first, truncated at ``run_length_max``."""
        cfg = self._config
        # Step 5: assemble new posterior with the 0-th entry being
        # the change-point case and entries 1..r+1 being the grown
        # run lengths
        new_log_unnormalised: list[float] = [log_change_total] + log_growth
        new_stats_full: list[_RunSuffStats] = [_RunSuffStats().update(x)] + new_stats_grow

        # Step 6: truncate at run_length_max (drop the longest runs,
        # which carry vanishing probability) — keeps memory bounded.
        if len(new_log_unnormalised) > cfg.run_length_max:
            new_log_unnormalised = new_log_unnormalised[: cfg.run_length_max]
            new_stats_full = new_stats_full[: cfg.run_length_max]
        return new_log_unnormalised, new_stats_full


def _logsumexp(values: Sequence[float]) -> float:
    """Numerically stable ``log(sum(exp(v) for v in values))``.

    Standard logsumexp trick: subtract the max before exponentiating.
    Returns ``-math.inf`` for an empty sequence.
    """
    if not values:
        return -math.inf
    finite = list(filter(_ABOVE_NEGATIVE_INFINITY, values))
    if not finite:
        return -math.inf
    return _logsumexp_of_finite(finite)


def _logsumexp_of_finite(finite: list[float]) -> float:
    """``log(sum(exp(v)))`` of a non-empty list with no ``-inf`` entry,
    shifted by its max for stability."""
    m = max(finite)
    total = sum(math.exp(v - m) for v in finite)
    if total <= 0.0:
        return -math.inf
    return m + math.log(total)
