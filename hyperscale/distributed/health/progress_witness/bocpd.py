"""
Bayesian Online Change-Point Detection (Adams & MacKay, 2007).

Detects when the underlying distribution of an observation stream
shifts — without a magic threshold. The only inputs are a hazard
prior (parameter-free, uniform over run lengths) and the predictive
likelihood family (Student-t induced by a Normal-inverse-Gamma
conjugate prior over Gaussian observations).

Used by the AD-26 Phase H6 throughput witness to flag regime shifts
in per-(worker, workflow) throughput. A drop in throughput appears
as a change-point with the new run's posterior mean below the prior
run's posterior mean — the witness consults the *direction* of the
shift to distinguish "throughput regressed → potential stuck
workflow" from "throughput surged → no concern."

Reference:
    Adams, R. P., & MacKay, D. J. C. (2007). Bayesian Online
    Change-Point Detection. arXiv:0710.3742.

Notation in the implementation matches the paper:
    r_t            run length at time t
    P(r_t | x_1:t) run-length posterior
    H(r)           hazard function — P(change-point | run length r)
    pi_t^(r)       predictive probability under run length r
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Sequence


# ============================================================================
# Configuration
# ============================================================================


@dataclass(slots=True, frozen=True)
class BOCPDConfig:
    """Configuration for ``BayesianOnlineChangePointDetector``.

    All fields have principled defaults — none are magic constants.
    Each is either:

    * a non-informative prior (kappa_0=1, alpha_0=1, beta_0=1) — flat
      enough that ~3-5 observations override it, but not so flat that
      the first observation collapses the predictive variance to 0;
    * a structural cap (run_length_max) preventing unbounded posterior
      growth — set to a value comfortably exceeding the expected
      stationarity window;
    * a numerical-stability epsilon — kept tiny.
    """

    # Normal-inverse-Gamma prior hyperparameters.
    # mu_0: prior mean of the observation distribution. Set at first
    # observation time so the prior never disagrees catastrophically
    # with the very first sample (cold-start safety).
    mu_0: float = 0.0
    kappa_0: float = 1.0  # Prior pseudo-count for the mean
    alpha_0: float = 1.0  # Prior pseudo-count for the variance
    beta_0: float = 1.0  # Prior scale for the variance

    # Hazard prior. Constant (memoryless) hazard with rate 1/lambda
    # corresponds to expecting a change every ``lambda`` observations.
    # Default 250 picks a "we don't really know — assume change-points
    # are rare relative to the windowed-stationarity scale we use."
    hazard_lambda: float = 250.0

    # Truncate the run-length posterior at this many run lengths.
    # Bounds the per-stream memory at O(run_length_max) instead of
    # the unbounded O(t). 1000 covers ~1000 observations of
    # contiguous run; longer streams keep the most recent 1000 run
    # lengths.
    run_length_max: int = 1000

    # Numerical-stability floor for posterior probabilities. Avoids
    # log(0) when an observation is wildly inconsistent with every
    # tracked run.
    epsilon: float = 1e-300


# ============================================================================
# Per-run-length sufficient statistics
# ============================================================================


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


def _predictive_params_from_suffstats(
    config: BOCPDConfig,
    stats: _RunSuffStats,
    mu_0: float,
) -> tuple[float, float, float]:
    """Posterior-predictive Student-t parameters ``(df, location, scale)``.

    Derived from the Normal-inverse-Gamma posterior at this run
    length:
        kappa_n = kappa_0 + n
        mu_n    = (kappa_0 * mu_0 + n * mean) / kappa_n
        alpha_n = alpha_0 + n / 2
        beta_n  = beta_0 + 0.5 * m2 + (kappa_0 * n / kappa_n) * (mean - mu_0)^2 / 2
    Posterior predictive is Student-t with:
        df       = 2 * alpha_n
        location = mu_n
        scale    = sqrt(beta_n * (kappa_n + 1) / (kappa_n * alpha_n))
    """
    n = stats.n
    if n == 0:
        # Prior predictive — Student-t with df = 2*alpha_0
        df = 2.0 * config.alpha_0
        location = mu_0
        scale = math.sqrt(
            config.beta_0 * (config.kappa_0 + 1.0)
            / (config.kappa_0 * config.alpha_0)
        )
        return df, location, scale

    kappa_n = config.kappa_0 + n
    mu_n = (config.kappa_0 * mu_0 + n * stats.mean) / kappa_n
    alpha_n = config.alpha_0 + n / 2.0
    beta_n = (
        config.beta_0
        + 0.5 * stats.m2
        + (config.kappa_0 * n) / kappa_n * (stats.mean - mu_0) ** 2 / 2.0
    )
    df = 2.0 * alpha_n
    location = mu_n
    scale = math.sqrt(beta_n * (kappa_n + 1.0) / (kappa_n * alpha_n))
    return df, location, scale


# ============================================================================
# Run-length posterior
# ============================================================================


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
        max_idx = 0
        max_p = self.probabilities[0]
        for idx, p in enumerate(self.probabilities):
            if p > max_p:
                max_idx = idx
                max_p = p
        return max_idx

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


# ============================================================================
# Public detector
# ============================================================================


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
        cfg = self._config
        old_probs = self._posterior.probabilities
        old_stats = self._posterior.suffstats

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
        log_change_total = _logsumexp(log_change_terms)

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

        # Step 7: normalise into a probability distribution
        log_norm = _logsumexp(new_log_unnormalised)
        new_probs = [math.exp(lp - log_norm) for lp in new_log_unnormalised]

        self._posterior = RunLengthPosterior(
            probabilities=new_probs, suffstats=new_stats_full
        )
        self._observation_count += 1
        return self._posterior


def _logsumexp(values: Sequence[float]) -> float:
    """Numerically stable ``log(sum(exp(v) for v in values))``.

    Standard logsumexp trick: subtract the max before exponentiating.
    Returns ``-math.inf`` for an empty sequence.
    """
    if not values:
        return -math.inf
    finite = [v for v in values if v > -math.inf]
    if not finite:
        return -math.inf
    m = max(finite)
    total = sum(math.exp(v - m) for v in finite)
    if total <= 0.0:
        return -math.inf
    return m + math.log(total)
