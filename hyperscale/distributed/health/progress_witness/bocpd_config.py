"""``BOCPDConfig`` -- pickled under the namespace
``hyperscale.distributed.health.progress_witness.bocpd`` (see that module)."""

from __future__ import annotations

from typing import TYPE_CHECKING
from dataclasses import dataclass

if TYPE_CHECKING:
    from .bayesian_online_change_point_detector import BayesianOnlineChangePointDetector


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
