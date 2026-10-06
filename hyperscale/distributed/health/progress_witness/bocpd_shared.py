"""Definitions shared by the classes of
``hyperscale.distributed.health.progress_witness.bocpd`` (see that module)."""

from __future__ import annotations

import math

from .bocpd_config import BOCPDConfig
from ._run_suff_stats import _RunSuffStats


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
