"""
Two-sample Kolmogorov-Smirnov stationarity test (AD-26 Phase H6).

Tests whether two empirical samples are drawn from the same
distribution, returning a p-value. Used to pick the largest
TimeWindowedTDigest scale at which the throughput observation
stream is currently stationary — giving the BOCPD detector a
baseline window without a hardcoded length.

The K-S statistic is the maximum vertical distance between two
empirical CDFs:
    D = sup_x |F_1(x) - F_2(x)|
Under the null hypothesis (same distribution), D scaled by the
effective sample size follows a known limiting distribution. The
p-value is computed via the standard Kolmogorov distribution
formula.

Reference:
    Press, W. H., et al. (2007). Numerical Recipes: The Art of
    Scientific Computing (3rd ed.). §14.3.3.

This module is the pickling namespace of the classes and functions
below. Each lives in a file of its own and is re-homed here -- its
``__module__`` set to this module -- so its pickled form names this
module, exactly as before the split: mixed-version clusters keep
talking and data written earlier keeps loading.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Sequence

from .two_sample_kolmogorov_smirnov import _kolmogorov_survival
from .ks_result import KSResult
from .two_sample_kolmogorov_smirnov import TwoSampleKolmogorovSmirnov

_REHOMED = (
    KSResult,
    TwoSampleKolmogorovSmirnov,
    _kolmogorov_survival,
)

for _rehomed in _REHOMED:
    _rehomed.__module__ = __name__
