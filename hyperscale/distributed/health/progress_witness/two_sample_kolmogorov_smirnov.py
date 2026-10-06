"""``TwoSampleKolmogorovSmirnov`` -- pickled under the namespace
``hyperscale.distributed.health.progress_witness.kolmogorov_smirnov`` (see that module)."""

from __future__ import annotations

import math
from typing import Sequence

from .ks_result import KSResult


def _kolmogorov_survival(x: float) -> float:
    """Survival function ``Q_KS(x) = 1 - K(x)`` for the Kolmogorov
    distribution.

    Implements the convergent infinite-series form (Numerical Recipes
    §14.3.3 eq 14.3.20):
        Q_KS(x) = 2 * sum_{j=1}^∞ (-1)^(j-1) * exp(-2 * j^2 * x^2)
    Truncated when terms drop below 1e-10 of the first.
    """
    if x <= 0.0:
        return 1.0
    if x >= 6.0:
        # Tail beyond machine precision; survival ≈ 0
        return 0.0
    total = 0.0
    sign = 1.0
    for j in range(1, 1000):
        term = sign * math.exp(-2.0 * j * j * x * x)
        total += term
        sign = -sign
        if abs(term) < 1e-10 * abs(total):
            break
    return max(0.0, min(1.0, 2.0 * total))


class TwoSampleKolmogorovSmirnov:
    """Two-sample K-S tester.

    Stateless utility — held as a class for API symmetry with the
    other progress-witness components. Construct once, call
    ``.test(sample_a, sample_b)`` repeatedly.
    """

    @staticmethod
    def test(
        sample_a: Sequence[float], sample_b: Sequence[float]
    ) -> KSResult:
        """Run the two-sample K-S test on ``sample_a`` vs
        ``sample_b``.

        Both samples must be non-empty. Returns a ``KSResult`` with
        the statistic, the asymptotic p-value, and both sample
        sizes.

        Algorithm: merge-walk the two sorted sample sequences and
        track the running difference between the two empirical
        CDFs. The maximum absolute difference is the statistic.
        Time complexity O((n1 + n2) log(n1 + n2)) for the sort.
        """
        n1 = len(sample_a)
        n2 = len(sample_b)
        if n1 == 0 or n2 == 0:
            raise ValueError(
                "TwoSampleKolmogorovSmirnov.test: both samples must be non-empty"
            )

        sorted_a = sorted(sample_a)
        sorted_b = sorted(sample_b)
        i, j = 0, 0
        cdf_a, cdf_b = 0.0, 0.0
        d_statistic = 0.0
        inv_n1 = 1.0 / n1
        inv_n2 = 1.0 / n2

        while i < n1 and j < n2:
            value_a = sorted_a[i]
            value_b = sorted_b[j]
            if value_a == value_b:
                # Tie — advance both CDFs at the same x so identical
                # distributions produce a zero K-S statistic.
                cdf_a += inv_n1
                cdf_b += inv_n2
                i += 1
                j += 1
            elif value_a < value_b:
                cdf_a += inv_n1
                i += 1
            else:
                cdf_b += inv_n2
                j += 1
            diff = abs(cdf_a - cdf_b)
            if diff > d_statistic:
                d_statistic = diff

        # Account for any remaining tail in the longer sample. Once
        # one stream is exhausted the other can only widen the CDF
        # gap — but the true maximum has already been captured during
        # the merge walk because the exhausted side's CDF is fixed
        # at 1.0.

        # Asymptotic p-value via the Kolmogorov distribution.
        n_eff = math.sqrt(n1 * n2 / (n1 + n2))
        # Numerical Recipes uses (n_eff + 0.12 + 0.11 / n_eff) as the
        # finite-sample correction to the asymptotic statistic.
        adjusted = (n_eff + 0.12 + 0.11 / n_eff) * d_statistic
        p_value = _kolmogorov_survival(adjusted)

        return KSResult(
            statistic=d_statistic,
            p_value=p_value,
            n1=n1,
            n2=n2,
        )
