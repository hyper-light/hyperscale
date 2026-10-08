import math
import operator
from bisect import bisect_right
from collections import Counter
from fractions import Fraction
from functools import partial, reduce
from itertools import accumulate, compress, repeat
from typing import Dict, List, Sequence

import numpy as np

from hyperscale.reporting.common.results_types import SampleDigestState

from .exact_arithmetic import (
    SQUARE_SUM_SCALE_BITS,
    SUM_SCALE_BITS,
    exact_float_sum_units,
    exact_square_sum_units,
)
from .log_linear_buckets import (
    EXACT_FLOAT_INTEGER_BOUND,
    bucket_midpoint_numerator_and_shift,
    float_bucket_keys,
    integer_bucket_key,
)


class SampleDigest:
    """
    A set of samples (ints or finite floats) reduced to a mergeable digest.

    The count, the sum and the sum of squares are exact integers (see
    ``exact_arithmetic``), the minimum and maximum are the samples
    themselves, and the distribution is a log-linear histogram (see
    ``log_linear_buckets``). Merging adds integers and bucket counts, so
    digests merged in any order and grouping equal the digest of the union,
    exactly, and every statistic computed from it is the same.

    The mean, variance and their derived rate are exact until rounded once
    to a float. Quantiles, the median and the mean absolute deviation come
    from bucket midpoints: each is within ``|x| / (2 * SUB_BUCKETS_PER_BINADE)``
    of the exact value for the samples x it is computed from.
    """

    __slots__ = (
        "count",
        "sum_units",
        "square_sum_units",
        "minimum",
        "maximum",
        "zero_count",
        "positive_buckets",
        "negative_buckets",
    )

    def __init__(
        self,
        count: int,
        sum_units: int,
        square_sum_units: int,
        minimum: int | float,
        maximum: int | float,
        zero_count: int,
        positive_buckets: Dict[int, int],
        negative_buckets: Dict[int, int],
    ) -> None:
        self.count = count
        self.sum_units = sum_units
        self.square_sum_units = square_sum_units
        self.minimum = minimum
        self.maximum = maximum
        self.zero_count = zero_count
        self.positive_buckets = positive_buckets
        self.negative_buckets = negative_buckets

    @classmethod
    def from_values(cls, values: Sequence[int | float]) -> "SampleDigest":
        """The digest of one or more ints or finite floats."""
        float_samples, large_integers = cls._partition_samples(values)
        parts = [cls._from_floats(float_samples)] if float_samples.size else []
        parts.extend([cls._from_integers(large_integers)] if large_integers else [])
        return cls.merge_all(parts)

    @classmethod
    def from_state(cls, state: SampleDigestState) -> "SampleDigest":
        """A digest from the state ``to_state`` returned."""
        return cls(
            state["count"],
            state["sum_units"],
            state["square_sum_units"],
            state["minimum"],
            state["maximum"],
            state["zero_count"],
            dict(state["positive_buckets"]),
            dict(state["negative_buckets"]),
        )

    @classmethod
    def merge_all(cls, digests: Sequence["SampleDigest"]) -> "SampleDigest":
        """The digest of the union of one or more digests' samples."""
        if not digests:
            raise ValueError("A sample digest needs at least one sample")
        return reduce(cls.merge, digests)

    def to_state(self) -> SampleDigestState:
        """The digest as plain ints, floats and dicts, for the wire."""
        return {
            "count": self.count,
            "sum_units": self.sum_units,
            "square_sum_units": self.square_sum_units,
            "minimum": self.minimum,
            "maximum": self.maximum,
            "zero_count": self.zero_count,
            "positive_buckets": dict(self.positive_buckets),
            "negative_buckets": dict(self.negative_buckets),
        }

    def merge(self, other: "SampleDigest") -> "SampleDigest":
        """The digest of this digest's samples and ``other``'s."""
        positive_buckets = Counter(self.positive_buckets)
        positive_buckets.update(other.positive_buckets)
        negative_buckets = Counter(self.negative_buckets)
        negative_buckets.update(other.negative_buckets)

        return SampleDigest(
            self.count + other.count,
            self.sum_units + other.sum_units,
            self.square_sum_units + other.square_sum_units,
            min(self.minimum, other.minimum),
            max(self.maximum, other.maximum),
            self.zero_count + other.zero_count,
            dict(positive_buckets),
            dict(negative_buckets),
        )

    def exact_sum(self) -> Fraction:
        """The exact sum of the samples."""
        return Fraction(self.sum_units, 1 << SUM_SCALE_BITS)

    def total(self) -> int | float:
        """The sum of the samples: an int when it is whole, else rounded once to a float."""
        exact_sum = self.exact_sum()
        return exact_sum.numerator if exact_sum.denominator == 1 else float(exact_sum)

    def exact_mean(self) -> Fraction:
        """The exact mean of the samples."""
        return Fraction(self.sum_units, self.count << SUM_SCALE_BITS)

    def exact_variance(self) -> Fraction:
        """The exact population variance of the samples (numpy's ``var``, ddof 0)."""
        return Fraction(
            self.count * self.square_sum_units - self.sum_units**2,
            self.count**2 << SQUARE_SUM_SCALE_BITS,
        )

    def stats(self, percentiles: Sequence[int]) -> Dict[str, int | float]:
        """The mean, extremes, median, spread and the quantiles at ``percentiles``."""
        midpoints, counts, cumulative_counts = self._ordered_buckets()
        variance = float(self.exact_variance())

        stats: Dict[str, int | float] = {
            "mean": float(self.exact_mean()),
            "max": self.maximum,
            "min": self.minimum,
            "med": self._quantile(50, midpoints, cumulative_counts),
            "stdev": math.sqrt(variance),
            "var": variance,
            "mad": self._mean_absolute_deviation(midpoints, counts),
        }
        stats.update(self._quantiles(percentiles, midpoints, cumulative_counts))
        return stats

    def quantile_stats(self, percentiles: Sequence[int]) -> Dict[str, float]:
        """The quantiles at ``percentiles``, keyed as ``{percentile}th_quantile``."""
        midpoints, _, cumulative_counts = self._ordered_buckets()
        return self._quantiles(percentiles, midpoints, cumulative_counts)

    def _quantiles(
        self,
        percentiles: Sequence[int],
        midpoints: List[Fraction],
        cumulative_counts: List[int],
    ) -> Dict[str, float]:
        return {
            f"{percentile}th_quantile": self._quantile(percentile, midpoints, cumulative_counts)
            for percentile in percentiles
        }

    def _quantile(
        self,
        percentile: int,
        midpoints: List[Fraction],
        cumulative_counts: List[int],
    ) -> float:
        """
        numpy's default ("linear") percentile: the order statistics at the
        ranks around ``percentile / 100 * (count - 1)``, interpolated exactly,
        with each order statistic read as its bucket's midpoint.
        """
        position = Fraction(percentile * (self.count - 1), 100)
        lower_rank = math.floor(position)
        upper_rank = min(lower_rank + 1, self.count - 1)

        lower = midpoints[bisect_right(cumulative_counts, lower_rank)]
        upper = midpoints[bisect_right(cumulative_counts, upper_rank)]
        return float(lower + (position - lower_rank) * (upper - lower))

    def _mean_absolute_deviation(self, midpoints: List[Fraction], counts: List[int]) -> float:
        """The mean absolute deviation from the exact mean, of the bucket midpoints."""
        mean = self.exact_mean()
        deviation_sum = sum(
            count * abs(midpoint - mean) for midpoint, count in zip(midpoints, counts)
        )
        return float(Fraction(deviation_sum) / self.count)

    def _ordered_buckets(self) -> tuple[List[Fraction], List[int], List[int]]:
        """Every bucket's midpoint and count in ascending value order, and the running counts."""
        negative_keys = sorted(self.negative_buckets, reverse=True)
        positive_keys = sorted(self.positive_buckets)

        midpoints = [
            *map(operator.neg, map(self._midpoint, negative_keys)),
            Fraction(0),
            *map(self._midpoint, positive_keys),
        ]
        counts = [
            *map(self.negative_buckets.__getitem__, negative_keys),
            self.zero_count,
            *map(self.positive_buckets.__getitem__, positive_keys),
        ]
        return midpoints, counts, list(accumulate(counts))

    @staticmethod
    def _midpoint(bucket_key: int) -> Fraction:
        numerator, shift = bucket_midpoint_numerator_and_shift(bucket_key)
        return numerator * Fraction(2) ** shift

    @classmethod
    def _partition_samples(cls, values: Sequence[int | float]) -> tuple[np.ndarray, List[int]]:
        """The samples a float carries exactly, as an array; and the larger ints."""
        if all(map(isinstance, values, repeat(float))):
            return cls._finite_samples(values), []

        large_integer_flags = list(map(cls._is_large_integer, values))
        return (
            cls._finite_samples(list(compress(values, map(operator.not_, large_integer_flags)))),
            list(compress(values, large_integer_flags)),
        )

    @staticmethod
    def _is_large_integer(value: int | float) -> bool:
        """True for an int a float cannot carry exactly; a sample that is no number is refused."""
        if isinstance(value, float):
            return False
        if not isinstance(value, int):
            raise TypeError(f"Samples must be ints or floats, not {type(value).__name__}")
        return abs(value) > EXACT_FLOAT_INTEGER_BOUND

    @staticmethod
    def _finite_samples(values: Sequence[int | float]) -> np.ndarray:
        samples = np.array(values, dtype=np.float64)
        if not np.isfinite(samples).all():
            raise ValueError("Samples must be finite: NaN and infinity have no exact sum")
        return samples

    @classmethod
    def _from_floats(cls, samples: np.ndarray) -> "SampleDigest":
        return cls(
            int(samples.size),
            exact_float_sum_units(samples.tolist()),
            exact_square_sum_units(samples),
            float(samples.min()),
            float(samples.max()),
            int(np.count_nonzero(samples == 0.0)),
            cls._count_bucket_keys(float_bucket_keys(samples[samples > 0.0])),
            cls._count_bucket_keys(float_bucket_keys(-samples[samples < 0.0])),
        )

    @classmethod
    def _from_integers(cls, samples: List[int]) -> "SampleDigest":
        """Ints too large for a float to carry exactly (none is zero)."""
        positive_samples = filter(partial(operator.lt, 0), samples)
        negative_magnitudes = map(operator.neg, filter(partial(operator.gt, 0), samples))

        return cls(
            len(samples),
            sum(samples) << SUM_SCALE_BITS,
            sum(map(operator.mul, samples, samples)) << SQUARE_SUM_SCALE_BITS,
            min(samples),
            max(samples),
            0,
            dict(Counter(map(integer_bucket_key, positive_samples))),
            dict(Counter(map(integer_bucket_key, negative_magnitudes))),
        )

    @staticmethod
    def _count_bucket_keys(bucket_keys: np.ndarray) -> Dict[int, int]:
        unique_keys, key_counts = np.unique(bucket_keys, return_counts=True)
        return dict(zip(unique_keys.tolist(), key_counts.tolist()))
