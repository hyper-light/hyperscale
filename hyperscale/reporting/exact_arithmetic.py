"""
Exact sums of samples, for statistics that merge across workers and
datacenters without depending on the order or grouping of the merge.

Every finite binary64 value is an integer multiple of 2**-1074 (the smallest
positive subnormal), and so is every Python int. A sum of samples is held
exactly as an integer count of 2**-1074 units, and a sum of their squares as
an integer count of 2**-2148 units. Integer addition is associative and
commutative, so merged partial sums equal the sum of the union exactly.
"""

import math
from itertools import chain
from typing import Iterable, Sequence

import numpy as np

# -log2 of the smallest positive subnormal binary64 value.
SUM_SCALE_BITS = 1074
SQUARE_SUM_SCALE_BITS = 2 * SUM_SCALE_BITS

# Veltkamp's splitter for a 53-bit significand: 2**ceil(53 / 2) + 1. It splits
# a value into a high and a low half of at most 26 significant bits each, so
# every product of two halves (at most 52 bits) is exact (Dekker 1971) --
# while none overflows or underflows.
VELTKAMP_SPLITTER = float(2**27 + 1)

# For a in [2**(e-1), 2**e) both halves are multiples of 2**(e-53), so the
# lowest bit of any product of halves is 2**(2e-106): it stays on the
# 2**-1074 grid while e >= -484, that is a >= 2**-485.
SPLIT_LOWER_BOUND = 2.0**-485

# The high half is below 2**e * (1 + 2**-26), so its square stays below
# 2**1024 while e <= 511, that is a < 2**511.
SPLIT_UPPER_BOUND = 2.0**511


def rational_units(value: int | float, scale_bits: int) -> int:
    """``value`` as an exact integer count of 2**-``scale_bits`` units."""
    numerator, denominator = value.as_integer_ratio()
    return numerator << (scale_bits - denominator.bit_length() + 1)


def exact_float_sum_units(values: Iterable[float]) -> int:
    """
    The exact sum of finite floats, in 2**-1074 units.

    ``math.fsum`` returns the sum rounded to a float. Subtracting each
    rounded partial and summing again yields the next one; the exact sum is
    the sum of the partials. Each partial is at most half an ulp of the one
    before, so the loop ends after at most ~2098 / 53 passes.
    """
    samples = list(values)
    negated_partials: list[float] = []

    while (partial := math.fsum(chain(samples, negated_partials))) != 0.0:
        negated_partials.append(-partial)

    return -sum(
        rational_units(negated_partial, SUM_SCALE_BITS)
        for negated_partial in negated_partials
    )


def exact_square_sum_units(samples: np.ndarray) -> int:
    """
    The exact sum of the squares of finite floats, in 2**-2148 units.

    Samples whose split products are exact (see ``SPLIT_LOWER_BOUND`` and
    ``SPLIT_UPPER_BOUND``) are squared as three exact float products; any
    other sample is squared as an exact integer ratio.
    """
    magnitudes = np.abs(samples)
    splittable = (samples == 0.0) | (
        (magnitudes >= SPLIT_LOWER_BOUND) & (magnitudes < SPLIT_UPPER_BOUND)
    )

    return split_square_sum_units(samples[splittable]) + rational_square_sum_units(
        samples[~splittable].tolist()
    )


def split_square_sum_units(samples: np.ndarray) -> int:
    """The exact sum of squares of splittable floats, in 2**-2148 units."""
    scaled = samples * VELTKAMP_SPLITTER
    high_halves = scaled - (scaled - samples)
    low_halves = samples - high_halves

    exact_products = chain(
        (high_halves * high_halves).tolist(),
        (2.0 * high_halves * low_halves).tolist(),
        (low_halves * low_halves).tolist(),
    )

    return exact_float_sum_units(exact_products) << SUM_SCALE_BITS


def rational_square_sum_units(samples: Sequence[int | float]) -> int:
    """The exact sum of squares of any ints or finite floats, in 2**-2148 units."""
    return sum(
        rational_units(sample, SUM_SCALE_BITS) ** 2 for sample in samples
    )
