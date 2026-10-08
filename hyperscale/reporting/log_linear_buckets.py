"""
Log-linear bucket indexes (HdrHistogram's layout) for sample digests.

Each binade [2**(e-1), 2**e) of magnitudes is cut into
``SUB_BUCKETS_PER_BINADE`` equal buckets. A bucket's key is
``e * SUB_BUCKETS_PER_BINADE + sub_bucket``. Keys come from a value's exact
binary exponent and leading significand bits, never from a logarithm, so a
value lands in the same bucket on every node, and the bucket's midpoint is
within 1 / (2 * SUB_BUCKETS_PER_BINADE) of every value in it, relatively.
"""

import math

import numpy as np

# Two significant decimal digits, as HdrHistogram sizes its sub-buckets:
# 2**ceil(log2(10**2)) = 128 buckets per binade, a relative error of at most
# 1/256 (DDSketch's default is 1/100). It keeps a digest at most 128 buckets
# per binade the samples span, so per-core results stay far inside the
# 5 MB decompressed message bound (MAX_DECOMPRESSED_SIZE).
SIGNIFICANT_DECIMAL_DIGITS = 2
SUB_BUCKET_BITS = math.ceil(math.log2(10**SIGNIFICANT_DECIMAL_DIGITS))
SUB_BUCKETS_PER_BINADE = 1 << SUB_BUCKET_BITS

# Ints up to 2**53 convert to floats exactly and are bucketed with them.
EXACT_FLOAT_INTEGER_BOUND = 2**53


def float_bucket_keys(magnitudes: np.ndarray) -> np.ndarray:
    """The bucket key of each positive finite float magnitude."""
    significands, exponents = np.frexp(magnitudes)
    # 2m - 1 is exact (Sterbenz) and scaling by a power of two is exact, so
    # the floor is the exact sub-bucket.
    sub_buckets = np.floor((2.0 * significands - 1.0) * SUB_BUCKETS_PER_BINADE)
    return exponents.astype(np.int64) * SUB_BUCKETS_PER_BINADE + sub_buckets.astype(np.int64)


def integer_bucket_key(magnitude: int) -> int:
    """The bucket key of a positive int, of any size."""
    exponent = magnitude.bit_length()
    binade_start = 1 << (exponent - 1)
    sub_bucket = ((magnitude - binade_start) << SUB_BUCKET_BITS) >> (exponent - 1)
    return exponent * SUB_BUCKETS_PER_BINADE + sub_bucket


def bucket_midpoint_numerator_and_shift(bucket_key: int) -> tuple[int, int]:
    """
    A bucket's midpoint as ``numerator * 2**shift``, exactly: the bucket
    [(S + s) * 2**(e-1) / S, (S + s + 1) * 2**(e-1) / S) has midpoint
    (2S + 2s + 1) * 2**(e - 2 - SUB_BUCKET_BITS).
    """
    exponent, sub_bucket = divmod(bucket_key, SUB_BUCKETS_PER_BINADE)
    return (
        2 * SUB_BUCKETS_PER_BINADE + 2 * sub_bucket + 1,
        exponent - 2 - SUB_BUCKET_BITS,
    )
