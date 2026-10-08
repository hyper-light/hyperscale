"""Sample digests merge exactly and their statistics hold to bounds derived from the arithmetic.

Each seed draws samples (timings, signed values, zeros, subnormals, values at
the exact-split bounds, ints past 2**63), splits them across sources, and
merges the sources' digests in a random order and grouping. The merged
digest must equal the digest of the union, field for field, and its
statistics must match an exact reference computed with Fractions from the
raw samples:

- mean and variance are exact until rounded once, so they equal the
  reference rounded to a float;
- stdev is sqrt of the rounded variance, rounded: within 1.5 units of
  roundoff (u = 2**-53) of the exact root, bounded here by 2u;
- each quantile interpolates order statistics read as bucket midpoints:
  within max(|x_lower|, |x_upper|) / (2 * SUB_BUCKETS_PER_BINADE) of the
  exact interpolation, plus half an ulp for the final rounding;
- the mean absolute deviation is within mean(|x|) / (2 * SUB_BUCKETS_PER_BINADE)
  plus half an ulp.
"""

import math
import random
from decimal import Decimal, localcontext
from fractions import Fraction

import pytest

from hyperscale.reporting.exact_arithmetic import (
    SPLIT_LOWER_BOUND,
    SPLIT_UPPER_BOUND,
    SQUARE_SUM_SCALE_BITS,
    SUM_SCALE_BITS,
    exact_float_sum_units,
)
from hyperscale.reporting.log_linear_buckets import SUB_BUCKETS_PER_BINADE
from hyperscale.reporting.sample_digest import SampleDigest

PERCENTILES = [0, 1, 10, 20, 25, 30, 40, 50, 60, 70, 75, 80, 90, 99, 100]
UNIT_ROUNDOFF = Fraction(1, 2**53)
HALF_BUCKET_RELATIVE_ERROR = Fraction(1, 2 * SUB_BUCKETS_PER_BINADE)
SEEDS = range(40)


def draw_sample(generator: random.Random) -> int | float:
    """One sample from a mix of the shapes results carry, and the edges of the arithmetic."""
    shape = generator.randrange(10)
    return [
        lambda: generator.lognormvariate(-4.0, 1.5),
        lambda: generator.uniform(-1e6, 1e6),
        lambda: 0.0,
        lambda: -0.0,
        lambda: generator.randrange(-(2**70), 2**70),
        lambda: generator.randrange(2**63, 2**64),
        lambda: 2**53 + generator.randrange(-2, 3),
        lambda: generator.choice([SPLIT_LOWER_BOUND, -SPLIT_LOWER_BOUND]) * generator.uniform(0.5, 2.0),
        lambda: generator.randrange(0, 1000),
        lambda: generator.uniform(-1.0, 1.0) * 2.0**-1060,
    ][shape]()


def draw_samples(generator: random.Random) -> list[int | float]:
    return [draw_sample(generator) for _ in range(generator.randrange(1, 400))]


def split_into_sources(generator: random.Random, samples: list[int | float]) -> list[list[int | float]]:
    shuffled = samples[:]
    generator.shuffle(shuffled)
    cut_count = generator.randrange(0, min(len(shuffled), 12))
    cuts = sorted(generator.sample(range(1, len(shuffled)), cut_count)) if len(shuffled) > 1 else []
    bounds = [0, *cuts, len(shuffled)]
    return [shuffled[start:end] for start, end in zip(bounds, bounds[1:])]


def merge_in_random_grouping(generator: random.Random, digests: list[SampleDigest]) -> SampleDigest:
    """Merge pairs drawn at random until one digest is left: any order, any grouping."""
    pending = digests[:]
    while len(pending) > 1:
        first = pending.pop(generator.randrange(len(pending)))
        second = pending.pop(generator.randrange(len(pending)))
        merged = first.merge(second) if generator.random() < 0.5 else second.merge(first)
        pending.insert(generator.randrange(len(pending) + 1), merged)
    return pending[0]


def round_trip(digest: SampleDigest) -> SampleDigest:
    return SampleDigest.from_state(digest.to_state())


class ExactReference:
    """Statistics of weighted samples computed exactly with Fractions."""

    def __init__(self, weighted_samples: list[tuple[int | float, int]]) -> None:
        ordered = sorted((Fraction(value), weight) for value, weight in weighted_samples)
        self.values = [value for value, _ in ordered]
        self.weights = [weight for _, weight in ordered]
        self.count = sum(self.weights)
        self.mean = sum(value * weight for value, weight in ordered) / self.count
        self.variance = sum(weight * (value - self.mean) ** 2 for value, weight in ordered) / self.count
        self.mean_absolute_deviation = sum(weight * abs(value - self.mean) for value, weight in ordered) / self.count
        self.mean_magnitude = sum(weight * abs(value) for value, weight in ordered) / self.count

    def order_statistic(self, rank: int) -> Fraction:
        cumulative = 0
        for value, weight in zip(self.values, self.weights):
            cumulative += weight
            if rank < cumulative:
                return value
        raise IndexError(rank)

    def quantile_and_bound(self, percentile: int) -> tuple[Fraction, Fraction]:
        position = Fraction(percentile * (self.count - 1), 100)
        lower_rank = math.floor(position)
        lower = self.order_statistic(lower_rank)
        upper = self.order_statistic(min(lower_rank + 1, self.count - 1))
        exact = lower + (position - lower_rank) * (upper - lower)
        return exact, max(abs(lower), abs(upper)) * HALF_BUCKET_RELATIVE_ERROR


def half_ulp(value: float) -> Fraction:
    return Fraction(math.ulp(value)) / 2


def assert_stats_match_reference(digest: SampleDigest, reference: ExactReference) -> None:
    stats = digest.stats(PERCENTILES)

    assert digest.count == reference.count
    assert stats["mean"] == float(reference.mean)
    assert stats["var"] == float(reference.variance)
    assert Fraction(stats["min"]) == reference.values[0]
    assert Fraction(stats["max"]) == reference.values[-1]

    with localcontext() as context:
        context.prec = 80
        exact_stdev = (Decimal(reference.variance.numerator) / Decimal(reference.variance.denominator)).sqrt()
        assert abs(Decimal(stats["stdev"]) - exact_stdev) <= 2 * Decimal(2**-53) * exact_stdev

    for percentile in PERCENTILES:
        exact, bound = reference.quantile_and_bound(percentile)
        reported = stats[f"{percentile}th_quantile"]
        assert abs(Fraction(reported) - exact) <= bound + half_ulp(reported), percentile

    exact_median, median_bound = reference.quantile_and_bound(50)
    assert abs(Fraction(stats["med"]) - exact_median) <= median_bound + half_ulp(stats["med"])

    mad_bound = reference.mean_magnitude * HALF_BUCKET_RELATIVE_ERROR + half_ulp(stats["mad"])
    assert abs(Fraction(stats["mad"]) - reference.mean_absolute_deviation) <= mad_bound


@pytest.mark.parametrize("seed", SEEDS)
def test_merged_digests_equal_the_digest_of_the_union(seed: int) -> None:
    generator = random.Random(seed)
    samples = draw_samples(generator)
    sources = split_into_sources(generator, samples)

    merged = merge_in_random_grouping(
        generator,
        [round_trip(SampleDigest.from_values(source)) for source in sources],
    )
    union = SampleDigest.from_values(samples)

    assert merged.to_state() == union.to_state()
    assert merged.stats(PERCENTILES) == union.stats(PERCENTILES)


@pytest.mark.parametrize("seed", SEEDS)
def test_sums_are_exact(seed: int) -> None:
    generator = random.Random(seed)
    samples = draw_samples(generator)
    digest = SampleDigest.from_values(samples)

    assert Fraction(digest.sum_units, 1 << SUM_SCALE_BITS) == sum(map(Fraction, samples))
    assert Fraction(digest.square_sum_units, 1 << SQUARE_SUM_SCALE_BITS) == sum(
        Fraction(sample) ** 2 for sample in samples
    )


@pytest.mark.parametrize("seed", SEEDS)
def test_statistics_hold_their_derived_bounds(seed: int) -> None:
    generator = random.Random(seed)
    # Bounded magnitudes: the variance of samples past 2**512 does not fit a float.
    samples = [
        sample
        for sample in draw_samples(generator)
        if abs(sample) < 2**80
    ] or [1.0]

    assert_stats_match_reference(
        SampleDigest.from_values(samples),
        ExactReference([(sample, 1) for sample in samples]),
    )


@pytest.mark.parametrize("seed", SEEDS)
def test_counts_past_int64_merge_exactly(seed: int) -> None:
    """Each sample repeated past 2**63 times: ranks, mean and variance stay exact."""
    generator = random.Random(seed)
    samples = [generator.lognormvariate(0.0, 2.0) for _ in range(generator.randrange(1, 50))]
    repetitions = [generator.randrange(2**62, 2**66) for _ in samples]

    sources = [
        scale_digest(SampleDigest.from_values([sample]), repetition)
        for sample, repetition in zip(samples, repetitions)
    ]
    merged = merge_in_random_grouping(generator, sources)

    assert merged.count == sum(repetitions) > 2**63
    assert_stats_match_reference(merged, ExactReference(list(zip(samples, repetitions))))


def scale_digest(digest: SampleDigest, repetitions: int) -> SampleDigest:
    """The digest of every sample repeated ``repetitions`` times."""
    return SampleDigest(
        digest.count * repetitions,
        digest.sum_units * repetitions,
        digest.square_sum_units * repetitions,
        digest.minimum,
        digest.maximum,
        digest.zero_count * repetitions,
        {key: count * repetitions for key, count in digest.positive_buckets.items()},
        {key: count * repetitions for key, count in digest.negative_buckets.items()},
    )


def test_large_sums_lose_nothing_a_float_accumulator_loses() -> None:
    """Cancellation and many small terms: the exact sum keeps what float sums drop."""
    samples = [1e16, 1.0, -1e16] * 1000 + [0.1] * 100_000

    digest = SampleDigest.from_values(samples)

    exact_sum = sum(map(Fraction, samples))
    assert digest.exact_sum() == exact_sum
    assert float(digest.exact_mean()) == float(exact_sum / len(samples))
    assert sum(samples) != float(exact_sum)


@pytest.mark.parametrize(
    "samples",
    [
        [2.0**-1074] * 3,
        [1e308, -1e308, 2.0**-1074],
        [SPLIT_UPPER_BOUND, -SPLIT_UPPER_BOUND, SPLIT_LOWER_BOUND],
        [math.nextafter(SPLIT_LOWER_BOUND, 0.0), math.nextafter(SPLIT_UPPER_BOUND, 0.0)],
        [2**64 + 1, -(2**64), 0.5],
    ],
    ids=["subnormals", "cancelling-extremes", "split-bounds", "inside-split-bounds", "large-ints"],
)
def test_sums_are_exact_at_the_edges_of_the_arithmetic(samples: list[int | float]) -> None:
    digest = SampleDigest.from_values(samples)

    assert digest.exact_sum() == sum(map(Fraction, samples))
    assert Fraction(digest.square_sum_units, 1 << SQUARE_SUM_SCALE_BITS) == sum(
        Fraction(sample) ** 2 for sample in samples
    )


def test_the_float_sum_expansion_is_exact() -> None:
    generator = random.Random(7)
    samples = [generator.uniform(-1.0, 1.0) * 2.0 ** generator.randrange(-1074, 1000) for _ in range(500)]

    assert Fraction(exact_float_sum_units(samples), 1 << SUM_SCALE_BITS) == sum(map(Fraction, samples))


@pytest.mark.parametrize("invalid_sample", [math.nan, math.inf, -math.inf])
def test_non_finite_samples_are_refused_loudly(invalid_sample: float) -> None:
    with pytest.raises(ValueError):
        SampleDigest.from_values([1.0, invalid_sample])


def test_samples_that_are_not_numbers_are_refused_loudly() -> None:
    with pytest.raises(TypeError):
        SampleDigest.from_values([1.0, "2.0"])


def test_a_digest_needs_a_sample() -> None:
    with pytest.raises(ValueError):
        SampleDigest.from_values([])


def test_a_whole_total_stays_an_int_past_int64() -> None:
    counts = [2**63 - 1, 2**63 - 1, 5]

    assert SampleDigest.from_values(counts).total() == sum(counts)
    assert isinstance(SampleDigest.from_values(counts).total(), int)
