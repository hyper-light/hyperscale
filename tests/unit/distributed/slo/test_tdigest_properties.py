"""
The SLO t-digest (AD-42) against exact quantiles and the algorithm's own
guarantees.

Reference: T. Dunning and O. Ertl, "Computing Extremely Accurate Quantiles
Using t-Digests" (2019), arXiv:1902.04023.

* Scale function k1 (section 2.1): k(q) = delta/(2 pi) * asin(2q - 1), here
  offset by delta/4 so that k(0) = 0 and k(1) = delta/2.
* Merging digest (Algorithm 1): a centroid spans at most one unit of k --
  k(q_right) - k(q_left) <= 1 -- unless it is a single point. Two adjacent
  centroids span more than one unit (else the greedy pass had merged
  them), so the k range of delta/2 holds fewer than delta/2 disjoint pairs:
  floor(|C| / 2) < delta/2, at most delta + 1 centroids however long the
  stream (section 2.1's size bound).
* Interpolation (section 2.3): a centroid's weight is centred on its mean;
  estimates interpolate between adjacent midpoints, from the minimum at
  weight 0 to the maximum at the total. The estimate is therefore
  monotone in q and never leaves [min, max]; q = 0 and q = 1 are the exact
  extremes.
* Accuracy: the estimate for q lies between the means of the two centroids
  whose midpoints bracket q's weight; each spans at most one unit of k and
  both contain q's weight, so the estimate's rank is within
  [k^-1(k(q) - 2), k^-1(k(q) + 2)] -- two units of k either side. That is
  the tolerance below, derived from the configured compression (delta),
  never a fixed epsilon. (The bound is exact for value-disjoint centroids;
  section 3's experiments show the merging digest stays within it on
  streams whose centroids interleave, which the shuffled cases check.)

Ranks are measured with duplicates honoured: an estimate x is right for q
when q lies within [F(x-), F(x)] of the exact sorted data.
"""

import bisect
import math
import random

import pytest

from hyperscale.distributed.env import Env
from hyperscale.distributed.slo.slo_config import SLOConfig
from hyperscale.distributed.slo.tdigest import TDigest

QUANTILES = [0.0, 0.0005, 0.001, 0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999, 0.9995, 1.0]
SAMPLE_COUNT = 20_000
SEEDS = range(3)

DISTRIBUTIONS = {
    "uniform": lambda rng: rng.random(),
    "normal": lambda rng: rng.gauss(0.0, 1.0),
    "lognormal": lambda rng: rng.lognormvariate(0.0, 2.0),
    "pareto_heavy_tail": lambda rng: rng.paretovariate(1.1),
    "duplicates": lambda rng: float(rng.randint(0, 20)),
    "constant": lambda rng: 42.0,
    "bimodal": lambda rng: rng.gauss(10.0, 1.0) if rng.random() < 0.9 else rng.gauss(1000.0, 50.0),
}
ORDERS = {
    "shuffled": lambda values: values,
    "sorted": sorted,
    "reverse_sorted": lambda values: sorted(values, reverse=True),
    # Extremes first, then the body: every compression meets new tails.
    "alternating_extremes": lambda values: [
        value
        for low, high in zip(sorted(values)[: len(values) // 2], reversed(sorted(values)[len(values) // 2 :]))
        for value in (low, high)
    ],
}


def config_for(delta: float) -> SLOConfig:
    return SLOConfig.from_env(Env(SLO_TDIGEST_DELTA=delta))


def k_scale(quantile: float, delta: float) -> float:
    """Dunning & Ertl's k1, offset to start at zero."""
    return delta / (2.0 * math.pi) * math.asin(2.0 * quantile - 1.0) + delta / 4.0


def k_scale_inverse(scaled: float, delta: float) -> float:
    clamped = min(max(scaled, 0.0), delta / 2.0)
    return 0.5 * (math.sin(2.0 * math.pi * (clamped - delta / 4.0) / delta) + 1.0)


def rank_interval(sorted_values: list[float], estimate: float) -> tuple[float, float]:
    count = len(sorted_values)
    return (
        bisect.bisect_left(sorted_values, estimate) / count,
        bisect.bisect_right(sorted_values, estimate) / count,
    )


def assert_within_rank_bound(digest: TDigest, sorted_values: list[float], delta: float, context: object) -> None:
    for quantile in QUANTILES:
        estimate = digest.quantile(quantile)
        lowest_rank, highest_rank = rank_interval(sorted_values, estimate)
        allowed_low = k_scale_inverse(k_scale(quantile, delta) - 2.0, delta)
        allowed_high = k_scale_inverse(k_scale(quantile, delta) + 2.0, delta)
        assert highest_rank >= allowed_low and lowest_rank <= allowed_high, (
            context,
            quantile,
            estimate,
            (lowest_rank, highest_rank),
            (allowed_low, allowed_high),
        )


def assert_size_and_structure(digest: TDigest, delta: float, context: object) -> None:
    digest.quantile(0.5)  # compresses
    centroids = digest._centroids
    assert len(centroids) // 2 < delta / 2.0, (context, len(centroids))
    assert [centroid.mean for centroid in centroids] == sorted(centroid.mean for centroid in centroids), context
    total_weight = sum(centroid.weight for centroid in centroids)
    assert total_weight == pytest.approx(digest.count(), rel=1e-12), context
    weight_before = 0.0
    for centroid in centroids:
        quantile_left = weight_before / total_weight
        quantile_right = (weight_before + centroid.weight) / total_weight
        if centroid.weight > 1.0:
            # Algorithm 1's invariant, in the digest's own arithmetic.
            assert quantile_right <= digest._k_inverse(digest._k(quantile_left) + 1.0), (context, centroid)
        weight_before += centroid.weight


def build(values: list[float], delta: float) -> TDigest:
    digest = TDigest(_config=config_for(delta))
    for value in values:
        digest.add(value)
    return digest


def test_the_scale_function_is_dunning_and_ertls_k1() -> None:
    for delta in (10.0, 100.0, 1000.0):
        digest = TDigest(_config=config_for(delta))
        for step in range(1001):
            quantile = step / 1000
            assert digest._k(quantile) == pytest.approx(k_scale(quantile, delta), abs=1e-12 * delta)
            assert digest._k_inverse(digest._k(quantile)) == pytest.approx(quantile, abs=1e-12)
        assert digest._k(0.0) == 0.0
        assert digest._k(1.0) == pytest.approx(delta / 2.0)
        # Past k(1) the inverse is the whole digest, not a wrap back down.
        assert digest._k_inverse(delta / 2.0 + 0.75) == 1.0


@pytest.mark.parametrize("distribution", sorted(DISTRIBUTIONS))
@pytest.mark.parametrize("order", sorted(ORDERS))
def test_quantiles_are_within_two_units_of_k_of_the_exact_rank(distribution: str, order: str) -> None:
    delta = config_for(Env().SLO_TDIGEST_DELTA).tdigest_delta
    for seed in SEEDS:
        rng = random.Random(seed)
        values = ORDERS[order]([DISTRIBUTIONS[distribution](rng) for _ in range(SAMPLE_COUNT)])
        digest = build(values, delta)
        sorted_values = sorted(values)
        context = (distribution, order, seed)
        assert digest.count() == SAMPLE_COUNT
        assert digest.quantile(0.0) == sorted_values[0]
        assert digest.quantile(1.0) == sorted_values[-1]
        assert_within_rank_bound(digest, sorted_values, delta, context)
        assert_size_and_structure(digest, delta, context)


@pytest.mark.parametrize("delta", [20.0, 50.0, 300.0])
def test_the_bound_follows_the_configured_compression(delta: float) -> None:
    rng = random.Random(int(delta))
    values = [rng.lognormvariate(0.0, 1.5) for _ in range(SAMPLE_COUNT)]
    digest = build(values, delta)
    assert_within_rank_bound(digest, sorted(values), delta, delta)
    assert_size_and_structure(digest, delta, delta)


@pytest.mark.parametrize("order", ["shuffled", "sorted", "reverse_sorted"])
def test_the_digest_stays_bounded_however_long_the_stream(order: str) -> None:
    """Section 2.1: at most delta + 1 centroids, independent of the count."""
    delta = Env().SLO_TDIGEST_DELTA
    rng = random.Random(7)
    values = ORDERS[order]([rng.random() for _ in range(200_000)])
    digest = TDigest(_config=config_for(delta))
    for checkpoint_index, value in enumerate(values, start=1):
        digest.add(value)
        if checkpoint_index % 50_000 == 0:
            assert_size_and_structure(digest, delta, (order, checkpoint_index))


@pytest.mark.parametrize("distribution", ["lognormal", "pareto_heavy_tail", "duplicates", "bimodal"])
def test_the_quantile_function_is_monotone_and_stays_within_the_extremes(distribution: str) -> None:
    rng = random.Random(11)
    values = [DISTRIBUTIONS[distribution](rng) for _ in range(SAMPLE_COUNT)]
    digest = build(values, Env().SLO_TDIGEST_DELTA)
    lowest, highest = min(values), max(values)
    estimates = [digest.quantile(step / 20_000) for step in range(20_001)]
    assert estimates[0] == lowest and estimates[-1] == highest
    assert all(earlier <= later for earlier, later in zip(estimates, estimates[1:]))
    assert all(lowest <= estimate <= highest for estimate in estimates)


@pytest.mark.parametrize("seed", SEEDS)
def test_singleton_centroids_interpolate_exactly_between_midpoints(seed: int) -> None:
    """Section 2.3: a point that is its own centroid sits at its midpoint
    weight, (i + 1/2) of n; between midpoints the estimate is linear, and
    below the first it runs from the minimum at weight zero."""
    delta = Env().SLO_TDIGEST_DELTA
    rng = random.Random(seed)
    # Few enough points that none merge: each spans 1/n of q, more than
    # one unit of k anywhere for this count and delta.
    values = [float(value) for value in rng.sample(range(10_000), 12)]
    digest = build(values, delta)
    digest.quantile(0.5)
    assert len(digest._centroids) == len(values)
    ordered = sorted(values)
    count = len(ordered)
    for index, value in enumerate(ordered):
        assert digest.quantile((index + 0.5) / count) == pytest.approx(value, rel=1e-12)
    for index in range(count - 1):
        between = digest.quantile((index + 1.0) / count)
        assert between == pytest.approx((ordered[index] + ordered[index + 1]) / 2.0, rel=1e-12)
    assert digest.quantile(0.25 / count) == pytest.approx(ordered[0], rel=1e-12)
    assert digest.quantile(1.0 - 0.25 / count) == pytest.approx(ordered[-1], rel=1e-12)


@pytest.mark.parametrize("seed", SEEDS)
def test_the_tails_interpolate_from_the_extremes_to_the_outer_centroids(seed: int) -> None:
    """Section 2.3: below the first centroid's midpoint the estimate runs
    linearly from the minimum (at weight zero) to its mean; above the last
    one's, from its mean to the maximum (at the total weight)."""
    rng = random.Random(seed)
    values = [rng.lognormvariate(0.0, 1.0) for _ in range(SAMPLE_COUNT)]
    digest = build(values, Env().SLO_TDIGEST_DELTA)
    digest.quantile(0.5)
    first, last = digest._centroids[0], digest._centroids[-1]
    total = digest.count()
    assert first.weight > 1.0 and last.weight >= 1.0
    lowest, highest = min(values), max(values)
    for fraction in (0.1, 0.5, 0.9):
        low_quantile = fraction * (first.weight / 2.0) / total
        assert digest.quantile(low_quantile) == pytest.approx(lowest + fraction * (first.mean - lowest), rel=1e-9)
        high_quantile = (total - (1.0 - fraction) * last.weight / 2.0) / total
        assert digest.quantile(high_quantile) == pytest.approx(last.mean + fraction * (highest - last.mean), rel=1e-9)


@pytest.mark.parametrize("seed", SEEDS)
def test_merging_is_commutative_and_associative_within_the_bound(seed: int) -> None:
    delta = Env().SLO_TDIGEST_DELTA
    rng = random.Random(seed)
    partitions = [
        [rng.gauss(50.0, 5.0) for _ in range(rng.randint(1, 9000))],
        [rng.lognormvariate(3.0, 1.0) for _ in range(rng.randint(1, 9000))],
        [rng.paretovariate(1.5) * 20.0 for _ in range(rng.randint(1, 9000))],
    ]
    union = sorted(value for partition in partitions for value in partition)

    def digest_of(index: int) -> TDigest:
        return build(partitions[index], delta)

    merges = {
        "a+b+c": digest_of(0).merge(digest_of(1)).merge(digest_of(2)),
        "c+b+a": digest_of(2).merge(digest_of(1)).merge(digest_of(0)),
        "a+(b+c)": digest_of(0).merge(digest_of(1).merge(digest_of(2))),
        "(c+a)+b": digest_of(2).merge(digest_of(0)).merge(digest_of(1)),
    }
    for name, merged in merges.items():
        context = (seed, name)
        assert merged.count() == len(union), context
        assert merged.quantile(0.0) == union[0] and merged.quantile(1.0) == union[-1], context
        assert_within_rank_bound(merged, union, delta, context)
        assert_size_and_structure(merged, delta, context)


def test_merging_an_empty_digest_changes_nothing() -> None:
    rng = random.Random(3)
    digest = build([rng.random() for _ in range(5000)], Env().SLO_TDIGEST_DELTA)
    before = [digest.quantile(quantile) for quantile in QUANTILES]
    digest.merge(TDigest(_config=config_for(Env().SLO_TDIGEST_DELTA)))
    assert [digest.quantile(quantile) for quantile in QUANTILES] == before
    empty = TDigest(_config=config_for(Env().SLO_TDIGEST_DELTA)).merge(digest)
    assert [empty.quantile(quantile) for quantile in QUANTILES] == before


@pytest.mark.parametrize("seed", SEEDS)
def test_serialization_round_trips_exactly(seed: int) -> None:
    config = config_for(Env().SLO_TDIGEST_DELTA)
    rng = random.Random(seed)
    digest = build([rng.lognormvariate(0.0, 2.0) for _ in range(rng.randint(1, 30_000))], config.tdigest_delta)
    encoded = digest.to_bytes()
    restored = TDigest.from_bytes(encoded, config)
    grid = [step / 1000 for step in range(1001)]
    assert [restored.quantile(quantile) for quantile in grid] == [digest.quantile(quantile) for quantile in grid]
    assert restored.count() == digest.count()
    assert restored.to_bytes() == encoded
    # A restored digest keeps absorbing: as if it had never travelled.
    extra = [rng.random() * 1000.0 for _ in range(3000)]
    for value in extra:
        restored.add(value)
        digest.add(value)
    assert restored.to_bytes() == digest.to_bytes()


def test_an_empty_digest_round_trips_and_rejects_invalid_input() -> None:
    config = config_for(Env().SLO_TDIGEST_DELTA)
    empty = TDigest(_config=config)
    restored = TDigest.from_bytes(empty.to_bytes(), config)
    assert restored.count() == 0.0
    restored.add(5.0)
    assert restored.quantile(0.0) == restored.quantile(1.0) == restored.quantile(0.5) == 5.0
    for invalid_quantile in (-1e-9, 1.0 + 1e-9):
        with pytest.raises(ValueError):
            empty.quantile(invalid_quantile)
    for invalid_weight in (0.0, -1.0):
        with pytest.raises(ValueError):
            empty.add(1.0, invalid_weight)


@pytest.mark.parametrize("seed", SEEDS)
def test_weighted_points_count_as_their_weight(seed: int) -> None:
    """A point of weight w ranks as w unit points of the same value."""
    delta = Env().SLO_TDIGEST_DELTA
    rng = random.Random(seed)
    weighted = [(rng.gauss(0.0, 1.0), rng.randint(1, 20)) for _ in range(3000)]
    digest = TDigest(_config=config_for(delta))
    for value, weight in weighted:
        digest.add(value, float(weight))
    expanded = sorted(value for value, weight in weighted for _ in range(weight))
    assert digest.count() == len(expanded)
    assert_within_rank_bound(digest, expanded, delta, seed)
