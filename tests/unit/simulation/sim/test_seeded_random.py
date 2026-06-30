"""
Unit tests for ``SeededRandom``.

The Random seam contract: production code calls
``self._random.uniform(...)`` etc. through the Phase 5 Protocol.
Under SIM, the backing is ``random.Random(seed)`` for replay.

Tests verify:

1. Same seed → identical draw sequence (the replay property).
2. Different seeds → different draw sequences (the seed actually
   matters).
3. Every Protocol method is implemented and returns a value of
   the right type.
4. The ``choices`` adapter respects the keyword-only ``k``.
"""

import pytest

from tests.simulation.harness.sim import SeededRandom


def test_same_seed_produces_identical_draws() -> None:
    """Two ``SeededRandom`` instances with the same seed return the
    same draw sequence — the replay property."""
    rng_a = SeededRandom(seed=42)
    rng_b = SeededRandom(seed=42)
    for _ in range(100):
        assert rng_a.random() == rng_b.random()


def test_different_seeds_diverge() -> None:
    """Different seeds produce different draw sequences."""
    rng_a = SeededRandom(seed=1)
    rng_b = SeededRandom(seed=2)
    draws_a = [rng_a.random() for _ in range(50)]
    draws_b = [rng_b.random() for _ in range(50)]
    assert draws_a != draws_b


def test_uniform_within_bounds() -> None:
    """``uniform(a, b)`` returns a float in ``[a, b]``."""
    rng = SeededRandom(seed=0)
    for _ in range(100):
        value = rng.uniform(1.0, 5.0)
        assert 1.0 <= value <= 5.0


def test_random_in_unit_interval() -> None:
    """``random()`` returns a float in ``[0, 1)``."""
    rng = SeededRandom(seed=0)
    for _ in range(100):
        value = rng.random()
        assert 0.0 <= value < 1.0


def test_randrange_single_arg() -> None:
    """``randrange(stop)`` returns an int in ``range(stop)``."""
    rng = SeededRandom(seed=0)
    for _ in range(100):
        value = rng.randrange(10)
        assert 0 <= value < 10


def test_randrange_two_arg() -> None:
    """``randrange(start, stop)`` returns an int in ``range(start, stop)``."""
    rng = SeededRandom(seed=0)
    for _ in range(100):
        value = rng.randrange(5, 15)
        assert 5 <= value < 15


def test_choice() -> None:
    """``choice(seq)`` returns an element from ``seq``."""
    rng = SeededRandom(seed=0)
    population = ["a", "b", "c", "d", "e"]
    for _ in range(50):
        assert rng.choice(population) in population


def test_choices_with_replacement() -> None:
    """``choices(pop, k=k)`` returns ``k`` elements (with replacement)."""
    rng = SeededRandom(seed=0)
    population = [1, 2, 3, 4, 5]
    result = rng.choices(population, k=10)
    assert len(result) == 10
    assert all(item in population for item in result)


def test_sample_without_replacement() -> None:
    """``sample(pop, k)`` returns ``k`` distinct elements."""
    rng = SeededRandom(seed=0)
    population = list(range(20))
    result = rng.sample(population, 5)
    assert len(result) == 5
    assert len(set(result)) == 5
    assert all(item in population for item in result)


def test_sample_k_larger_than_population_raises() -> None:
    """Matches ``random.Random.sample``: raises when ``k > len(pop)``."""
    rng = SeededRandom(seed=0)
    with pytest.raises(ValueError):
        rng.sample([1, 2, 3], 10)


def test_seed_is_exposed() -> None:
    """``self.seed`` is publicly readable so the harness can
    serialize it into the replay artifact."""
    rng = SeededRandom(seed=12345)
    assert rng.seed == 12345
