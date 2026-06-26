"""
Default ``Random`` implementation that delegates to the stdlib
``random`` module-level functions. Phase 5 production code uses this;
Phase 6 SIM mode substitutes a ``SeededRandom`` backed by an
explicitly-seeded ``random.Random`` instance.

Implementation note: every method delegates to the stdlib equivalent
without modification — the seam exists to be swappable, not to add
behavior.
"""

import random
from typing import Sequence, TypeVar


T = TypeVar("T")


class RealRandom:
    """Stdlib-backed ``Random`` implementation. Stateless wrapper
    around the module-level ``random`` functions; the underlying RNG
    state lives in the process-global ``random`` module."""

    def uniform(self, a: float, b: float) -> float:
        return random.uniform(a, b)

    def random(self) -> float:
        return random.random()

    def randrange(self, start: int, stop: int | None = None) -> int:
        if stop is None:
            return random.randrange(start)
        return random.randrange(start, stop)

    def choice(self, seq: Sequence[T]) -> T:
        return random.choice(seq)

    def choices(self, population: Sequence[T], *, k: int) -> list[T]:
        return random.choices(population, k=k)

    def sample(self, population: Sequence[T], k: int) -> list[T]:
        return random.sample(population, k)
