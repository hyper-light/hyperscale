"""
Default ``Random`` implementation that delegates to the stdlib
``random`` module-level functions. Phase 5 production code uses this;
Phase 6 SIM mode substitutes a ``SeededRandom`` backed by an
explicitly-seeded ``random.Random`` instance.

Implementation note: same zero-overhead shape as ``RealClock`` —
every method is an instance attribute bound to the stdlib function
at construction time, not a class-level method. ``self._random.sample``
is one Python attribute lookup plus the stdlib call, with no
delegating wrapper frame. The benchmark numbers documented in
``real_clock.py`` apply identically here.

``choices`` needs a thin adapter because the Clock-Protocol-style
``choices(population, *, k)`` keyword shape doesn't directly match
``random.choices(population, *, weights=None, cum_weights=None, k=1)``
in a way that preserves the positional/keyword distinction; calling
the stdlib function as ``random.choices(population, k=k)`` keeps the
adapter at a single line.
"""

import random
from typing import Sequence, TypeVar


T = TypeVar("T")


def _choices_adapter(population: Sequence[T], *, k: int) -> list[T]:
    """Bridge the Protocol's keyword-only ``k`` form to
    ``random.choices(population, k=k)``."""
    return random.choices(population, k=k)


class RealRandom:
    """Stdlib-backed ``Random`` implementation. Stateless wrapper
    around the module-level ``random`` functions; the underlying RNG
    state lives in the process-global ``random`` module.

    Every method is an instance attribute set at construction time so
    ``self._random.sample(...)`` is one attribute lookup plus the
    underlying call — no method-bind overhead in the hot probe /
    election / gossip-target-selection paths.
    """

    __slots__ = (
        "uniform",
        "random",
        "randrange",
        "choice",
        "choices",
        "sample",
    )

    def __init__(self) -> None:
        self.uniform = random.uniform
        self.random = random.random
        self.randrange = random.randrange
        self.choice = random.choice
        self.choices = _choices_adapter
        self.sample = random.sample
