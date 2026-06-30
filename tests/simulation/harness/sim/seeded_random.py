"""
``Random``-Protocol implementation backed by a seeded
``random.Random`` instance.

Why a seeded Random
-------------------

Determinism requires reproducibility across runs. Production load-
bearing random sites — jittered backoff in retry loops, election
timeout jitter, SWIM gossip peer selection, worker registration
jitter — all draw from the Phase 5 ``Random`` Protocol. Under SIM,
the backing instance is ``random.Random(seed)`` constructed once
per harness with the seed captured in the replay artifact.

Why not the global ``random`` module
------------------------------------

The module-level ``random.random()`` shares state across the
process. SIM runs share an event loop with the determinism gate
itself, the test fixture setup code, and any pytest plugin
internals — every one of those could perturb the shared state
non-deterministically. A per-harness ``random.Random(seed)``
isolates the SIM draws from everything outside.

Note ``choices`` adapter
------------------------

``random.Random.choices`` signature is
``choices(population, weights=None, *, cum_weights=None, k=1)``;
the Protocol shape is keyword-only ``k``. A one-line adapter
bridges them so consumers can call ``self._random.choices(pop, k=n)``
identically against REAL and SIM.
"""

import random
from typing import Sequence, TypeVar


T = TypeVar("T")


class SeededRandom:
    """``Random``-Protocol implementation seeded for replay.

    Construct with an explicit integer seed. The seed is exposed via
    ``self.seed`` so the harness can serialize it into the replay
    artifact. The Protocol method surface is identical to
    ``RealRandom`` so consumers don't see a difference between REAL
    and SIM at the call site.
    """

    __slots__ = ("seed", "_rng")

    def __init__(self, seed: int) -> None:
        self.seed = seed
        self._rng = random.Random(seed)

    def uniform(self, a: float, b: float) -> float:
        """Sample uniformly from ``[a, b]``."""
        return self._rng.uniform(a, b)

    def random(self) -> float:
        """Sample uniformly from ``[0.0, 1.0)``."""
        return self._rng.random()

    def randrange(self, start: int, stop: int | None = None) -> int:
        """Sample uniformly from ``range(start, stop)``."""
        if stop is None:
            return self._rng.randrange(start)
        return self._rng.randrange(start, stop)

    def choice(self, seq: Sequence[T]) -> T:
        """Sample one element uniformly from ``seq``."""
        return self._rng.choice(seq)

    def choices(self, population: Sequence[T], *, k: int) -> list[T]:
        """Sample ``k`` elements with replacement from ``population``.

        Bridges the Protocol's keyword-only ``k`` to
        ``random.Random.choices`` whose own ``k`` is keyword-only.
        """
        return self._rng.choices(population, k=k)

    def sample(self, population: Sequence[T], k: int) -> list[T]:
        """Sample ``k`` distinct elements without replacement from
        ``population``."""
        return self._rng.sample(population, k)
