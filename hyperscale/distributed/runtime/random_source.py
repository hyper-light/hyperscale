"""
Random interface — the dependency-injection seam for every
load-bearing non-cryptographic random draw in the distributed
runtime (jittered backoff, election timeout jitter, SWIM gossip
peer selection, worker registration jitter).

Cryptographic randomness is OUT of scope and stays on ``secrets`` /
``os.urandom``:

* AES-GCM nonces and HKDF salts in
  ``hyperscale/distributed/encryption/aes_gcm.py``.
* ``MESSAGE_INCARNATION = secrets.token_bytes(8)`` at
  ``hyperscale/distributed/models/message.py``.
* ``MercurySyncBaseServer._secure_random = secrets.SystemRandom()`` —
  kept as a separate field; the four peer-selection sites that
  currently use it are non-crypto and get redirected to the injected
  ``Random`` in Phase 5b.

Module name is ``random_source`` (not ``random``) to avoid shadowing
the stdlib ``random`` module in callers that need to keep using it
for crypto-adjacent or test purposes.
"""

from typing import Protocol, Sequence, TypeVar


T = TypeVar("T")


class Random(Protocol):
    """Draw from non-cryptographic probability distributions.

    Method surface is the exact set used across the ~43 production
    sites in ``hyperscale/distributed/`` — no speculative widening
    (``gauss``, ``shuffle``, etc.). Phase 6's ``SeededRandom`` will
    implement this Protocol on top of a seeded ``random.Random``.
    """

    def uniform(self, a: float, b: float) -> float:
        """Sample uniformly from ``[a, b]``."""
        ...

    def random(self) -> float:
        """Sample uniformly from ``[0.0, 1.0)``."""
        ...

    def randrange(self, start: int, stop: int | None = None) -> int:
        """Sample uniformly from ``range(start, stop)``."""
        ...

    def choice(self, seq: Sequence[T]) -> T:
        """Sample one element uniformly from ``seq``."""
        ...

    def choices(self, population: Sequence[T], *, k: int) -> list[T]:
        """Sample ``k`` elements with replacement from ``population``."""
        ...

    def sample(self, population: Sequence[T], k: int) -> list[T]:
        """Sample ``k`` distinct elements without replacement from
        ``population``. Raises ``ValueError`` when
        ``k > len(population)``, matching ``random.sample``."""
        ...
