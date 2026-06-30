import asyncio
from typing import Optional

from hyperscale.distributed.runtime import Clock, RealClock

from .constants import MAX_SEQ
from .snowflake import Snowflake


_DEFAULT_CLOCK: Clock = RealClock()


class SnowflakeGenerator:
    """Snowflake ID generator with monotonic-non-decreasing IDs.

    The generator explicitly returns ``None`` from ``generate`` /
    ``generate_sync`` when its internal ``_ts`` cursor would regress
    relative to the wall-clock reading. That is the invariant Phase 6
    SIM mode must preserve: a ``VirtualClock`` only advances, so
    ``VirtualClock.time()`` never decreases between calls — IDs stay
    strictly monotonic in simulated runs without any special handling
    on the SIM side.

    Phase 5 DI: accepts ``clock: Clock | None = None`` as a
    keyword-only argument. Defaults to the module-level ``RealClock``
    singleton so legacy callers (``hyperscale/distributed/models/message.py:30``
    and the SnowflakeGenerator constructions in tests) see byte-
    identical behavior. Wall-time is read via ``self._clock.time()``
    rather than the prior ``from time import time; time()`` form,
    closing the Plan Watch out #1 gap that originally bypassed the
    Phase 5 seam.
    """

    def __init__(
        self,
        instance: int,
        *,
        seq: int = 0,
        timestamp: Optional[int] = None,
        clock: Clock | None = None,
    ):
        self._clock: Clock = clock if clock is not None else _DEFAULT_CLOCK

        current = int(self._clock.time() * 1000)

        timestamp = timestamp or current

        self._ts = timestamp

        self._inf = instance << 12
        self._seq = seq
        self._lock: asyncio.Lock | None = None

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    @classmethod
    def from_snowflake(cls, sf: Snowflake) -> "SnowflakeGenerator":
        return cls(sf.instance, seq=sf.seq, epoch=sf.epoch, timestamp=sf.timestamp)

    def __iter__(self):
        return self

    def generate_sync(self) -> Optional[int]:
        """
        Synchronous generation - use only from non-async contexts.
        NOT thread-safe - caller must ensure single-threaded access.
        """
        current = int(self._clock.time() * 1000)

        if self._ts == current:
            if self._seq == MAX_SEQ:
                return None

            self._seq += 1

        elif self._ts > current:
            return None

        else:
            self._seq = 0

        self._ts = current

        return self._ts << 22 | self._inf | self._seq

    async def generate(self) -> Optional[int]:
        """Async generation with lock protection."""
        async with self._get_lock():
            current = int(self._clock.time() * 1000)

            if self._ts == current:
                if self._seq == MAX_SEQ:
                    return None

                self._seq += 1

            elif self._ts > current:
                return None

            else:
                self._seq = 0

            self._ts = current

            return self._ts << 22 | self._inf | self._seq
