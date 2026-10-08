import asyncio
from typing import Optional

from hyperscale.distributed.runtime import Clock, RealClock

from .constants import MAX_SEQ
from .snowflake import Snowflake


_DEFAULT_CLOCK: Clock = RealClock()


class SnowflakeGenerator:
    """Snowflake ID generator: total and monotone.

    ``generate`` / ``generate_sync`` always return an id, and ids are
    strictly increasing for the lifetime of the generator. The realtime
    clock is not monotone — NTP slew can step it backwards — so the
    generator never regresses its ``_ts`` cursor: a backwards wall
    reading reuses the latest cursor and keeps sequencing, and a
    sequence exhausted within one clock millisecond (virtual-time
    bursts decouple id rate from wall time entirely) borrows the next
    logical millisecond instead of failing. The prior behavior —
    returning ``None`` on regression/exhaustion — pushed an unhandleable
    case onto callers: ``message.py`` blocked the event loop in a
    ``time.sleep`` retry (a deadlock under a frozen virtual clock), and
    a ``None`` shard id reaching a wire consumer raised ``TypeError``
    deep in the receive path.

    Phase 5 DI: accepts ``clock: Clock | None = None`` as a
    keyword-only argument. Defaults to the module-level ``RealClock``
    singleton; under SIM a ``VirtualClock`` (which only advances) makes
    ids follow virtual time with the same total, monotone contract.
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
        return cls(sf.instance, seq=sf.seq, timestamp=sf.timestamp)

    def __iter__(self):
        return self

    def _next_id(self) -> int:
        """Advance the cursor and compose the next id (single-threaded)."""
        current = int(self._clock.time() * 1000)

        # Never regress: a backwards realtime step reuses the latest
        # cursor so ids stay unique and ordered.
        if current < self._ts:
            current = self._ts

        if self._ts == current:
            if self._seq == MAX_SEQ:
                # Sequence exhausted within this millisecond: borrow the
                # next logical millisecond rather than failing.
                current += 1
                self._seq = 0
            else:
                self._seq += 1

        else:
            self._seq = 0

        self._ts = current

        return self._ts << 22 | self._inf | self._seq

    def generate_sync(self) -> int:
        """
        Synchronous generation - use only from non-async contexts.
        NOT thread-safe - caller must ensure single-threaded access.
        """
        return self._next_id()

    async def generate(self) -> int:
        """Async generation with lock protection."""
        async with self._get_lock():
            return self._next_id()
