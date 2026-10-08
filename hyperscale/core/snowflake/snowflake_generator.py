from time import time

from .constants import MAX_INSTANCE, MAX_SEQ
from .snowflake import Snowflake

# Runtime-default time source (seconds, ``time.time`` semantics) — the
# SIM seam rebinding point, mirror of the hook in
# ``hyperscale/logging/snowflake``. This class mints the executor
# protocols' shard ids, whose PARSED timestamps drive LWW context
# ordering at the receive sites — with wall readings, the relative
# order of two children's same-window updates flipped with host
# scheduling (the chaos VOPR's residual 5/100 twin forks after the
# instance component was made address-derived). Under SIM the swap
# machinery rebinds this to the virtual clock's wall model; REAL mode
# keeps the realtime default, and the never-regress cursor absorbs
# backwards steps from either source.
_DEFAULT_TIME_SOURCE = time


class SnowflakeGenerator:
    """Snowflake id generator: total and monotone.

    ``generate`` always returns an id. The realtime clock is not
    monotone — NTP slew can step it backwards — so the generator never
    regresses its timestamp cursor: a backwards wall reading reuses the
    latest cursor and keeps sequencing. A sequence exhausted within one
    clock millisecond (virtual-time bursts decouple send rate from wall
    time entirely) borrows the next logical millisecond instead of
    failing. Ids are therefore strictly increasing and unique for the
    lifetime of the generator, with no ``None`` escape hatch for
    callers to (mis)handle — a ``None`` shard id on the wire parsed as
    a Snowflake raises ``TypeError`` deep in the receive path, which is
    exactly the class of sporadic, wall-clock-triggered failure this
    guards against.
    """

    def __init__(
        self,
        instance: int,
        *,
        seq: int = 0,
        timestamp: int | None = None,
    ):
        current = int(_DEFAULT_TIME_SOURCE() * 1000)

        timestamp = timestamp or current

        self._ts = timestamp

        # Mask instance to 10 bits to fit Snowflake format
        self._inf = (instance & MAX_INSTANCE) << 12
        self._seq = seq

    @classmethod
    def from_snowflake(cls, sf: Snowflake) -> "SnowflakeGenerator":
        return cls(sf.instance, seq=sf.seq, timestamp=sf.timestamp)

    def __iter__(self):
        return self

    def generate(self) -> int:
        current = int(_DEFAULT_TIME_SOURCE() * 1000)

        # Never regress: a backwards realtime step reuses the latest
        # cursor so ids stay unique and ordered.
        if current < self._ts:
            current = self._ts

        if self._ts == current:
            if self._seq == MAX_SEQ:
                # Sequence exhausted within this millisecond: borrow the
                # next logical millisecond rather than failing the send.
                current += 1
                self._seq = 0
            else:
                self._seq += 1

        else:
            self._seq = 0

        self._ts = current

        return self._ts << 22 | self._inf | self._seq
