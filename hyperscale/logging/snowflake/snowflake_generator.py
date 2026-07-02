from time import time

from .constants import MAX_SEQ
from .snowflake import Snowflake


class SnowflakeGenerator:
    """Snowflake id generator: total and monotone.

    ``generate`` always returns an id. The realtime clock is not
    monotone — NTP slew can step it backwards — so the generator never
    regresses its timestamp cursor: a backwards wall reading reuses the
    latest cursor and keeps sequencing. A sequence exhausted within one
    clock millisecond borrows the next logical millisecond instead of
    failing. Ids are therefore strictly increasing and unique for the
    lifetime of the generator, with no ``None`` escape hatch for
    callers to (mis)handle.
    """

    def __init__(
        self,
        instance: int,
        *,
        seq: int = 0,
        timestamp: int | None = None,
    ):
        current = int(time() * 1000)

        timestamp = timestamp or current

        self._ts = timestamp

        self._inf = instance << 12
        self._seq = seq

    @classmethod
    def from_snowflake(cls, sf: Snowflake) -> "SnowflakeGenerator":
        return cls(sf.instance, seq=sf.seq, timestamp=sf.timestamp)

    def __iter__(self):
        return self

    def generate(self) -> int:
        current = int(time() * 1000)

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
