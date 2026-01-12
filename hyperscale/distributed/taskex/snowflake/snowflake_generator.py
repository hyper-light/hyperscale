import asyncio
from time import time
from typing import Optional

from .constants import MAX_SEQ
from .snowflake import Snowflake


class SnowflakeGenerator:
    def __init__(
        self,
        instance: int,
        *,
        seq: int = 0,
        timestamp: Optional[int] = None,
    ):
        current = int(time() * 1000)

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
        current = int(time() * 1000)

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
            current = int(time() * 1000)

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
