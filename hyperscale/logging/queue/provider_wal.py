from __future__ import annotations

import asyncio
from typing import AsyncIterator, Literal

from hyperscale.logging.exceptions import (
    WALBackpressureError,
    WALConsumerTooSlowError,
)
from hyperscale.logging.models import Log


class ProviderWAL:
    def __init__(
        self,
        max_size: int = 10000,
        put_timeout: float = 30.0,
    ) -> None:
        self._buffer: list[Log | None] = [None] * max_size
        self._max_size = max_size
        self._put_timeout = put_timeout

        self._head: int = 0
        self._tail: int = 0

        self._lock = asyncio.Lock()
        self._not_full = asyncio.Condition()
        self._not_empty = asyncio.Condition()

        self._consumer_positions: dict[str, int] = {}

    @property
    def size(self) -> int:
        return self._tail - self._head

    @property
    def is_full(self) -> bool:
        return self.size >= self._max_size

    @property
    def min_consumer_position(self) -> int:
        if not self._consumer_positions:
            return self._tail
        return min(self._consumer_positions.values())

    async def append(self, log: Log) -> int:
        async with self._lock:
            self._advance_head()

            if self.is_full:
                try:
                    await asyncio.wait_for(
                        self._wait_for_space(),
                        timeout=self._put_timeout,
                    )
                except asyncio.TimeoutError:
                    raise WALBackpressureError(
                        f"Provider WAL full ({self._max_size} entries) for {self._put_timeout}s. "
                        f"Slowest consumer at position {self.min_consumer_position}, "
                        f"head={self._head}, tail={self._tail}."
                    ) from None

            sequence = self._tail
            self._buffer[sequence % self._max_size] = log
            self._tail += 1

        async with self._not_empty:
            self._not_empty.notify_all()

        return sequence

    async def _wait_for_space(self) -> None:
        async with self._not_full:
            while self.is_full:
                await self._not_full.wait()
                self._advance_head()

    def _advance_head(self) -> int:
        min_position = self.min_consumer_position
        entries_discarded = 0

        while self._head < min_position:
            self._buffer[self._head % self._max_size] = None
            self._head += 1
            entries_discarded += 1

        return entries_discarded

    async def read_from(
        self,
        consumer_id: str,
        start_sequence: int | None = None,
    ) -> AsyncIterator[tuple[int, Log]]:
        if start_sequence is None:
            start_sequence = self._consumer_positions.get(consumer_id, self._head)

        current = start_sequence

        while True:
            async with self._not_empty:
                while current >= self._tail:
                    await self._not_empty.wait()

            async with self._lock:
                if current < self._head:
                    raise WALConsumerTooSlowError(
                        f"Consumer '{consumer_id}' at seq {current} but head advanced to {self._head}. "
                        f"Consumer fell too far behind and missed {self._head - current} entries."
                    )

                log = self._buffer[current % self._max_size]
                if log is None:
                    raise RuntimeError(f"WAL corruption: null entry at seq {current}")

            yield current, log
            current += 1

    async def acknowledge(self, consumer_id: str, sequence: int) -> None:
        async with self._lock:
            current_position = self._consumer_positions.get(consumer_id, self._head)

            if sequence < current_position:
                return

            if sequence >= self._tail:
                raise ValueError(
                    f"Cannot acknowledge seq {sequence}, tail is {self._tail}"
                )

            self._consumer_positions[consumer_id] = sequence + 1

            old_head = self._head
            self._advance_head()

            if self._head > old_head:
                async with self._not_full:
                    self._not_full.notify_all()

    def register_consumer(
        self,
        consumer_id: str,
        start_from: Literal["earliest", "latest"] = "earliest",
    ) -> int:
        if start_from == "earliest":
            position = self._head
        elif start_from == "latest":
            position = self._tail
        else:
            raise ValueError(f"Invalid start_from: {start_from}")

        self._consumer_positions[consumer_id] = position
        return position

    def unregister_consumer(self, consumer_id: str) -> None:
        self._consumer_positions.pop(consumer_id, None)

    @property
    def head(self) -> int:
        return self._head

    @property
    def tail(self) -> int:
        return self._tail

    @property
    def consumer_count(self) -> int:
        return len(self._consumer_positions)
