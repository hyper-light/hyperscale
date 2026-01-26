from __future__ import annotations

import asyncio
from time import time

from .lsn import LSN


class HybridLamportClock:
    def __init__(
        self,
        node_id: int,
        logical_time: int = 0,
        sequence: int = 0,
    ) -> None:
        if not 0 <= node_id <= LSN.MAX_NODE_ID:
            raise ValueError(f"node_id must be 0-{LSN.MAX_NODE_ID}, got {node_id}")

        self._node_id = node_id
        self._logical_time = logical_time
        self._sequence = sequence
        self._last_wall_ms: int = 0
        self._lock = asyncio.Lock()

    @classmethod
    def recover(
        cls,
        node_id: int,
        last_lsn: LSN | None,
    ) -> HybridLamportClock:
        if last_lsn is None:
            return cls(node_id)

        return cls(
            node_id=node_id,
            logical_time=last_lsn.logical_time + 1,
            sequence=0,
        )

    async def generate(self) -> LSN:
        async with self._lock:
            current_wall_ms = int(time() * 1000) & LSN.MAX_WALL_CLOCK

            if current_wall_ms == self._last_wall_ms:
                self._sequence += 1

                if self._sequence > LSN.MAX_SEQUENCE:
                    self._logical_time += 1
                    self._sequence = 0
            else:
                self._last_wall_ms = current_wall_ms
                self._sequence = 0

            self._logical_time += 1

            return LSN(
                logical_time=self._logical_time,
                node_id=self._node_id,
                sequence=self._sequence,
                wall_clock=current_wall_ms,
            )

    async def receive(self, remote_lsn: LSN) -> None:
        async with self._lock:
            if remote_lsn.logical_time >= self._logical_time:
                self._logical_time = remote_lsn.logical_time + 1

    async def witness(self, remote_lsn: LSN) -> None:
        async with self._lock:
            if remote_lsn.logical_time > self._logical_time:
                self._logical_time = remote_lsn.logical_time

    @property
    def current_logical_time(self) -> int:
        return self._logical_time

    @property
    def node_id(self) -> int:
        return self._node_id
