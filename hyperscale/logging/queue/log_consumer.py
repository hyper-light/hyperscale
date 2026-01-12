from __future__ import annotations

import asyncio
from typing import (
    TYPE_CHECKING,
    AsyncIterator,
    Callable,
    TypeVar,
)

from hyperscale.logging.models import Log

from .consumer_status import ConsumerStatus

if TYPE_CHECKING:
    from .provider_wal import ProviderWAL

T = TypeVar("T")


class LogConsumer:
    def __init__(
        self,
        consumer_id: str,
        provider_wal: ProviderWAL,
        local_buffer_size: int = 1000,
        ack_interval: int = 100,
    ) -> None:
        self._consumer_id = consumer_id
        self._provider_wal = provider_wal
        self._local_buffer: asyncio.Queue[tuple[int, Log]] = asyncio.Queue(
            maxsize=local_buffer_size
        )
        self._ack_interval = ack_interval

        self._last_acked_sequence: int | None = None
        self._running = False
        self._pull_task: asyncio.Task[None] | None = None
        self.status = ConsumerStatus.READY

    @property
    def pending(self) -> bool:
        return not self._local_buffer.empty()

    @property
    def queue_depth(self) -> int:
        return self._local_buffer.qsize()

    async def start(self) -> None:
        self._running = True
        self.status = ConsumerStatus.RUNNING

        start_position = self._provider_wal.register_consumer(
            self._consumer_id,
            start_from="earliest",
        )

        self._pull_task = asyncio.create_task(self._pull_loop(start_position))

    async def _pull_loop(self, start_sequence: int) -> None:
        try:
            async for sequence, log in self._provider_wal.read_from(
                self._consumer_id,
                start_sequence,
            ):
                if not self._running:
                    break

                await self._local_buffer.put((sequence, log))

        except asyncio.CancelledError:
            pass

        finally:
            self.status = ConsumerStatus.CLOSED

    async def iter_logs(
        self,
        filter: Callable[[T], bool] | None = None,
    ) -> AsyncIterator[Log]:
        pending_sequences: list[int] = []

        while self._running or not self._local_buffer.empty():
            try:
                sequence, log = await asyncio.wait_for(
                    self._local_buffer.get(),
                    timeout=0.1,
                )
            except asyncio.TimeoutError:
                continue

            if filter is None or filter(log.entry):
                yield log

            pending_sequences.append(sequence)

            if len(pending_sequences) >= self._ack_interval:
                await self._acknowledge_batch(pending_sequences)
                pending_sequences.clear()

        if pending_sequences:
            await self._acknowledge_batch(pending_sequences)

    async def _acknowledge_batch(self, sequences: list[int]) -> None:
        if not sequences:
            return

        max_sequence = max(sequences)
        await self._provider_wal.acknowledge(self._consumer_id, max_sequence)
        self._last_acked_sequence = max_sequence

    async def wait_for_pending(self) -> None:
        while not self._local_buffer.empty():
            await asyncio.sleep(0.01)

    async def stop(self) -> None:
        self._running = False
        self.status = ConsumerStatus.CLOSING

        if self._pull_task:
            self._pull_task.cancel()
            try:
                await self._pull_task
            except asyncio.CancelledError:
                pass

        self._provider_wal.unregister_consumer(self._consumer_id)
        self.status = ConsumerStatus.CLOSED

    def abort(self) -> None:
        self._running = False
        self.status = ConsumerStatus.ABORTING

        if self._pull_task:
            self._pull_task.cancel()

        while not self._local_buffer.empty():
            try:
                self._local_buffer.get_nowait()
            except asyncio.QueueEmpty:
                break

        self._provider_wal.unregister_consumer(self._consumer_id)
        self.status = ConsumerStatus.CLOSED
