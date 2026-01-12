from __future__ import annotations

import asyncio
import uuid

from hyperscale.logging.models import Log

from .log_consumer import LogConsumer
from .provider_status import ProviderStatus
from .provider_wal import ProviderWAL


class LogProvider:
    def __init__(
        self,
        wal_size: int = 10000,
        put_timeout: float = 30.0,
    ) -> None:
        self._wal = ProviderWAL(max_size=wal_size, put_timeout=put_timeout)
        self._consumers: dict[str, LogConsumer] = {}
        self._close_waiter: asyncio.Future[None] | None = None
        self.closing: bool = False
        self.status = ProviderStatus.READY

    @property
    def subscriptions_count(self) -> int:
        return len(self._consumers)

    def create_consumer(
        self,
        consumer_id: str | None = None,
        local_buffer_size: int = 1000,
        ack_interval: int = 100,
    ) -> LogConsumer:
        if consumer_id is None:
            consumer_id = str(uuid.uuid4())

        consumer = LogConsumer(
            consumer_id=consumer_id,
            provider_wal=self._wal,
            local_buffer_size=local_buffer_size,
            ack_interval=ack_interval,
        )

        self._consumers[consumer_id] = consumer
        return consumer

    def subscribe(self, consumer: LogConsumer) -> None:
        if self.status == ProviderStatus.READY:
            self.status = ProviderStatus.RUNNING

        if self.status == ProviderStatus.RUNNING:
            self._consumers[consumer._consumer_id] = consumer

    async def put(self, log: Log) -> int:
        if self.status == ProviderStatus.READY:
            self.status = ProviderStatus.RUNNING

        return await self._wal.append(log)

    async def unsubscribe(self, consumer_id: str) -> None:
        consumer = self._consumers.pop(consumer_id, None)
        if consumer:
            await consumer.stop()

    async def signal_shutdown(self) -> None:
        self.status = ProviderStatus.CLOSING
        self.closing = True

        for consumer in self._consumers.values():
            await consumer.stop()

        self._consumers.clear()
        self.status = ProviderStatus.CLOSED

    def abort(self) -> None:
        self.status = ProviderStatus.CLOSING
        self.closing = True

        for consumer in self._consumers.values():
            consumer.abort()

        self._consumers.clear()
        self.status = ProviderStatus.CLOSED

    @property
    def wal(self) -> ProviderWAL:
        return self._wal
