import asyncio
from typing import List

from hyperscale.logging.models import Log

from .consumer_status import ConsumerStatus
from .log_consumer import LogConsumer
from .provider_status import ProviderStatus


class LogProvider:

    def __init__(self) -> None:
        self._close_waiter: asyncio.Future | None = None
        self.closing: bool = False
        self._consumers: List[LogConsumer] = []
        self.status = ProviderStatus.READY

    @property
    def subscriptions_count(self):
        return len(self._consumers)

    def subscribe(self, consumer: LogConsumer):

        if self.status == ProviderStatus.READY:
            self.status = ProviderStatus.RUNNING

        if self.status == ProviderStatus.RUNNING:
            self._consumers.append(consumer)

    async def put(self, log: Log):

        if self.status == ProviderStatus.RUNNING:
            await asyncio.gather(*[
                consumer.put(log) for consumer in self._consumers if consumer.status in [
                    ConsumerStatus.READY,
                    ConsumerStatus.RUNNING,
                ]
            ])
                    

        await asyncio.sleep(0)

    async def signal_shutdown(self):
        self.status = ProviderStatus.CLOSING

        for consumer in self._consumers:
            consumer.stop()

            if consumer.pending:
                await consumer.wait_for_pending()

        self.status = ProviderStatus.CLOSED



