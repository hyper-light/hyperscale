import asyncio
from collections import defaultdict
from hyperscale.core.engines.client.shared.models import (
    URL as SFTPUrl,
)
from hyperscale.core.testing.models import (
    URL,
    Auth,
    Data,
)
from hyperscale.core.engines.client.shared.timeouts import Timeouts
from .protocols import SFTPProtocol


class MercurySyncSFTPConnction:

    def __init__(
        self,
        pool_size: int | None = None,
        timeouts: Timeouts = Timeouts(),
        reset_connections: bool = False,
    ):
        self._concurrency = pool_size
        self.timeouts = timeouts
        self.reset_connections = reset_connections

        self._dns_lock: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._dns_waiters: dict[str, asyncio.Future] = defaultdict(asyncio.Future)
        self._pending_queue: list[asyncio.Future] = []

        self._client_waiters: dict[asyncio.Transport, asyncio.Future] = {}
        self._connections: list[SFTPProtocol] = []

        self._hosts: dict[str, tuple[str, int]] = {}

        self._semaphore: asyncio.Semaphore = None
        self._connection_waiters: list[asyncio.Future] = []

        self._url_cache: dict[str, SFTPUrl] = {}
        self._optimized: dict[str, URL | Auth | Data ] = {}

    async def _execute(
        self,
    ):
        pass