import asyncio
import functools
from typing import Callable, Dict, Optional, Tuple

from hyperscale.distributed.discovery.dns.core.record import RecordType
from hyperscale.distributed.models.dns import DNSMessage


class Memoizer:
    def __init__(self):
        self.data: Dict[str, asyncio.Task] = {}

    def memoize_async(
        self,
        key: Callable[
            [Tuple[Optional[DNSMessage], str, RecordType]], Tuple[str, RecordType]
        ] = None,
    ):
        data = self.data

        def wrapper(func):
            @functools.wraps(func)
            async def wrapped(*args, **kwargs):
                cache_key = ()
                if key:
                    cache_key = key

                task = data.get(cache_key)

                if task is None:
                    task = asyncio.create_task(func(*args, **kwargs))

                    data[cache_key] = task

                    task.add_done_callback(lambda _: self.clear(cache_key))

                return await task

            return wrapped

        return wrapper

    def clear(self, key: str):
        self.data.pop(key, None)
