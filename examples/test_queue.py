import asyncio
import random
import time

from hyperscale.core.jobs.data_structures import LockedSet


async def add(idx: int, locked: LockedSet):
    locked.put_no_wait(idx)


async def read(locked: LockedSet[int]):
    return await locked.get()


async def generate(total: int):
    for idx in range(total):
        yield idx, random.randrange(0, 10**6)


async def run():
    locked = LockedSet()

    start = time.monotonic()
    await asyncio.gather(
        *[
            add(idx, locked) if rand % 2 == 0 else read(locked)
            async for idx, rand in generate(10**5)
        ]
    )

    print(time.monotonic() - start)


asyncio.run(run())
