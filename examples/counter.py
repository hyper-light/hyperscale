import asyncio

from hyperscale.ui.components.counter import Counter


async def run():
    counter = Counter(
        color="aquamarine_2",
        mode="extended",
    )

    await counter.update(amount=2e10)
    count = await counter.get()

    print(count)


asyncio.run(run())
