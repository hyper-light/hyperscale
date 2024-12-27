import asyncio
import time

from hyperscale.ui.components.windowed_rate import (
    WindowedRate,
    WindowedRateConfig,
)


async def update_loop(rate: WindowedRate):
    start = time.monotonic()
    elapsed = 0

    while elapsed < 120:
        await asyncio.sleep(1)
        await rate.update(amount=1000)

        elapsed = time.monotonic() - start


async def run():
    rate = WindowedRate(
        WindowedRateConfig(),
        color="aquamarine_2",
        mode="extended",
    )

    amount = await rate.get()
    start = time.monotonic()
    elapsed = 0

    update_task = asyncio.create_task(update_loop(rate))

    while elapsed < 120:
        await asyncio.sleep(0.08)
        amount = await rate.get()
        print(amount)

        elapsed = time.monotonic() - start

    await update_task


asyncio.run(run())
