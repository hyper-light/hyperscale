import asyncio
import time

from hyperscale.terminal.components.rate import Rate, RateConfig


async def run():
    rate = Rate(
        RateConfig(
            rate_period=1,
            rate_unit="s",
        ),
        color="aquamarine_2",
        mode="extended",
    )

    amount = await rate.get()
    start = time.monotonic()
    elapsed = 0

    while elapsed < 120:
        await rate.update(amount=100)
        amount = await rate.get()
        await asyncio.sleep(1)
        print(amount)

        elapsed = time.monotonic() - start


asyncio.run(run())
