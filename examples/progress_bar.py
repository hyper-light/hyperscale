import asyncio

from hyperscale.terminal.components.progress_bar import (
    BarFactory,
    ProgressBarColorConfig,
)


async def run():
    factory = BarFactory()

    bar = factory.create_bar(
        100,
        borders_char="|",
        colors=ProgressBarColorConfig(
            active_color="royal_blue",
            fail_color="white",
            ok_color="hot_pink_3",
        ),
        mode="extended",
    )

    await bar.fit(12)
    await bar.run()
    async for _ in bar:
        await asyncio.sleep(1)


asyncio.run(run())
