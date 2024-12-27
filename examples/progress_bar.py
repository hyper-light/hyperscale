import asyncio

from hyperscale.ui.components.progress_bar import (
    BarFactory,
    ProgressBarColorConfig,
)


async def run():
    factory = BarFactory()

    bar = factory.create_bar(
        20,
        colors=ProgressBarColorConfig(
            active_color="royal_blue",
            fail_color="white",
            ok_color="hot_pink_3",
        ),
        mode="extended",
    )

    await bar.fit(31)
    await bar.run()

    items = []

    async for idx in bar:
        await asyncio.sleep(1)
        items.append(idx)


asyncio.run(run())
