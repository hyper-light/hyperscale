import asyncio

from hyperscale.terminal.components.progress_bar import (
    BarFactory,
    ProgressBarColorConfig,
)


async def run():
    factory = BarFactory()

    bar = factory.create_bar(
        20,
        borders_char="|",
        colors=ProgressBarColorConfig(
            active_color="royal_blue",
            fail_color="white",
            ok_color="hot_pink_3",
        ),
        mode="extended",
    )

    await bar.fit(40)
    await bar.run()

    items = []

    for _ in range(20):
        await asyncio.sleep(1)
        item = await anext(bar)
        items.append(item)

    print(len(items))


asyncio.run(run())
