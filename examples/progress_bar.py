import asyncio

from hyperscale.terminal.components.progress_bar import (
    BarFactory,
    ProgressBarColorConfig,
)


async def run():
    factory = BarFactory()

    bar = factory.create_bar(
        10,
        borders_char="block_brace",
        colors=ProgressBarColorConfig(
            active_color="aquamarine_2",
            fail_color="hot_pink_3",
            ok_color="white",
        ),
    )

    bar.run()

    for _ in range(10):
        bar.update()
        await asyncio.sleep(1)


asyncio.run(run())
