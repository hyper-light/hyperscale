import asyncio

from hyperscale.terminal.components.progress_bar import (
    BarFactory,
    ProgressBarColorConfig,
)
from hyperscale.terminal.components.render_engine.canvas import Canvas


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
        disable_output=True,
    )

    canvas = Canvas(100, 25)
    # await bar.run()
    # async for _ in bar:
    #     frame = await bar.get_next_frame()
    #     print(frame)
    #     await asyncio.sleep(1)


asyncio.run(run())
