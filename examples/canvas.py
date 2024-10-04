import asyncio

from hyperscale.terminal.components.progress_bar import (
    BarFactory,
    ProgressBarColorConfig,
)
from hyperscale.terminal.components.render_engine import (
    Alignment,
    Component,
    RenderEngine,
    Section,
    SectionConfig,
)
from hyperscale.terminal.components.text import Text


async def display():
    engine = RenderEngine()

    factory = BarFactory()

    bar = factory.create_bar(
        size=20,
        colors=ProgressBarColorConfig(
            active_color="royal_blue",
            fail_color="white",
            ok_color="hot_pink_3",
        ),
        mode="extended",
        disable_output=True,
    )

    await engine.initialize(
        [
            Section(
                SectionConfig(
                    width="small",
                    height="xx-small",
                    top_border=" ",
                    bottom_border=" ",
                    border_color="aquamarine_2",
                    mode="extended",
                ),
                [
                    Component(
                        bar,
                        Alignment(
                            horizontal="left",
                            vertical="center",
                            priority="high",
                        ),
                    ),
                    Component(
                        Text("Hello!"),
                        Alignment(
                            horizontal="right",
                            vertical="center",
                            priority="low",
                        ),
                    ),
                ],
            ),
            Section(
                SectionConfig(
                    width="large",
                    height="xx-small",
                    left_border="|",
                    top_border="-",
                    right_border="|",
                    bottom_border="-",
                )
            ),
            Section(
                SectionConfig(
                    width="small",
                    height="small",
                    left_border="|",
                    top_border="-",
                    right_border="|",
                    bottom_border="-",
                )
            ),
            Section(
                SectionConfig(
                    width="small",
                    height="small",
                    left_border="|",
                    top_border="-",
                    right_border="|",
                    bottom_border="-",
                )
            ),
            Section(
                SectionConfig(
                    width="small",
                    height="small",
                    left_border="|",
                    top_border="-",
                    right_border="|",
                    bottom_border="-",
                )
            ),
            Section(
                SectionConfig(
                    width="full",
                    height="small",
                    top_border="-",
                    bottom_border="-",
                )
            ),
        ],
    )

    await engine.render()
    items = []

    async for idx in bar:
        await asyncio.sleep(0.5)
        items.append(idx)

    await engine.stop()


asyncio.run(display())
