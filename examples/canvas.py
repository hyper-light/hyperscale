import asyncio

from hyperscale.terminal.components.progress_bar import (
    BarFactory,
    ProgressBarColorConfig,
)
from hyperscale.terminal.components.render_engine.canvas import Canvas
from hyperscale.terminal.components.render_engine.component import Alignment, Component
from hyperscale.terminal.components.render_engine.section import Section, SectionConfig


async def display():
    canvas = Canvas()

    factory = BarFactory()

    bar = factory.create_bar(
        26,
        colors=ProgressBarColorConfig(
            active_color="royal_blue",
            fail_color="white",
            ok_color="hot_pink_3",
        ),
        mode="extended",
        disable_output=True,
    )

    await canvas.initialize(
        [
            Section(
                SectionConfig(
                    width="small",
                    height="xx-small",
                    left_border="|",
                    top_border=" ",
                    right_border="|",
                    bottom_border=" ",
                    border_color="aquamarine_2",
                    mode="extended",
                ),
                [
                    Component(
                        bar,
                        Alignment(
                            horizontal="right",
                            vertical="center",
                        ),
                    )
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
        width=103,
        height=30,
    )

    print(await canvas.render())

    await canvas.render()


asyncio.run(display())
