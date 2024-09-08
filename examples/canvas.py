import asyncio

from hyperscale.terminal.components.render_engine.canvas import Canvas
from hyperscale.terminal.components.render_engine.component import Alignment, Component
from hyperscale.terminal.components.render_engine.section import Section, SectionConfig
from hyperscale.terminal.components.text import Text


async def display():
    canvas = Canvas()

    await canvas.initialize(
        [
            Section(
                SectionConfig(
                    width="small",
                    height="xx-small",
                    left_border="|",
                    top_border="-",
                    right_border="|",
                    bottom_border="-",
                    border_color="aquamarine_2",
                    mode="extended",
                ),
                [
                    Component(
                        Text(
                            "Hello!",
                            color="aquamarine_2",
                            mode="extended",
                        ),
                        Alignment(
                            horizontal="center",
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
                    left_border="|",
                    top_border="-",
                    right_border="|",
                    bottom_border="-",
                )
            ),
        ],
        width=102,
        height=30,
    )

    print(await canvas.render())

    await canvas.render()


asyncio.run(display())
