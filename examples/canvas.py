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
                    height="medium",
                    left_border="|",
                    top_border="-",
                    bottom_border=" ",
                    left_padding=1,
                ),
                [
                    Component(
                        Text(
                            "Hello!",
                            color="aquamarine_2",
                            mode="extended",
                        ),
                        Alignment(
                            horizontal="left",
                            vertical="bottom",
                        ),
                    )
                ],
            ),
            Section(
                SectionConfig(
                    width="large",
                    height="medium",
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
        ]
    )

    print(await canvas.render())

    await canvas.render()


asyncio.run(display())
