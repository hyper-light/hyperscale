import asyncio
import time

from hyperscale.terminal.components.header import Header, HeaderConfig

from hyperscale.terminal.components.progress_bar import (
    ProgressBar,
    ProgressBarConfig,
)
from hyperscale.terminal.components.render_engine import (
    Alignment,
    Component,
    RenderEngine,
    Section,
    SectionConfig,
)
from hyperscale.terminal.components.scatter_plot import (
    PlotConfig,
    ScatterPlot,
)
from hyperscale.terminal.components.table import (
    Table,
    TableConfig,
)


class Example:

    async def add(self, count: int):
        return count + 1


async def update_timings(timings: list[tuple[int, int]]):
    return timings


async def display():
    bar = ProgressBar(
        ProgressBarConfig(
            data=60,
            active_color="royal_blue",
            failed_color="white",
            complete_color="hot_pink_3",
            terminal_mode="extended",
        )
    )

    sections = [
        Section(
            SectionConfig(height="xx-small", width="full"),
            [
                Component(
                    "header",
                    Header(
                        HeaderConfig(
                            header_text="hyperscale",
                            formatters={
                                "y": [
                                    lambda letter, _: "\n".join(
                                        [" " + line for line in letter.split("\n")]
                                    )
                                ],
                                "l": [
                                    lambda letter, _: "\n".join(
                                        [
                                            line[:-1] if idx == 2 else line
                                            for idx, line in enumerate(
                                                letter.split("\n")
                                            )
                                        ]
                                    )
                                ],
                                "e": [
                                    lambda letter, idx: "\n".join(
                                        [
                                            line[1:] if idx < 2 else line
                                            for idx, line in enumerate(
                                                letter.split("\n")
                                            )
                                        ]
                                    )
                                    if idx == 9
                                    else letter
                                ],
                            },
                            color="aquamarine_2",
                            attributes=["bold"],
                            terminal_mode="extended",
                        )
                    ),
                    Alignment(),
                )
            ],
        ),
        Section(
            SectionConfig(
                width="small",
                height="smallest",
                left_border="|",
                top_border="-",
                bottom_border="-",
            ),
            [
                Component(
                    "progress_bar_example",
                    bar,
                    Alignment(
                        horizontal="left",
                        vertical="center",
                    ),
                    subscriptions=['add'],
                ),
            ],
        ),
        Section(
            SectionConfig(
                height="smallest",
                left_border="|",
                top_border="-",
                right_border="|",
                bottom_border="-",
            ),
        ),
        Section(
            SectionConfig(
                width="small",
                height="xx-small",
                left_border="|",
                top_border="-",
                bottom_border="-",
            )
        ),
        Section(
            SectionConfig(
                width="small",
                height="xx-small",
                left_border="|",
                top_border="-",
                bottom_border="-",
            )
        ),
        Section(
            SectionConfig(
                width="small",
                height="xx-small",
                left_border="|",
                top_border="-",
                right_border="|",
                bottom_border="-",
            )
        ),
        Section(
            SectionConfig(
                width="large",
                height="medium",
                left_border="|",
                top_border="-",
                bottom_border="-",
            ),
            [
                Component(
                    "scatter_test",
                    ScatterPlot(
                        PlotConfig(
                            plot_name="Test",
                            x_axis_name="Time (sec)",
                            y_axis_name="Value",
                            line_color="aquamarine_2",
                            point_char="dot",
                            terminal_mode="extended",
                        ),
                    ),
                    Alignment(
                        horizontal="center",
                    ),
                    horizontal_padding=4,
                ),
            ],
        ),
        Section(
            SectionConfig(
                width="small",
                height="medium",
                left_border="|",
                top_border="-",
                right_border="|",
                bottom_border="-",
            ),
            [
                Component(
                    "table_test",
                    Table(
                        TableConfig(
                            headers={
                                "one": {
                                    "precision": ".2f",
                                    "color": "aquamarine_2",
                                },
                                "two": {
                                    "data_color": lambda value: "hot_pink_3"
                                    if value is None
                                    else None
                                },
                                "three": {
                                    "precision": ".2f",
                                },
                                "four": {},
                            },
                            border_color="aquamarine_2",
                            terminal_mode="extended",
                            table_format="simple",
                        )
                    ),
                    Alignment(
                        horizontal="center",
                    ),
                    horizontal_padding=2,
                ),
            ],
        ),
    ]

    engine = RenderEngine(sections, actions=[Example.add])

    ex = Example()


    await engine.render(
        horizontal_padding=4,
        vertical_padding=1,
    )

    data: list[tuple[int, int]] = []

    elapsed = 0
    start = time.monotonic()

    data = []
    table_data = []

    async for idx in bar:
        await asyncio.sleep(1)

        await ex.add(idx)

        elapsed = time.monotonic() - start

    await engine.stop()


asyncio.run(display())
