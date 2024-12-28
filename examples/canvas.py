import asyncio
import time

from hyperscale.ui.components.header import Header, HeaderConfig

from hyperscale.ui.components.progress_bar import (
    ProgressBar,
    ProgressBarConfig,
)
from hyperscale.ui.components.terminal import (
    Alignment,
    Component,
    Terminal,
    Section,
    SectionConfig,
    action
)
from hyperscale.ui.components.counter import (
    Counter,
    CounterConfig
)
from hyperscale.ui.components.scatter_plot import (
    PlotConfig,
    ScatterPlot,
)
from hyperscale.ui.components.table import (
    Table,
    TableConfig,
)



@action(alias='add_to_total')
async def add(count: int):
    return count + 1


@action()
async def update_timings(timings: list[tuple[int, int]]):
    return timings


@action()
async def update_table(rows: list[dict[str, int]]):
    return rows


async def display():

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
                    ProgressBar(
                        ProgressBarConfig(
                            total=60,
                            active_color="royal_blue",
                            failed_color="white",
                            complete_color="hot_pink_3",
                            terminal_mode="extended",
                        )
                    ),
                    Alignment(
                        horizontal="left",
                        vertical="center",
                    ),
                    subscriptions=['add_to_total'],
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
            ),
            [
                Component(
                    'counter',
                    Counter(
                        CounterConfig(
                            terminal_mode='extended'
                        )
                    ),
                    subscriptions=['add_to_total']
                )
            ]
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
                    subscriptions=['update_timings'],
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
                    subscriptions=['update_table'],
                    horizontal_padding=2,
                ),
            ],
        ),
    ]

    engine = Terminal(sections)


    await engine.render(
        horizontal_padding=4,
        vertical_padding=1,
    )

    data: list[tuple[int, int]] = []

    elapsed = 0
    start = time.monotonic()

    data = []
    table_data = []

    for idx in range(60):
        await asyncio.sleep(1)

        data.append((elapsed, idx))

        if idx < 15:
            table_data.append({'one': idx})
            await update_table(table_data)

        await add(idx)
        await update_timings(data)
        
        elapsed = time.monotonic() - start

    await engine.stop()


asyncio.run(display())
