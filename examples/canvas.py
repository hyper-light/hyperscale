import asyncio
import time

from hyperscale.ui.components.header import Header, HeaderConfig

from hyperscale.ui.components.progress_bar import (
    ProgressBar,
    ProgressBarConfig,
)
from hyperscale.ui.components.terminal import (
    Terminal,
    Section,
    SectionConfig,
    action
)
from hyperscale.ui.components.counter import (
    Counter,
    CounterConfig
)
from hyperscale.ui.components.total_rate import (
    TotalRate,
    TotalRateConfig
)
from hyperscale.ui.components.windowed_rate import (
    WindowedRate,
    WindowedRateConfig
)
from hyperscale.ui.components.scatter_plot import (
    PlotConfig,
    ScatterPlot,
)
from hyperscale.ui.components.table import (
    Table,
    TableConfig,
)
from hyperscale.ui.components.timer import (
    Timer,
    TimerConfig,
)


@action(alias='add_to_total')
async def add(count: int):
    return count + 1

@action()
async def update_rate(data: list[tuple[int | float, float]]):
    return data


@action()
async def update_timings(timings: list[tuple[int, int]]):
    return timings

@action()
async def update_table(rows: list[dict[str, int]]):
    return rows

@action()
async def update_timer():
    return


async def display():

    sections = [
        Section(
            SectionConfig(
                height="xx-small", 
                width="full"
            ),
            component=Header(
                "header",
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
        ),
        Section(
            SectionConfig(
                width="small",
                height="smallest",
                left_border="|",
                top_border="-",
                bottom_border="-",
                horizontal_alignment="left",
                vertical_alignment="center",
            ),
            component=ProgressBar(
                "progress_bar_example",
                ProgressBarConfig(
                    total=60,
                    active_color="royal_blue",
                    failed_color="white",
                    complete_color="hot_pink_3",
                    terminal_mode="extended",
                )
            ),
            subscriptions=['add_to_total'],
        ),
        Section(
            SectionConfig(
                width='large',
                height="smallest",
                left_border="|",
                top_border="-",
                right_border="|",
                bottom_border="-",
            ),
        ),
        Section(
            SectionConfig(
                width="x-small",
                height="xx-small",
                left_border="|",
                top_border="-",
                bottom_border="-",
            ),
            component=Counter(
                'counter',
                CounterConfig(
                    unit='total actions',
                    terminal_mode='extended'
                )
            ),
            subscriptions=['add_to_total']
        ),
        Section(
            SectionConfig(
                width="x-small",
                height="xx-small",
                left_border="|",
                top_border="-",
                bottom_border="-",
            ),
            component=TotalRate(
                'total_rate',
                TotalRateConfig(
                    unit='aps',
                    terminal_mode='extended'
                )
            ),
            subscriptions=['add_to_total']
        ),
        Section(
            SectionConfig(
                width="x-small",
                height="xx-small",
                left_border="|",
                top_border="-",
                bottom_border="-",
            ),
            component=WindowedRate(
                'windowed_rate',
                WindowedRateConfig(
                    unit='aps',
                    rate_period=5
                )
            ),
            subscriptions=['update_rate']
        ),
        Section(
            SectionConfig(
                height="xx-small",
                left_border="|",
                top_border="-",
                right_border="|",
                bottom_border="-",
            ),
            component=Timer(
                'timer',
                TimerConfig(
                    color='aquamarine_2',
                    terminal_mode='extended',
                )
            ),
            subscriptions=['update_timer']
        ),
        Section(
            SectionConfig(
                width="large",
                height="medium",
                left_border="|",
                top_border="-",
                bottom_border="-",
                left_padding=4,
                right_padding=4,
                horizontal_alignment="center",
            ),
            component=ScatterPlot(
                "scatter_test",
                PlotConfig(
                    plot_name="Test",
                    x_axis_name="Time (sec)",
                    y_axis_name="Value",
                    line_color="aquamarine_2",
                    point_char="dot",
                    terminal_mode="extended",
                ),
            ),
            subscriptions=['update_timings'],
        ),
        Section(
            SectionConfig(
                width="small",
                height="medium",
                left_border="|",
                top_border="-",
                right_border="|",
                bottom_border="-",
                left_padding=2,
                right_padding=2,
                horizontal_alignment="center",
            ),
            component=Table(
                "table_test",
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
                        "five": {}
                    },
                    minimum_column_width=10,
                    border_color="aquamarine_2",
                    terminal_mode="extended",
                    table_format="simple",
                )
            ),
            subscriptions=['update_table'],
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
    samples = []

    await update_timer()

    for idx in range(60):
        await asyncio.sleep(1)

        data.append((elapsed, idx))

        if idx < 15:
            table_data.append({'one': idx})
            await update_table(table_data)

        samples.append((1, time.monotonic()))


        await asyncio.gather(*[
            update_rate(samples),
            add(idx),
            update_timings(data)
        ])
        
        elapsed = time.monotonic() - start

    await update_timer()

    await engine.stop()


asyncio.run(display())
