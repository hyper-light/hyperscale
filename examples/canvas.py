import asyncio
import functools
import time

from hyperscale.ui.components.header import Header, HeaderConfig

from hyperscale.ui.components.progress_bar import (
    ProgressBar,
    ProgressBarConfig,
)
from hyperscale.ui.components.terminal import Terminal, Section, SectionConfig, action
from hyperscale.ui.components.counter import Counter, CounterConfig
from hyperscale.ui.components.total_rate import TotalRate, TotalRateConfig
from hyperscale.ui.components.windowed_rate import WindowedRate, WindowedRateConfig
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
from hyperscale.ui.components.text import (
    Text,
    TextConfig,
)


@action()
async def add(count: int):
    next_count = count + 1

    channel = "add_to_total"
    if count % 2 == 1:
        channel = "add_to_total_two"

    if channel == "add_to_total_two" and next_count > 30:
        next_count = 30

    return (
        channel,
        next_count,
    )


@action()
async def update_rate(data: list[tuple[int | float, float]]):
    return data


@action()
async def update_timings(timings: list[tuple[int, int]]):
    return timings


@action()
async def update_table(stats: dict[str, dict[str, int]]):
    return [
        {
            "step": step_name,
            "complete": step.get("completed", 0),
            "success": step.get("success", 0),
            "failed": step.get("failed", 0),
        }
        for step_name, step in stats.items()
    ]


@action()
async def update_timer():
    return


async def display():
    sections = [
        Section(
            SectionConfig(height="xx-small", width="full"),
            components=[
                Header(
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
                                        for idx, line in enumerate(letter.split("\n"))
                                    ]
                                )
                            ],
                            "e": [
                                lambda letter, idx: "\n".join(
                                    [
                                        line[1:] if idx < 2 else line
                                        for idx, line in enumerate(letter.split("\n"))
                                    ]
                                )
                                if idx == 9
                                else letter
                            ],
                        },
                        color="aquamarine_2",
                        attributes=["bold"],
                        terminal_mode="extended",
                    ),
                ),
            ],
        ),
        Section(
            SectionConfig(
                width="small",
                height="smallest",
                left_border="|",
                top_border="-",
                bottom_border="-",
                max_height=3,
                horizontal_alignment="center",
                vertical_alignment="center",
            ),
            components=[
                ProgressBar(
                    "progress_bar_one",
                    ProgressBarConfig(
                        total=60,
                        active_color="royal_blue",
                        failed_color="white",
                        complete_color="hot_pink_3",
                        terminal_mode="extended",
                    ),
                    subscriptions=["add_to_total"],
                ),
                ProgressBar(
                    "progress_bar_two",
                    ProgressBarConfig(
                        total=30,
                        active_color="royal_blue",
                        failed_color="white",
                        complete_color="hot_pink_3",
                        terminal_mode="extended",
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
                max_height=3,
                horizontal_alignment="center",
                vertical_alignment="center",
            ),
            components=[
                Text(
                    "text",
                    TextConfig(
                        text="Initializing...",
                    ),
                )
            ],
        ),
        Section(
            SectionConfig(
                width="small",
                height="xx-small",
                left_border="|",
                top_border="-",
                bottom_border="-",
                horizontal_alignment="center",
                max_height=3,
            ),
            components=[
                Timer(
                    "timer",
                    TimerConfig(
                        color="aquamarine_2",
                        terminal_mode="extended",
                        horizontal_alignment="center",
                    ),
                    subscriptions=["update_timer"],
                ),
            ],
        ),
        Section(
            SectionConfig(
                width="small",
                height="xx-small",
                left_border="|",
                top_border="-",
                bottom_border="-",
                max_height=3,
                left_padding=1,
                right_padding=1,
                horizontal_alignment="center",
            ),
            components=[
                Counter(
                    "counter",
                    CounterConfig(unit="total actions", terminal_mode="extended"),
                    subscriptions=["add_to_total"],
                ),
            ],
        ),
        Section(
            SectionConfig(
                width="small",
                height="xx-small",
                left_border="|",
                right_border="|",
                top_border="-",
                bottom_border="-",
                max_height=3,
                horizontal_alignment="center",
            ),
            components=[
                TotalRate(
                    "total_rate",
                    TotalRateConfig(unit="aps", terminal_mode="extended"),
                    subscriptions=["add_to_total"],
                )
            ],
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
            components=[
                ScatterPlot(
                    "scatter_test",
                    PlotConfig(
                        plot_name="Completions Per. Second",
                        x_axis_name="Time (sec)",
                        y_axis_name="Executions",
                        line_color="aquamarine_2",
                        point_char="dot",
                        terminal_mode="extended",
                    ),
                    subscriptions=["update_timings"],
                )
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
                left_padding=2,
                right_padding=2,
                horizontal_alignment="center",
            ),
            components=[
                Table(
                    "table_test",
                    TableConfig(
                        headers={
                            "step": {"default": "N/A", "fixed": True},
                            "complete": {
                                "default": 0,
                            },
                            "success": {
                                "data_color": lambda value: "aquamarine_2"
                                if value > 0
                                else None,
                                "default": 0,
                            },
                            "failed": {
                                "data_color": lambda value: "hot_pink_3"
                                if value > 0
                                else None,
                                "default": 0,
                            },
                        },
                        minimum_column_width=12,
                        border_color="aquamarine_2",
                        terminal_mode="extended",
                        table_format="simple",
                        no_update_on_push=True,
                    ),
                    subscriptions=["update_table"],
                )
            ],
        ),
    ]

    engine = Terminal(sections)

    engine.add_channel("progress_bar_two", "add_to_total_two")

    await engine.render(
        horizontal_padding=4,
        vertical_padding=1,
    )

    data: list[tuple[int, int]] = []

    elapsed = 0
    start = time.monotonic()

    data = []
    table_data = {"http_get_login": {"completed": 0, "success": 0, "failed": 0}}
    samples = []

    active_progress_bar = "progress_bar_one"

    await update_timer()

    for idx in range(60):
        await asyncio.sleep(1)

        data.append((elapsed, idx))

        if idx < 15:
            await update_table(table_data)

        if idx % 5 == 0 and idx > 0:
            active_progress_bar = (
                "progress_bar_two"
                if active_progress_bar == "progress_bar_one"
                else "progress_bar_one"
            )
            await engine.set_component_active(active_progress_bar)

        samples.append((1, time.monotonic()))

        await asyncio.gather(*[add(idx), update_timings(data)])

        elapsed = time.monotonic() - start

    await update_timer()

    await engine.stop()


asyncio.run(display())
