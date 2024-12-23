import asyncio
import time

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
from hyperscale.terminal.components.scatter_plot import (
    PlotConfig,
    ScatterPlot,
)
from hyperscale.terminal.components.table import (
    Table,
    TableConfig,
)


async def display():
    engine = RenderEngine()

    factory = BarFactory()

    bar = factory.create_bar(
        60,
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
                        ),
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
        ],
    )

    await engine.render()
    data: list[tuple[int, int]] = []

    elapsed = 0
    start = time.monotonic()

    data = []
    table_data = []

    async for idx in bar:
        await asyncio.sleep(1)

        if idx <= 15:
            table_data.append({"one": idx})

            await engine.update(
                "table_test",
                table_data,
            )

        data.append((elapsed, idx))

        await engine.update(
            "scatter_test",
            data,
        )

        elapsed = time.monotonic() - start

    await engine.stop()


asyncio.run(display())
