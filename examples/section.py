import asyncio
import time

from hyperscale.ui.components.terminal import (
    Alignment,
    Component,
    Section,
    SectionConfig,
)
from hyperscale.ui.components.scatter_plot import (
    PlotConfig,
    ScatterPlot,
)
from hyperscale.ui.components.table import (
    Table,
    TableConfig,
)


async def run():
    section = Section(
        SectionConfig(
            width="large",
            height="full",
            left_border="|",
            top_border="-",
            inside_border="|",
            right_border="|",
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
    )

    section.initialize(200, 12)
    await section.create_blocks()

    data = []
    table_data = []

    start = time.monotonic()
    elapsed = 0

    for idx in range(60):
        next_frame = await section.render()

        print("\n".join(next_frame))

        await asyncio.sleep(1)

        if idx <= 15:
            table_data.append({"one": idx})

            await section.update(
                "table_test",
                table_data,
            )

        data.append((elapsed, idx))

        await section.update(
            "scatter_test",
            data,
        )

        elapsed = time.monotonic() - start


asyncio.run(run())
