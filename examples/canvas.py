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
                    height="xx-small",
                    left_border="|",
                    top_border="-",
                    right_border="|",
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
                    ),
                ],
            ),
            Section(
                SectionConfig(
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
                    height="xx-small",
                    left_border="|",
                    top_border="-",
                    right_border="|",
                    bottom_border="-",
                )
            ),
            Section(
                SectionConfig(
                    width="full",
                    height="medium",
                    left_border="|",
                    top_border="-",
                    right_border="|",
                    bottom_border="-",
                ),
                [
                    Component(
                        "table_test",
                        # ScatterPlot(
                        #     PlotConfig(
                        #         plot_name="Test",
                        #         x_axis_name="Time (sec)",
                        #         y_axis_name="Value",
                        #         line_color="aquamarine_2",
                        #         point_char="dot",
                        #         terminal_mode="extended",
                        #     ),
                        # ),
                        Table(
                            TableConfig(
                                headers={
                                    "one": {
                                        "field_type": "integer",
                                    },
                                    "two": {
                                        "field_type": "integer",
                                    },
                                    "three": {
                                        "precision": ".2f",
                                        "field_type": "integer",
                                    },
                                    "four": {"field_type": "string"},
                                },
                                header_color_map={
                                    "one": "aquamarine_2",
                                },
                                data_color_map={
                                    "two": lambda value: "hot_pink_3"
                                    if "None" == value
                                    else None
                                },
                                table_color="aquamarine_2",
                                terminal_mode="extended",
                                table_format="fancy_outline",
                            )
                        ),
                        Alignment(
                            horizontal="center",
                            priority="exclusive",
                        ),
                        horizontal_padding=2,
                    )
                ],
            ),
        ],
    )

    await engine.render()
    data: list[tuple[int, int]] = []

    elapsed = 0
    start = time.monotonic()

    data = []

    async for idx in bar:
        await asyncio.sleep(1)

        if idx <= 15:
            data.append({"one": idx})

            await engine.update(
                "table_test",
                data,
            )

        elapsed = time.monotonic() - start

    await engine.stop()


asyncio.run(display())
