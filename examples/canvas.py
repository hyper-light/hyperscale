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
                ),
                [
                    Component(
                        "scatter_plot_test",
                        ScatterPlot(
                            PlotConfig(
                                plot_name="Test",
                                x_max=60,
                                y_max=50,
                                x_axis_name="Time (sec)",
                                y_axis_name="Value",
                                line_color="cyan",
                                point_char="dot",
                            ),
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

    async for idx in bar:
        await asyncio.sleep(0.5)
        data.append((elapsed, idx))
        await engine.update("scatter_plot_test", data)
        elapsed = time.monotonic() - start

    await engine.stop()


asyncio.run(display())
