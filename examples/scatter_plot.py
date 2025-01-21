import asyncio

from hyperscale.ui.components.scatter_plot import (
    PlotConfig,
    ScatterPlot,
)


async def run():
    plot = ScatterPlot(
        PlotConfig(
            plot_name="Test",
            x_axis_name="Time (sec)",
            y_axis_name="Value",
            x_max=20,
            y_max=100,
            line_color="cyan",
            point_char="dot",
        ),
    )

    await plot.fit(100, 10)

    # await plot.update(
    #     [
    #         (1, 10),
    #         (2, 35),
    #         (3, 25),
    #         (4, 80),
    #         (5, 65),
    #         (6, 60),
    #         (7, 55),
    #         (8, 15),
    #         (9, 10),
    #         (10, 30),
    #     ]
    # )

    print(await plot.get_next_frame())


asyncio.run(run())
