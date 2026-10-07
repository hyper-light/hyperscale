"""
The scatter plot's axes and the content-sized table, as the dashboards and
`run workflow`'s UI draw them:

- the plot spans its whole width: the newest point lands in the canvas'
  last column, the oldest in its first;
- value and time ticks fall at nice steps (1, 2 or 5 x 10^k) and read with
  the fewest decimals that tell them apart -- never "52.8000000";
- a table sized to its content never cuts its identifying column (a
  worker's address): narrower columns lose their headers' tails first,
  then the lowest-priority columns are dropped from the right.
"""

import re

from hyperscale.ui.components.scatter_plot import PlotConfig, PlotSeries, ScatterPlot
from hyperscale.ui.components.scatter_plot.plot_axes import (
    CANVAS_COLUMN,
    TIME_ARROW,
    nice_ticks,
    tick_label,
)
from hyperscale.ui.components.table import Table
from hyperscale.ui.components.terminal.canvas import Canvas
from hyperscale.ui.components.terminal.terminal import canvas_size
from hyperscale.ui.components.meter import MeterReading
from hyperscale.ui.components.status_badge import StatusBadgeReading
from hyperscale.ui.config.mode import TerminalMode
from hyperscale.ui.node_dashboard import ManagerDashboardReader
from hyperscale.ui.node_dashboard.node_dashboard import HORIZONTAL_PADDING, VERTICAL_PADDING, WIDTH_SHARE
from hyperscale.ui.node_dashboard.node_dashboard_rows import NodeDashboardRows
from hyperscale.ui.node_dashboard.node_dashboard_sections import (
    generate_node_dashboard_sections,
    node_dashboard_table_config,
    table_component_name,
)
from hyperscale.ui.node_dashboard.node_dashboard_table_cells import table_cells

ANSI_SEQUENCE = re.compile(r"\x1b\[[0-9;:?]*[A-Za-z]")
POINT = "●"
PLOT_WIDTH = 47
PLOT_HEIGHT = 14
# A label with more decimals than any nice step needs.
LONG_DECIMALS = re.compile(r"\d\.\d{3,}")
WORKER_ADDRESS = "127.0.0.1:15138"
# A manager's worker row as the dashboard draws it: its statuses as badges
# and its cores in use as a meter ten columns wide, in ASCII.
METER_WIDTH = 10
(WORKER_ROW,) = table_cells(
    [
        {
            "worker": WORKER_ADDRESS,
            "state": StatusBadgeReading("healthy", "ok"),
            "cores": MeterReading(used=6, total=8, label="6/8"),
            "load": StatusBadgeReading("healthy", "ok"),
            "p95 ms": 50.0,
        }
    ],
    TerminalMode.COMPATIBILITY,
    METER_WIDTH,
)


def plain(lines: list[str]) -> list[str]:
    return [ANSI_SEQUENCE.sub("", line) for line in lines]


async def drawn(plot: ScatterPlot, points: list[tuple[float, float]] | dict[str, list[tuple[float, float]]]) -> list[str]:
    await plot.fit(max_width=PLOT_WIDTH, max_height=PLOT_HEIGHT)
    await plot.update(points)
    lines, _ = await plot.get_next_frame()
    return plain(lines)


def single_series_plot() -> ScatterPlot:
    return ScatterPlot(
        "single", PlotConfig(plot_name="rate", x_axis_name="Time (sec)", y_axis_name="Executions", point_char="dot")
    )


def point_columns(lines: list[str]) -> list[int]:
    return [match.start() for line in lines for match in re.finditer(POINT, line)]


async def test_the_points_span_the_plots_whole_width() -> None:
    # A window of samples, oldest at 0 and newest at its end: the newest
    # lands in the canvas' last column (the plot's inner right edge), the
    # oldest in its first.
    lines = await drawn(single_series_plot(), [(float(second), 10.0 + second % 7) for second in range(300)])
    canvas_columns = PLOT_WIDTH - CANVAS_COLUMN - len(TIME_ARROW)
    columns = point_columns(lines)
    assert max(columns) == CANVAS_COLUMN + canvas_columns - 1, "\n".join(lines)
    assert min(columns) == CANVAS_COLUMN, "\n".join(lines)
    assert all(len(line) == PLOT_WIDTH for line in lines), "\n".join(lines)


async def test_a_few_samples_spread_across_the_width_too() -> None:
    lines = await drawn(single_series_plot(), [(float(second), second * 10.0) for second in range(1, 6)])
    canvas_columns = PLOT_WIDTH - CANVAS_COLUMN - len(TIME_ARROW)
    assert max(point_columns(lines)) == CANVAS_COLUMN + canvas_columns - 1, "\n".join(lines)


async def test_a_multi_series_plot_spans_its_width_below_its_legend() -> None:
    plot = ScatterPlot(
        "series",
        PlotConfig(
            plot_name="rate",
            x_axis_name="Time (sec)",
            y_axis_name="wf /s",
            series=[PlotSeries(name="failed", point_char="x"), PlotSeries(name="done", point_char="dot")],
        ),
    )
    lines = await drawn(plot, {"done": [(float(second), 4.0) for second in range(60)]})
    canvas_columns = PLOT_WIDTH - CANVAS_COLUMN - len(TIME_ARROW)
    assert max(point_columns(lines)) == CANVAS_COLUMN + canvas_columns - 1, "\n".join(lines)


def test_ticks_fall_at_nice_steps_with_the_fewest_decimals() -> None:
    for low, high, count, rounded, expected in (
        (0.0, 55.0, 6, True, ["0", "10", "20", "30", "40", "50", "60"]),
        (0.0, 0.88, 5, True, ["0.0", "0.2", "0.4", "0.6", "0.8", "1.0"]),
        (0.0, 1.1, 4, True, ["0.0", "0.5", "1.0", "1.5"]),
        (0.0, 300.0, 4, True, ["0", "100", "200", "300"]),
        (0.0, 0.033, 4, True, ["0.00", "0.01", "0.02", "0.03", "0.04"]),
        # A value axis of five rows: no more than one tick a row.
        (0.0, 66.0, 6, False, ["0", "20", "40", "60", "80"]),
    ):
        ticks = nice_ticks(low, high, count, rounded)
        assert [tick_label(value, ticks.decimals) for value in ticks.values] == expected, (low, high, count)


async def test_no_axis_label_carries_needless_decimals() -> None:
    for points in (
        [(float(second), second * 13.2) for second in range(1, 6)],
        [(float(second), (second % 5) * 0.2) for second in range(300)],
        [],
    ):
        lines = await drawn(single_series_plot(), points)
        labels = [line.split("|", 1)[0].strip() for line in lines] + lines[-1].split("|", 1)[1].split()
        assert not [label for label in labels if LONG_DECIMALS.search(label)], "\n".join(lines)


async def rendered_table(width: int) -> list[str]:
    table = Table("workers", node_dashboard_table_config(ManagerDashboardReader.layout, "compatability"))
    await table.fit(width, 6)
    await table.update([WORKER_ROW])
    lines, _ = await table.get_next_frame()
    return lines


async def test_a_worker_address_is_never_cut_and_columns_drop_from_the_right() -> None:
    headers = list(ManagerDashboardReader.layout.table_headers)
    for width in (80, 65, 40, 30, 24, 18):
        lines = await rendered_table(width)
        assert all(len(line) == width for line in lines), (width, lines)
        assert WORKER_ADDRESS in lines[-1], (width, lines)
        shown_headers = [header for header in headers if header in lines[0]]
        # The columns shown are the first ones, in order: any dropped are
        # the last.
        assert lines[-1].split()[0] == WORKER_ADDRESS, (width, lines)
        assert shown_headers == headers[: len(shown_headers)], (width, lines)

    # Wide enough for every value: every column shows.
    assert (await rendered_table(65))[-1].split() == [
        WORKER_ADDRESS,
        "+",
        "healthy",
        "#####-",
        "6/8",
        "+",
        "healthy",
        "50.0",
    ]
    # Too narrow for them all: the rightmost go first.
    assert (await rendered_table(30))[-1].split() == [WORKER_ADDRESS, "+", "healthy"]


async def test_the_dashboard_shows_a_worker_address_whole_at_120_and_100_columns() -> None:
    layout = ManagerDashboardReader.layout
    for columns in (120, 100):
        rows = NodeDashboardRows()
        rows.need(badge_line_count=1, table_row_count=1)
        canvas = Canvas(
            generate_node_dashboard_sections(
                layout, node_dashboard_table_config(layout, "compatability"), "compatability", rows
            )
        )
        canvas_width, canvas_height = canvas_size(columns, 38, HORIZONTAL_PADDING, VERTICAL_PADDING, WIDTH_SHARE)
        await canvas.initialize(width=canvas_width, height=canvas_height)
        await canvas.get_component(table_component_name(layout)).update([WORKER_ROW])
        frame = ANSI_SEQUENCE.sub("", await canvas.render())
        worker_line = next(line for line in frame.split("\n") if "127.0.0.1:" in line)
        assert WORKER_ADDRESS in worker_line, f"{columns} columns:\n{frame}"
