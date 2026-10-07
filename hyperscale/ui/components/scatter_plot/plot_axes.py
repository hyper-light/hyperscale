"""A scatter plot's axes: tick values at nice steps, labelled with the
fewest decimals that keep adjacent ticks apart (Paul Heckbert, "Nice
Numbers for Graph Labels", Graphics Gems, 1990), and drawn around the
plotted canvas so it spans the plot's whole width.

The canvas is drawn by plotille (a value-axis label column of ten
characters and " | " before each row); these functions relabel its value
axis and draw its time axis below it in place of plotille's, whose label
followed the arrow and so took its width from the canvas.
"""

import bisect
import math

from .models import NiceTicks

# Heckbert's nice fractions, and the thresholds that pick one: rounded to
# the nearest, or the smallest nice fraction at or above the value.
NICE_FRACTIONS = (1.0, 2.0, 5.0, 10.0)
ROUNDED_THRESHOLDS = (1.5, 3.0, 7.0)
CEILING_THRESHOLDS = (1.0, 2.0, 5.0)
NICE_SEARCH = {True: (bisect.bisect_right, ROUNDED_THRESHOLDS), False: (bisect.bisect_left, CEILING_THRESHOLDS)}
# plotille's value-axis label column, and the " | " between it and a row.
VALUE_LABEL_WIDTH = 10
VALUE_GUTTER = " | "
# The time axis' lines below the canvas: the axis, and its labels.
TIME_AXIS_LINES = 2
# The columns before a row's canvas, and the time axis' arrow after it.
CANVAS_COLUMN = VALUE_LABEL_WIDTH + len(VALUE_GUTTER)
TIME_ARROW = ">"
# Time-axis labels sit no closer than plotille spaced its own.
TIME_TICK_SPACING = 10
# Braille cells are two dots wide and four tall.
DOTS_PER_COLUMN = 2
DOTS_PER_ROW = 4
# The value axis ends this far above the largest value: plotille draws no
# point on an axis' maximum.
VALUE_HEADROOM = 1.1


def nice_number(value: float, rounded: bool) -> float:
    """A nice number near ``value``: the nearest (``rounded``) or the
    smallest at or above it."""
    exponent = math.floor(math.log10(value))
    search, thresholds = NICE_SEARCH[rounded]
    return NICE_FRACTIONS[search(thresholds, value / 10**exponent)] * 10**exponent


def nice_ticks(low: float, high: float, count: int, rounded: bool) -> NiceTicks:
    """About ``count`` ticks at a nice step covering ``low`` to ``high``
    (``high`` > ``low``): Heckbert's loose labelling, the step the nice
    number nearest (``rounded``) or next above the range over ``count`` -
    1 -- at most ``count`` ticks then (the range itself is not first
    rounded up, which on a short range halves the ticks)."""
    step = nice_number((high - low) / max(count - 1, 1), rounded)
    first = math.floor(low / step) * step
    tick_count = round((math.ceil(high / step) * step - first) / step) + 1
    return NiceTicks(
        values=[first + index * step for index in range(tick_count)],
        step=step,
        decimals=max(-math.floor(math.log10(step)), 0),
    )


def tick_label(value: float, decimals: int) -> str:
    """A tick's label: integers as integers, else the decimals that tell
    its neighbours apart; a value too wide for the label column in three
    significant digits."""
    label = f"{value:.{decimals}f}"
    return label if len(label) <= VALUE_LABEL_WIDTH else f"{value:.3g}"


def value_axis_ticks(values: list[float], low: float, explicit_high: float | None, rows: int) -> NiceTicks:
    """The value axis' ticks, from ``low`` to the first nice tick above the
    largest value (or the configured end): no more than one a row (the
    step the nice number at or above the range over the rows), so no two
    share a row and the axis ends as close above the values as a nice
    step allows."""
    largest = max(values, default=low) * VALUE_HEADROOM
    high = explicit_high if explicit_high is not None else largest
    return nice_ticks(low, high if high > low else low + 1, rows + 1, False)


def time_axis_end(times: list[float], low: float, explicit_high: float | None, columns: int) -> float:
    """The time axis' end: where the newest time lands in the canvas' last
    dot column (plotille draws no point on the axis' end), so the points
    span the whole width; or the configured end."""
    if explicit_high is not None:
        return explicit_high

    dot_columns = columns * DOTS_PER_COLUMN
    span = max(max(times, default=low) - low, 0.0) or 1.0
    return low + span * dot_columns / (dot_columns - 1)


def label_rows(ticks: NiceTicks, low: float, high: float, rows: int) -> dict[int, str]:
    """Each tick's label by the canvas row a point of its value is drawn in
    (0 at the bottom; ``rows``, the axis' end, the line above the top row):
    plotille's mapping, a braille cell four dots tall, rounded to the dot."""
    dot_height = (high - low) / (rows * DOTS_PER_ROW)
    return {
        round((value - low) / dot_height) // DOTS_PER_ROW: tick_label(value, ticks.decimals)
        for value in reversed(ticks.values)
        if low <= value <= high
    }


def relabel_value_axis(plot_lines: list[str], labels: dict[int, str], rows: int) -> list[str]:
    """plotille's lines with each value-axis label replaced: its axis title,
    the top line, then the rows from the top."""
    return [
        plot_lines[0],
        *(
            labels.get(rows - index, "").rjust(VALUE_LABEL_WIDTH) + line[VALUE_LABEL_WIDTH:]
            for index, line in enumerate(plot_lines[1:])
        ),
    ]


def tick_columns(ticks: NiceTicks, low: float, high: float, columns: int) -> dict[int, str]:
    """Each time tick's label by the canvas column its value falls in."""
    dot_width = (high - low) / (columns * DOTS_PER_COLUMN)
    return {
        round((value - low) / dot_width) // DOTS_PER_COLUMN: tick_label(value, ticks.decimals)
        for value in ticks.values
        if low <= value < high
    }


def label_fits(column: int, label: str, free_from: int, columns: int) -> bool:
    """Whether a time label at ``column`` clears the label before it and
    ends inside the canvas."""
    return free_from <= column and column + len(label) <= columns


def fitting_labels(labels: dict[int, str], columns: int) -> dict[int, str]:
    """The time labels that fit, left to right, each clear of the one
    before it and inside the canvas."""
    kept: dict[int, str] = {}
    free_from = 0
    for column, label in sorted(labels.items()):
        if label_fits(column, label, free_from, columns):
            kept[column] = label
            free_from = column + len(label) + 1

    return kept


def label_line(labels: dict[int, str], columns: int) -> str:
    """The time labels, each from its column."""
    line = [" "] * columns
    for column, label in labels.items():
        line[column : column + len(label)] = label

    return "".join(line)


def time_axis_lines(title: str, low: float, high: float, columns: int) -> list[str]:
    """The time axis below the canvas: its line, with a mark over each
    labelled tick, then its title (in the value labels' column, as the
    value axis' title is) and its labels."""
    ticks = nice_ticks(low, high, columns // TIME_TICK_SPACING + 1, True)
    labels = fitting_labels(tick_columns(ticks, low, high, columns), columns)
    marks = "".join("|" if column in labels else "-" for column in range(columns))
    return [
        "-" * (VALUE_LABEL_WIDTH + 1) + "|-" + marks + TIME_ARROW,
        title[:VALUE_LABEL_WIDTH].rjust(VALUE_LABEL_WIDTH) + VALUE_GUTTER + label_line(labels, columns),
    ]
