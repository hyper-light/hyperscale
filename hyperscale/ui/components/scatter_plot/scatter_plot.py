import asyncio
import re
from collections import OrderedDict, defaultdict
from typing import Dict, List, Tuple, Union

from hyperscale.ui.config.mode import TerminalMode
from hyperscale.ui.config.widget_fit_dimensions import WidgetFitDimensions
from hyperscale.ui.styling import get_style, stylize
from hyperscale.ui.styling.colors import Color
from hyperscale.ui.styling.tones import TONE_PALETTES

from .models import NiceTicks, SeriesUpdate
from .plot_axes import (
    CANVAS_COLUMN,
    TIME_ARROW,
    TIME_AXIS_LINES,
    label_rows,
    relabel_value_axis,
    time_axis_end,
    time_axis_lines,
    value_axis_ticks,
)
from .plot_config import PlotConfig
from .plot_legend import legend_entry_text, legend_series
from .plotille import Figure
from .point_char import PointChar, PointCharName

CompletionRateSet = Tuple[str, List[Union[int, float]]]
PlotPoints = list[tuple[int | float, int | float]]
# A multi-series plot's update: each series' points by its name.
SeriesPoints = dict[str, PlotPoints]
# Color sequences take no column: a line's width is its length without them.
COLOR_SEQUENCE = re.compile(r"\x1b\[[0-9;:]*m")
# The space between two series' entries in the legend: wider than the
# space inside an entry, so each entry reads as one unit.
LEGEND_SEPARATOR = "   "
# The lines a plot draws besides its canvas rows: the value axis' title
# and top line, the time axis and its labels.
PLOT_FRAME_LINES = 4
# One series to draw: its points, its color and its point character.
PlottedSeries = tuple[PlotPoints, int, PointCharName | None]


def all_points(plotted: list[PlottedSeries]) -> PlotPoints:
    """Every series' points together: the axes cover them all."""
    return [point for points, _, _ in plotted for point in points]


def as_series_update(data: SeriesUpdate | SeriesPoints) -> SeriesUpdate:
    """A multi-series update as a ``SeriesUpdate``: one given as each
    series' points alone has no readings."""
    if isinstance(data, SeriesUpdate):
        return data

    return SeriesUpdate(points=data)


class ScatterPlot:
    def __init__(
        self,
        name: str,
        config: PlotConfig,
        subscriptions: list[str] | None = None,
    ) -> None:
        self.fit_type = WidgetFitDimensions.X_Y_AXIS
        self.name = name

        if subscriptions is None:
            subscriptions = []

        self._config = config
        self.subscriptions = subscriptions

        self._mode = TerminalMode.to_mode(config.terminal_mode)

        self._data: list[
            tuple[
                int | float,
                int | float,
            ]
        ] = []

        self._last_state: List[
            tuple[
                int | float,
                int | float,
            ]
        ] = []

        self._last_rendered_frames: list[str] = []

        self._max_height = 0
        self._max_width = 0
        self._corrected_width: int | None = None
        self._corrected_height: int | None = None
        self._width = 0

        self.actions_and_tasks_table_rows: Dict[str, List[OrderedDict]] = defaultdict(
            list
        )
        self.actions_and_tasks_tables: Dict[str, str] = {}

        self._update_lock: asyncio.Lock | None = None
        self._updates: asyncio.Queue | None = None
        self._line_color = config.line_color
        # A multi-series plot draws its legend on the line above the plot.
        self._legend_lines = int(config.series is not None)
        self._legend_series = legend_series(config) if config.series is not None else []
        self._palette = TONE_PALETTES[self._mode]

    @property
    def raw_size(self):
        return self._max_width

    @property
    def size(self):
        return self._max_width

    async def fit(
        self,
        max_width: int | None = None,
        max_height: int | None = None,
    ):
        if self._update_lock is None:
            self._update_lock = asyncio.Lock()

        if self._updates is None:
            self._updates = asyncio.Queue()

        self._max_width = max_width
        self._max_height = max_height
        # The canvas spans the plot but for the value axis' labels before it
        # and the time axis' arrow after it, and its rows all but the plot's
        # frame lines and a multi-series legend.
        self._corrected_width = max(max_width - CANVAS_COLUMN - len(TIME_ARROW), 1)
        self._corrected_height = max(max_height - PLOT_FRAME_LINES - self._legend_lines, 1)

        self._last_rendered_frames.clear()

        self._updates.put_nowait(self._no_points())

    def _no_points(self) -> SeriesPoints | PlotPoints:
        """An update with no point: no series' points, or none at all."""
        return {} if self._config.series is not None else []

    async def update(
        self,
        data: int
        | float
        | list[
            tuple[
                int | float,
                int | float,
            ]
        ],
    ):
        await self._update_lock.acquire()

        self._updates.put_nowait(data)

        self._update_lock.release()

    async def get_next_frame(self):
        data = await self._check_if_should_rerender()

        # An update with no point redraws the bare axes (as before the
        # first point): never the last points again, as if still current.
        if data is None:
            return self._last_rendered_frames, False

        self._last_rendered_frames = await self._render(data)
        return self._last_rendered_frames, True

    async def _render(self, data: SeriesUpdate | SeriesPoints | PlotPoints) -> list[str]:
        if self._config.series is not None:
            return await self._render_series(as_series_update(data))

        return self._render_single(data)

    def _render_single(self, data: PlotPoints) -> list[str]:
        """The plot of its one series."""
        line_color = Color.by_name(get_style(self._line_color, self._data), mode=self._mode)
        return self._padded(self._draw([(data, line_color, self._config.point_char)]))

    async def _render_series(self, update: SeriesUpdate) -> list[str]:
        """Draw every series of a multi-series plot (an update is each
        series' points by name; a series missing from it has none) over
        shared axes: time across, and up the union of every series'
        values. The series are drawn in their declared order, so where
        points of several series land on one cell the series declared
        later wins: the cell shows its point character in its color. A
        legend line above the plot names each series in its color, after
        its point character and before its current reading, with the
        update's extra reading right-aligned after them."""
        series_points = update.points
        plotted: list[PlottedSeries] = [
            (
                series_points.get(series.name, []),
                Color.by_name(get_style(series.color, series_points.get(series.name, [])), mode=self._mode),
                series.point_char,
            )
            for series in self._config.series
        ]
        return self._padded([await self._legend_line(update), *self._draw(plotted)])

    def _draw(self, plotted: list[PlottedSeries]) -> list[str]:
        """The canvas of every series, its value axis relabelled at nice
        ticks and its time axis drawn below it: the points span the
        canvas' width (the newest in its last column) and its height (the
        value axis ends at the nice tick above the largest value)."""
        time_end, value_ticks = self._axis_bounds(all_points(plotted))
        figure = Figure()
        figure.width = self._corrected_width
        figure.height = self._corrected_height
        figure.x_label = ""
        figure.y_label = self._config.y_axis_name
        figure.origin = self._config.use_origin
        figure.set_x_limits(min_=self._config.x_min, max_=time_end)
        figure.set_y_limits(min_=self._config.y_min, max_=value_ticks.values[-1])
        figure.color_mode = "byte"
        for points, color, point_char in plotted:
            self._scatter_series(figure, points, color, point_char)

        plot_lines = figure.show().split("\n")
        return [
            *relabel_value_axis(
                plot_lines[:-TIME_AXIS_LINES],
                label_rows(value_ticks, self._config.y_min, value_ticks.values[-1], self._corrected_height),
                self._corrected_height,
            ),
            *time_axis_lines(self._config.x_axis_name, self._config.x_min, time_end, self._corrected_width),
        ]

    def _axis_bounds(self, points: PlotPoints) -> tuple[float, NiceTicks]:
        """The time axis' end and the value axis' ticks for ``points``."""
        return (
            time_axis_end([x_value for x_value, _ in points], self._config.x_min, self._config.x_max, self._corrected_width),
            value_axis_ticks(
                [y_value for _, y_value in points],
                self._config.y_min,
                self._config.y_max,
                self._corrected_height,
                self._config.value_tick_rows,
            ),
        )

    def _padded(self, plot_lines: list[str]) -> list[str]:
        """Each line padded to the plot's width (color sequences take no
        column), so a shorter frame leaves nothing of a longer one."""
        return [
            plot_line + " " * max(self._max_width - len(COLOR_SEQUENCE.sub("", plot_line)), 0)
            for plot_line in plot_lines
        ]

    def _scatter_series(self, figure: Figure, points: PlotPoints, color: int, point_char: PointCharName | None) -> None:
        """Draw one series' points onto ``figure``."""
        figure.scatter(
            [x_value for x_value, _ in points],
            [y_value for _, y_value in points],
            lc=color,
            marker=PointChar.by_name(point_char),
        )

    async def _legend_line(self, update: SeriesUpdate) -> str:
        """Each series' point character, name and reading in its color, in
        the legend's order, then the extra reading right-aligned."""
        entries = [legend_entry_text(series, update.readings) for series in self._legend_series]
        styled_entries = [
            await stylize(entry, color=get_style(series.color, update.points.get(series.name, [])), mode=self._mode)
            for entry, series in zip(entries, self._legend_series)
        ]
        entries_width = len(LEGEND_SEPARATOR.join(entries))
        return LEGEND_SEPARATOR.join(styled_entries) + await self._extra_reading(update.extra_reading, entries_width)

    async def _extra_reading(self, extra_reading: str | None, entries_width: int) -> str:
        """The extra reading in the dim label color, right-aligned in the
        columns the entries leave -- none where it would not fit apart
        from them."""
        if extra_reading is None:
            return ""

        if (gap_width := self._max_width - entries_width - len(extra_reading)) < len(LEGEND_SEPARATOR):
            return ""

        return " " * gap_width + await stylize(extra_reading, color=self._palette.label_color, mode=self._mode)

    async def _check_if_should_rerender(self):
        # Each update is the plot's whole data set, so only the newest one
        # queued is drawn: a plot that waited unshown, or was refit (which
        # queues its empty axes), never replays the updates it missed one
        # frame at a time.
        data = None
        while self._updates.empty() is False:
            data = self._updates.get_nowait()

        return data

    async def pause(self):
        pass

    async def resume(self):
        pass

    async def stop(self):
        if self._update_lock is not None and self._update_lock.locked():
            self._update_lock.release()

    async def abort(self):
        if self._update_lock is not None and self._update_lock.locked():
            self._update_lock.release()
