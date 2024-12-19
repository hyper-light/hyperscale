import asyncio
from collections import OrderedDict, defaultdict
from os import get_terminal_size
from typing import Dict, List, Tuple, Union

import plotille

from hyperscale.terminal.config.mode import TerminalMode
from hyperscale.terminal.config.widget_fit_dimensions import WidgetFitDimensions
from hyperscale.terminal.styling.colors import Color

from .plot_config import PlotConfig
from .point_char import PointChar

CompletionRateSet = Tuple[str, List[Union[int, float]]]


class ScatterPlot:
    def __init__(self, config: PlotConfig) -> None:
        self.config = config
        self.fit_type = WidgetFitDimensions.X_Y_AXIS
        self._mode = TerminalMode.to_mode(config.terminal_mode)

        self._data: list[
            tuple[
                int | float,
                int | float,
            ]
        ] = []

        self._max_height = 0
        self._max_width = 0
        self._corrected_width: int | None = None
        self._corrected_height: int | None = None
        self._width = 0

        self.actions_and_tasks_table_rows: Dict[str, List[OrderedDict]] = defaultdict(
            list
        )
        self.actions_and_tasks_tables: Dict[str, str] = {}

        self._loop: asyncio.AbstractEventLoop | None = None
        self._update_lock: asyncio.Lock | None = None

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
        terminal_width = 0
        terminal_height = 0

        if self._loop is None:
            self._loop = asyncio.get_event_loop()

        if max_width is None or max_height is None:
            terminal_width, terminal_height = await self._loop.run_in_executor(
                None, get_terminal_size
            )

        if max_width is None:
            max_width = terminal_width

        self._width = max_width

        if max_height is None:
            max_height = terminal_height

        self._max_width = max_width
        self._max_height = max_height

        (
            x_max,
            y_max,
            x_vals,
            y_vals,
        ) = self._generate_x_and_y_vals()

        plot: str = plotille.scatter(
            x_vals,
            y_vals,
            width=self._max_width,
            height=self._max_height,
            y_min=self.config.y_min,
            y_max=int(round(y_max, 0)),
            x_min=self.config.x_min,
            x_max=int(round(x_max, 0)),
            linesep="\n",
            X_label=self.config.x_axis_name,
            Y_label=self.config.y_axis_name,
            lc=Color.by_name(
                self.config.line_color,
                mode=self._mode,
            ),
            color_mode="byte",
            origin=self.config.use_origin,
            marker=PointChar.by_name(self.config.point_char),
        )

        plot_lines = plot.split("\n")

        max_line_length = max([len(line) for line in plot_lines])
        width_difference = max_line_length - self._max_width
        self._corrected_width = self._max_width - width_difference

        plot_height = len(plot_lines)
        height_difference = plot_height - self._max_height
        self._corrected_height = self._max_height - height_difference

    async def update(
        self,
        data: list[
            tuple[
                int | float,
                int | float,
            ]
        ],
    ):
        if self._update_lock is None:
            self._update_lock = asyncio.Lock()

        await self._update_lock.acquire()

        self._data = data

        self._update_lock.release()

    async def get_next_frame(self):
        try:
            (
                x_max,
                y_max,
                x_vals,
                y_vals,
            ) = self._generate_x_and_y_vals()

            length_adjustments = self._get_plot_length_adjustments(
                x_max,
                y_max,
                x_vals,
                y_vals,
            )

            plot: str = plotille.scatter(
                x_vals,
                y_vals,
                width=self._corrected_width,
                height=self._corrected_height,
                y_min=self.config.y_min,
                y_max=int(round(y_max, 0)),
                x_min=self.config.x_min,
                x_max=int(round(x_max, 0)),
                linesep="\n",
                X_label=self.config.x_axis_name,
                Y_label=self.config.y_axis_name,
                lc=Color.by_name(
                    self.config.line_color,
                    mode=self._mode,
                ),
                color_mode="byte",
                origin=self.config.use_origin,
                marker=PointChar.by_name(self.config.point_char),
            )

            plot_lines = plot.split("\n")

            for idx, plot_line in enumerate(plot_lines):
                plot_lines[idx] = plot_line + (" " * length_adjustments[idx])

            return plot_lines

        except Exception:
            import traceback

            print(traceback.format_exc())
            exit(0)

    def _get_plot_length_adjustments(
        self,
        x_max: int,
        y_max: int,
        x_vals: list[int],
        y_vals: list[int],
    ):
        plot: str = plotille.scatter(
            x_vals,
            y_vals,
            width=self._corrected_width,
            height=self._corrected_height,
            y_min=self.config.y_min,
            y_max=int(round(y_max, 0)),
            x_min=self.config.x_min,
            x_max=int(round(x_max, 0)),
            linesep="\n",
            X_label=self.config.x_axis_name,
            Y_label=self.config.y_axis_name,
            origin=self.config.use_origin,
            marker=PointChar.by_name(self.config.point_char),
        )

        plot_lines = plot.split("\n")
        length_adjustments: list[int] = []

        for idx, plot_line in enumerate(plot_lines):
            if len(plot_line) <= self._max_width:
                difference = self._max_width - len(plot_line)
                length_adjustments.append(difference)

            else:
                length_adjustments.append(0)

        return length_adjustments

    def _generate_x_and_y_vals(self):
        x_range = self.config.x_range
        if x_range and self.config.x_range_inclusive:
            x_vals = [idx for idx in range(self.config.x_range_start, x_range + 1)]

        elif x_range:
            x_vals = [idx for idx in range(self.config.x_range_start, x_range + 1)]

        else:
            x_vals = [x_val for x_val, _ in self._data]

        y_vals = [y_val for _, y_val in self._data]

        x_max = self.config.x_max
        if x_max is None and len(x_vals) < 1:
            x_max = self._max_width * 0.8

        elif x_max is None:
            x_max = max(x_vals) * 1.1

        y_max = self.config.y_max
        if y_max is None and len(y_vals) < 1:
            y_max = self._max_height

        elif y_max is None:
            y_max = max(y_vals) * 1.1

        if x_max <= self.config.x_min:
            x_max = self.config.x_min + 1

        if y_max <= self.config.y_min:
            y_max = self.config.y_min + 1

        return (
            x_max,
            y_max,
            x_vals,
            y_vals,
        )

    async def stop(self):
        pass

    async def abort(self):
        pass
