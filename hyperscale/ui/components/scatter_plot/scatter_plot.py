import asyncio
import sys
from collections import OrderedDict, defaultdict
from typing import Dict, List, Tuple, Union

from hyperscale.ui.config.mode import TerminalMode
from hyperscale.ui.config.widget_fit_dimensions import WidgetFitDimensions
from hyperscale.ui.styling import get_style
from hyperscale.ui.styling.colors import Color

from .plot_config import PlotConfig
from .plotille import scatter
from .point_char import PointChar

CompletionRateSet = Tuple[str, List[Union[int, float]]]


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
        self._is_atty = True

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

        (
            x_max,
            y_max,
            x_vals,
            y_vals,
        ) = self._generate_x_and_y_vals([])

        plot: str = scatter(
            x_vals,
            y_vals,
            width=max(self._max_width, 1),
            height=max(self._max_height, 1),
            y_min=self._config.y_min,
            y_max=int(round(y_max, 0)),
            x_min=self._config.x_min,
            x_max=int(round(x_max, 0)),
            linesep="\n",
            X_label=self._config.x_axis_name,
            Y_label=self._config.y_axis_name,
            color_mode="byte",
            origin=self._config.use_origin,
            marker=PointChar.by_name(self._config.point_char),
        )

        plot_lines = plot.split("\n")

        max_line_length = max([len(line) for line in plot_lines])
        width_difference = max_line_length - self._max_width
        self._corrected_width = self._max_width - width_difference

        plot_height = len(plot_lines)
        height_difference = plot_height - self._max_height
        self._corrected_height = self._max_height - height_difference

        if self._corrected_height <= 0:
            self._corrected_height = 1

        if self._corrected_width <= 0:
            self._corrected_width = 1

        self._last_rendered_frames.clear()

        loop = asyncio.get_event_loop()
        self._is_atty = loop.run_in_executor(None, sys.stdout.isatty)

        if self._is_atty is False:
            self._line_color = None

        self._updates.put_nowait([])

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

        if data:
            (
                x_max,
                y_max,
                x_vals,
                y_vals,
            ) = self._generate_x_and_y_vals(data)

            length_adjustments = self._get_plot_length_adjustments(
                x_max,
                y_max,
                x_vals,
                y_vals,
            )

            plot: str = scatter(
                x_vals,
                y_vals,
                width=self._corrected_width,
                height=self._corrected_height,
                y_min=self._config.y_min,
                y_max=int(round(y_max, 0)),
                x_min=self._config.x_min,
                x_max=int(round(x_max, 0)),
                linesep="\n",
                X_label=self._config.x_axis_name,
                Y_label=self._config.y_axis_name,
                lc=Color.by_name(
                    get_style(
                        self._line_color,
                        self._data,
                    ),
                    mode=self._mode,
                ),
                color_mode="byte",
                origin=self._config.use_origin,
                marker=PointChar.by_name(self._config.point_char),
            )

            plot_lines = plot.split("\n")

            for idx, plot_line in enumerate(plot_lines):
                plot_lines[idx] = plot_line + (" " * length_adjustments[idx])

            self._last_rendered_frames = plot_lines
            self._last_state = self._data

            return plot_lines, True

        return self._last_rendered_frames, False

    async def _check_if_should_rerender(self):
        if self._updates.empty() is False:
            return await self._updates.get()

    def _get_plot_length_adjustments(
        self,
        x_max: int,
        y_max: int,
        x_vals: list[int],
        y_vals: list[int],
    ):
        plot: str = scatter(
            x_vals,
            y_vals,
            width=self._corrected_width,
            height=self._corrected_height,
            y_min=self._config.y_min,
            y_max=int(round(y_max, 0)),
            x_min=self._config.x_min,
            x_max=int(round(x_max, 0)),
            linesep="\n",
            X_label=self._config.x_axis_name,
            Y_label=self._config.y_axis_name,
            origin=self._config.use_origin,
            marker=PointChar.by_name(self._config.point_char),
        )

        plot_lines = plot.split("\n")
        length_adjustments: list[int] = []

        for plot_line in plot_lines:
            if len(plot_line) <= self._max_width:
                difference = self._max_width - len(plot_line)
                length_adjustments.append(difference)

            else:
                length_adjustments.append(0)

        return length_adjustments

    def _generate_x_and_y_vals(
        self,
        data: list[
            tuple[
                int | float,
                int | float,
            ]
        ],
    ):
        x_range = self._config.x_range
        if x_range and self._config.x_range_inclusive:
            x_vals = [idx for idx in range(self._config.x_range_start, x_range + 1)]

        elif x_range:
            x_vals = [idx for idx in range(self._config.x_range_start, x_range + 1)]

        else:
            x_vals = [x_val for x_val, _ in data]

        y_vals = [y_val for _, y_val in data]

        x_max = self._config.x_max
        if x_max is None and len(x_vals) < 1:
            x_max = self._max_width * 0.8

        elif x_max is None:
            x_max = max(x_vals) * 1.1

        y_max = self._config.y_max
        if y_max is None and len(y_vals) < 1:
            y_max = self._max_height

        elif y_max is None:
            y_max = max(y_vals) * 1.1

        if x_max <= self._config.x_min:
            x_max = self._config.x_min + 1

        if y_max <= self._config.y_min:
            y_max = self._config.y_min + 1

        return (
            x_max,
            y_max,
            x_vals,
            y_vals,
        )

    async def pause(self):
        pass

    async def resume(self):
        pass

    async def stop(self):
        if self._update_lock.locked():
            self._update_lock.release()

    async def abort(self):
        if self._update_lock.locked():
            self._update_lock.release()
