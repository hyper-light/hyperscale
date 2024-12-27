import math

from hyperscale.ui.components.link import Link
from hyperscale.ui.components.progress_bar import ProgressBar
from hyperscale.ui.components.scatter_plot import ScatterPlot
from hyperscale.ui.components.spinner import Spinner
from hyperscale.ui.components.text import Text
from hyperscale.ui.components.total_rate import TotalRate
from hyperscale.ui.components.windowed_rate import WindowedRate
from hyperscale.ui.config.widget_fit_dimensions import WidgetFitDimensions
from typing import List

from .alignment import Alignment



class Component:
    def __init__(
        self,
        name: str,
        component: Text | Spinner | Link | ProgressBar | ScatterPlot | TotalRate | WindowedRate,
        alignment: Alignment | None = None,
        subscriptions: List[str] | None = None,
        horizontal_padding: int = 0,
        vertical_padding: int = 0,
    ) -> None:
        if alignment is None:
            alignment = Alignment()


        if subscriptions is None:
            subscriptions = []

        self.name = name
        self.component = component
        self.alignment = alignment
        self.subscriptions = subscriptions
        self._last_frame: list[str] | None = None

        self._max_height: int = 0
        self._max_width: int = 0
        alignment: Alignment | None = None,

        self._horizontal_padding = horizontal_padding
        self._vertical_padding = vertical_padding
        self._left_remainder_pad = 0
        self._right_remainder_pad = 0

    @property
    def vertical_alignment(self):
        return self.alignment.vertical

    @property
    def horizontal_alignment(self):
        return self.alignment.horizontal

    @property
    def horizontal_padding(self):
        return self._horizontal_padding

    @property
    def raw_size(self):
        return self.component.raw_size + self._horizontal_padding

    @property
    def size(self):
        return self.component.size + self._horizontal_padding

    @property
    def fit_type(self):
        return self.component.fit_type
    
    @property
    def update_func(self):
        return self.component.update

    async def fit(
        self,
        max_width: int | None = None,
        max_height: int | None = None,
    ):
        self._max_width = max_width
        self._max_height = max_height
        
        if self._last_frame:
            self._last_frame = None

        match self.component.fit_type:
            case WidgetFitDimensions.X_AXIS:
                await self.component.fit(max_width - self._horizontal_padding)

            case WidgetFitDimensions.Y_AXIS:
                await self.component.fit(max_height - self._vertical_padding)

            case WidgetFitDimensions.X_Y_AXIS:
                await self.component.fit(
                    max_width=max_width - self._horizontal_padding,
                    max_height=max_height - self._vertical_padding,
                )

            case _:
                await self.component.fit(max_width - self._horizontal_padding)

        remainder = max_width - self.component.raw_size - self._horizontal_padding

        (
            left_remainder_pad,
            right_remainder_pad,
        ) = self._set_horizontal_pad_remainder(remainder)

        self._left_remainder_pad = left_remainder_pad
        self._right_remainder_pad = right_remainder_pad

    def _set_horizontal_pad_remainder(
        self,
        remainder: int,
    ):
        left_remainder_pad = 0
        right_remainder_pad = 0

        match self.alignment.horizontal:
            case "left":
                right_remainder_pad = remainder

            case "center":
                left_remainder_pad = math.ceil(remainder / 2)
                right_remainder_pad = math.floor(remainder / 2)

            case "right":
                left_remainder_pad = remainder

        return (
            left_remainder_pad,
            right_remainder_pad,
        )

    async def render(self, render_idx: int):

        render: tuple[str | list[str], bool] = await self.component.get_next_frame()

        frames, execute_rerender = render

        if self._last_frame is None or execute_rerender:

            if isinstance(frames, str):
                frames = [frames]

            (
                left_pad,
                right_pad,
            ) = self._calculate_left_and_right_pad()

            frames = self._pad_frames_horizontal(
                frames,
                left_pad,
                right_pad,
            )

            padded_frames = self._pad_frames_vertical(frames)

            self._last_frame = padded_frames

            return (
                render_idx, 
                padded_frames, 
                execute_rerender,
            )
        
        return (
            render_idx, 
            self._last_frame, 
            execute_rerender,
        )

    def _pad_frames_horizontal(
        self,
        frames: list[str],
        left_pad: int,
        right_pad: int,
    ):
        padded_frames: list[str] = []
        for frame in frames:
            padded_frames.append(left_pad * " " + frame + right_pad * " ")

        return padded_frames

    def _pad_frames_vertical(self, frames: list[str]):
        frames_height = len(frames)

        remainder = self._max_height - frames_height

        top_remainder = int(math.ceil(remainder / 2))
        bottom_remainder = int(math.floor(remainder / 2))

        adjusted_frames: list[str] = []

        match self.alignment.vertical:
            case "top":
                adjusted_frames.extend(frames)
                adjusted_frames.extend(
                    [" " * self._max_width for _ in range(remainder)]
                )

            case "center":
                adjusted_frames.extend(
                    [" " * self._max_width for _ in range(top_remainder)]
                )
                adjusted_frames.extend(frames)
                adjusted_frames.extend(
                    [" " * self._max_width for _ in range(bottom_remainder)]
                )

            case "bottom":
                adjusted_frames.extend(
                    [" " * self._max_width for _ in range(remainder)]
                )

                adjusted_frames.extend(frames)

        return adjusted_frames

    def _calculate_left_and_right_pad(self):
        left_pad = 0
        right_pad = 0

        match self.alignment.horizontal:
            case "left":
                right_pad = self._horizontal_padding + self._right_remainder_pad

            case "center":
                left_pad = (
                    math.ceil(self._horizontal_padding / 2) + self._left_remainder_pad
                )
                right_pad = (
                    math.floor(self._horizontal_padding / 2) + self._right_remainder_pad
                )

            case "right":
                left_pad = self._horizontal_padding + self._left_remainder_pad

            case _:
                left_pad = (
                    self._horizontal_padding
                    + self._left_remainder_pad
                    + self._right_remainder_pad
                )

        return (
            left_pad,
            right_pad,
        )

    async def pause(self):
        await self.component.pause()

    async def resume(self):
        await self.component.resume()

    async def stop(self):
        await self.component.stop()

    async def abort(self):
        await self.component.abort()
