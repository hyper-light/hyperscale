import math

from hyperscale.terminal.components.link import Link
from hyperscale.terminal.components.progress_bar import Bar
from hyperscale.terminal.components.scatter_plot import ScatterPlot
from hyperscale.terminal.components.spinner import Spinner
from hyperscale.terminal.components.text import Text
from hyperscale.terminal.components.total_rate import TotalRate
from hyperscale.terminal.components.windowed_rate import WindowedRate
from hyperscale.terminal.config.widget_fit_dimensions import WidgetFitDimensions

from .alignment import Alignment


class Component:
    def __init__(
        self,
        name: str,
        component: Text | Spinner | Link | Bar | ScatterPlot | TotalRate | WindowedRate,
        alignment: Alignment | None = None,
        horizontal_padding: int = 0,
        vertical_padding: int = 0,
    ) -> None:
        if alignment is None:
            alignment = Alignment()

        self.name = name
        self.component = component
        self.alignment = alignment
        self._horizontal_position: int | None = None
        self._vertical_position: int | None = None
        self._frame_width: int = component.size

        self._frame_height: int = 0
        self._horizontal_padding = horizontal_padding
        self._vertical_padding = vertical_padding
        self._left_remainder_pad = 0
        self._right_remainder_pad = 0

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

    async def fit(
        self,
        max_width: int | None = None,
        max_height: int | None = None,
    ):
        match self.component.fit_type:
            case WidgetFitDimensions.X_AXIS:
                await self.component.fit(max_width - self._horizontal_padding)

            case WidgetFitDimensions.Y_AXIS:
                await self.component.fit(max_height - self._vertical_padding)

            case WidgetFitDimensions.X_Y_AXIS:
                await self.component.fit(
                    max_width - self._horizontal_padding,
                    max_height - self._vertical_padding,
                )

            case _:
                await self.component.fit(max_width - self._horizontal_padding)

        remainder = max_width - self.component.raw_size - self._horizontal_padding

        if remainder > 0:
            self._left_remainder_pad = math.ceil(remainder / 2)
            self._right_remainder_pad = math.floor(remainder / 2)

    def _set_position(
        self,
        section_width: int,
        section_height: int,
        section_horizontal_center: int,
        section_vertical_center: int,
    ):
        match self.alignment.horizontal:
            case "left":
                self._horizontal_position = (
                    0 + self._horizontal_padding + self._left_remainder_pad
                )

            case "center":
                self._horizontal_position = max(
                    section_horizontal_center - math.ceil(self._frame_width / 2), 0
                )

            case "right":
                self._horizontal_position = max(
                    section_width
                    - self._frame_width
                    - self._horizontal_padding
                    - self._right_remainder_pad,
                    0,
                )

            case _:
                self._horizontal_position = (
                    0 + self._horizontal_padding + self._left_remainder_pad
                )

        match self.alignment.vertical:
            case "top":
                self._vertical_position = 0 + self._vertical_padding

            case "center":
                self._vertical_position = section_vertical_center

            case "bottom":
                self._vertical_position = (
                    section_height - self._frame_height - self._vertical_padding
                )

            case _:
                self._vertical_position = 0 + self._vertical_padding

    async def render(
        self,
        section_width: int,
        section_height: int,
        section_horizontal_center: int,
        section_vertical_center: int,
    ):
        frame: str | list[str] = await self.component.get_next_frame()
        self._frame_width = self.component.raw_size

        if self._horizontal_position is None and self._vertical_position is None:
            self._frame_height = (
                len(frame.split("\n")) if isinstance(frame, str) else len(frame)
            )

            self._set_position(
                section_width,
                section_height,
                section_horizontal_center,
                section_vertical_center,
            )

        left_pad = 0
        right_pad = 0

        match self.alignment.horizontal:
            case "left":
                right_pad = (
                    self._horizontal_padding
                    + self._left_remainder_pad
                    + self._right_remainder_pad
                )

            case "center":
                left_pad = (
                    math.ceil(self._horizontal_padding / 2) + self._left_remainder_pad
                )
                right_pad = (
                    math.floor(self._horizontal_padding / 2) + self._right_remainder_pad
                )

            case "right":
                left_pad = (
                    self._horizontal_padding
                    + self._left_remainder_pad
                    + self._right_remainder_pad
                )

            case _:
                left_pad = (
                    self._horizontal_padding
                    + self._left_remainder_pad
                    + self._right_remainder_pad
                )

        if isinstance(frame, str):
            frame = self._render_frame_string(
                frame,
                left_pad,
                right_pad,
            )

        else:
            frame = self._render_frame_string_list(
                frame,
                left_pad,
                right_pad,
            )

        return (
            frame,
            self._vertical_position,
            self.component.raw_size
            + self._horizontal_padding
            + self._left_remainder_pad
            + self._right_remainder_pad,
            self.alignment.horizontal,
        )

    def _render_frame_string(
        self,
        frame: str,
        left_pad: int,
        right_pad: int,
    ):
        frame_sections: list[str] = []

        if left_pad:
            frame_sections.append(" " * left_pad)

        frame_sections.append(frame)

        if right_pad:
            frame_sections.append(" " * right_pad)

        frame = "".join(frame_sections)

        return frame

    def _render_frame_string_list(
        self,
        frame: list[str],
        left_pad: int,
        right_pad: int,
    ):
        frame_segments: list[str] = []

        for frame_segment in frame:
            frame_segments.append(
                self._render_frame_string(
                    frame_segment,
                    left_pad,
                    right_pad,
                )
            )

        return frame_segments

    async def stop(self):
        await self.component.stop()

    async def abort(self):
        await self.component.abort()
