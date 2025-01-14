import asyncio
import math
import uuid
from typing import Any, Dict, List

from hyperscale.ui.config.mode import TerminalMode
from hyperscale.ui.styling import stylize

from hyperscale.ui.components.counter import Counter
from hyperscale.ui.components.empty import Empty
from hyperscale.ui.components.header import Header
from hyperscale.ui.components.link import Link
from hyperscale.ui.components.multiline_text import MultilineText
from hyperscale.ui.components.progress_bar import ProgressBar
from hyperscale.ui.components.scatter_plot import ScatterPlot
from hyperscale.ui.components.spinner import Spinner
from hyperscale.ui.components.text import Text
from hyperscale.ui.components.total_rate import TotalRate
from hyperscale.ui.components.windowed_rate import WindowedRate
from hyperscale.ui.config.widget_fit_dimensions import WidgetFitDimensions
from typing import Generic, TypeVar

from .section_config import SectionConfig, HorizontalSectionSize, VerticalSectionSize


Component = (
    Counter
    | Empty
    | Header
    | Link
    | MultilineText
    | ProgressBar
    | ScatterPlot
    | Spinner
    | Text
    | TotalRate
    | WindowedRate
)


T = TypeVar("T", bound=list[Component])


class Section(Generic[T]):
    def __init__(
        self,
        config: SectionConfig,
        components: T | None = None,
    ) -> None:
        self._empty = False

        if components is None or (isinstance(components, list) and len(components) < 1):
            components = [Empty(str(uuid.uuid4()))]

            self._empty = True

        self.config = config

        self.component_names = [component.name for component in components]
        self.components: Dict[str, Component] = {
            component.name: component for component in components
        }

        self._active_component = self.component_names[0]
        self.component = self.components[self._active_component]

        self._blocks: List[str] = []

        self._mode = TerminalMode.to_mode(self.config.mode)

        self._actual_width = 0
        self._actual_height = 0
        self._inner_width = 0
        self._inner_height = 0

        self._left_remainder_pad = 0
        self._right_remainder_pad = 0

        self._scale: Dict[HorizontalSectionSize | VerticalSectionSize, float] = {
            "auto": 1,
            "smallest": 0.1,
            "xx-small": 0.15,
            "x-small": 0.25,
            "small": 1 / 3,
            "medium": 0.5,
            "large": 2 / 3,
            "x-large": 0.75,
            "xx-large": 0.85,
            "full": 1,
        }

        self._bottom_padding: str | None = None
        self._bottom_border: str | None = None
        self._last_render: List[str] | None = None
        self._left_pad: str = ""
        self._right_pad: str = ""
        self._left_border: str | None = None
        self._right_border: str | None = None

    @property
    def has_component(self):
        return self._empty is False

    @property
    def width(self):
        return self._actual_width

    @property
    def height(self):
        return self._actual_height

    def set_active(self, component_name: str):
        if component := self.components.get(component_name):
            self._active_component = component_name
            self.component = component

    async def resize(
        self,
        canvas_width: int,
        canvans_height: int,
    ):
        if self._last_render:
            self._last_render = None

        width_scale = self._scale[self.config.width]
        self._actual_width = math.floor(width_scale * canvas_width)

        height_scale = self._scale[self.config.height]
        self._actual_height = math.floor(height_scale * canvans_height)

        if self.config.max_width and self._actual_width > self.config.max_width:
            self._actual_width = self.config.max_width

        if self.config.max_height and self._actual_height > self.config.max_height:
            self._actual_height = self.config.max_height

        horizontal_padding = self.config.left_padding + self.config.right_padding
        vertical_padding = self.config.top_padding + self.config.bottom_padding

        if self.config.left_border:
            border_size = len(self.config.left_border)
            horizontal_padding += border_size

        if self.config.right_border:
            border_size = len(self.config.right_border)
            horizontal_padding += border_size

        self._inner_width = self._actual_width - horizontal_padding

        if self.config.top_border:
            border_size = len(self.config.top_border.split("\n"))
            vertical_padding += border_size

        if self.config.bottom_border:
            border_size = len(self.config.bottom_border.split("\n"))
            vertical_padding += border_size

        self._inner_height = self._actual_height - vertical_padding

    async def create_blocks(self):
        if len(self._blocks) > 0:
            self._blocks.clear()

        top_border = await self._create_border_row(self.config.top_border)
        if top_border:
            self._blocks.append(top_border)

        top_padding = await self._create_padding_row(self.config.top_padding)

        if top_padding:
            self._blocks.extend(top_padding)

        if self.config.bottom_padding:
            self._bottom_padding = await self._create_padding_row(
                self.config.bottom_padding
            )

        if self.config.bottom_border:
            self._bottom_border = await self._create_border_row(
                self.config.bottom_border
            )

        if self.config.left_border:
            self._left_border = await stylize(
                self.config.left_border,
                color=self.config.border_color,
                mode=TerminalMode.to_mode(self.config.mode),
            )

        if self.config.right_border:
            self._right_border = await stylize(
                self.config.right_border,
                color=self.config.border_color,
                mode=TerminalMode.to_mode(self.config.mode),
            )

        if self._empty is False:
            await self._fit_components()

        self._left_pad = " " * (self.config.left_padding + self._left_remainder_pad)
        self._right_pad = " " * (self.config.right_padding + self._right_remainder_pad)

    async def render(self):
        if self._empty is False:
            return await self._render_with_component()

        elif self._last_render is None:
            render = await self._render_without_component()
            self._last_render = render

            return render

        else:
            return self._last_render

    async def _fit_components(self, component_name: str | None = None):
        if component_name and (component := self.components.get(component_name)):
            await self._fit_component(component)

        else:
            await asyncio.gather(
                *[
                    self._fit_component(component)
                    for component in self.components.values()
                ]
            )

    async def _fit_component(
        self,
        component: Component,
    ):
        match component.fit_type:
            case WidgetFitDimensions.X_AXIS:
                await component.fit(self._inner_width)

            case WidgetFitDimensions.Y_AXIS:
                await component.fit(self._inner_height)

            case WidgetFitDimensions.X_Y_AXIS:
                await component.fit(
                    max_width=self._inner_width,
                    max_height=self._inner_height,
                )

            case _:
                await component.fit(self._inner_width)

        remainder = self._inner_width - component.raw_size

        (
            left_remainder_pad,
            right_remainder_pad,
        ) = self._set_horizontal_pad_remainder(remainder)

        self._left_remainder_pad = left_remainder_pad
        self._right_remainder_pad = right_remainder_pad

    async def _create_border_row(
        self,
        border_char: str,
    ):
        if border_char is None:
            return None

        return await stylize(
            border_char * self._actual_width,
            color=self.config.border_color,
            mode=self._mode,
        )

    async def _create_padding_row(self, padding_rows: int):
        if padding_rows < 1:
            return None

        padding_row: str = " "

        if self.config.left_border:
            padding_row += await stylize(
                self.config.left_border,
                color=self.config.border_color,
                mode=self._mode,
            )

        if self.config.left_padding:
            padding_row += " " * self.config.left_padding

        padding_row += " " * self._inner_width

        if self.config.right_padding:
            padding_row += " " * self.config.right_padding

        if self.config.right_border:
            padding_row += self.config.right_border

        return [padding_row for _ in range(padding_rows)]

    def _set_horizontal_pad_remainder(
        self,
        remainder: int,
    ):
        left_remainder_pad = 0
        right_remainder_pad = 0

        match self.config.horizontal_alignment:
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

    async def _render_without_component(self):
        blocks = list(self._blocks)

        if self._inner_height > 0:
            blocks.extend(
                await asyncio.gather(
                    *[self._create_fill_line() for _ in range(self._inner_height)]
                )
            )

        else:
            blocks.append(await self._create_fill_line())

        if self._bottom_padding:
            blocks.extend(self._bottom_padding)

        if self._bottom_border:
            blocks.append(self._bottom_border)

        return blocks

    async def _render_with_component(self):
        (rendered_lines, rerender) = await self.component.get_next_frame()

        if rerender is False and self._last_render:
            return self._last_render

        rendered_lines = self._pad_frames_vertical(rendered_lines)

        for idx, line in enumerate(rendered_lines):
            assembled_line = self._left_pad + line + self._right_pad

            if self.config.left_border:
                assembled_line = self._left_border + assembled_line

            if self.config.right_border:
                assembled_line = assembled_line + self._right_border

            rendered_lines[idx] = assembled_line

        blocks = list(self._blocks)
        blocks.extend(rendered_lines)

        if self._bottom_padding:
            blocks.extend(self._bottom_padding)

        if self._bottom_border:
            blocks.append(self._bottom_border)

        self._last_render = blocks

        return blocks

    def _pad_frames_vertical(self, frames: list[str]):
        frames_height = len(frames)

        remainder = self._inner_height - frames_height

        top_remainder = int(math.ceil(remainder / 2))
        bottom_remainder = int(math.floor(remainder / 2))

        adjusted_frames: list[str] = []

        match self.config.vertical_alignment:
            case "top":
                adjusted_frames.extend(frames)
                adjusted_frames.extend(
                    [" " * self.component.raw_size for _ in range(remainder)]
                )

            case "center":
                adjusted_frames.extend(
                    [" " * self.component.raw_size for _ in range(top_remainder)]
                )
                adjusted_frames.extend(frames)
                adjusted_frames.extend(
                    [" " * self.component.raw_size for _ in range(bottom_remainder)]
                )

            case "bottom":
                adjusted_frames.extend(
                    [" " * self.component.raw_size for _ in range(remainder)]
                )

                adjusted_frames.extend(frames)

        return adjusted_frames

    def fit_width(self, remainder: int):
        self._inner_width += remainder
        self._actual_width += remainder

    async def fit_height(self, height: int):
        if self._actual_height >= height:
            return

        delta = height - self._actual_height

        self._blocks.extend(
            await asyncio.gather(*[self._create_fill_line() for _ in range(delta)])
        )

        self._actual_height = height

    async def _create_fill_line(self):
        fill_line = " " * self._inner_width

        if self.config.left_padding:
            fill_line = (" " * self.config.left_padding) + fill_line

        if self.config.left_border:
            fill_line = (
                await stylize(
                    self.config.left_border,
                    color=self.config.border_color,
                    mode=TerminalMode.to_mode(self.config.mode),
                )
                + fill_line
            )

        if self.config.right_padding:
            fill_line += " " * self.config.right_padding

        if self.config.right_border:
            fill_line += await stylize(
                self.config.right_border,
                color=self.config.border_color,
                mode=TerminalMode.to_mode(self.config.mode),
            )

        return fill_line

    async def pause(self):
        if self._empty is False:
            await self.component.pause()

    async def resume(self):
        if self._empty is False:
            await self.component.resume()

    async def stop(self):
        if self._empty is False:
            await self.component.stop()

    async def abort(self):
        if self._empty is False:
            await self.component.abort()
