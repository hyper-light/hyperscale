import asyncio
import math
from typing import Dict, List

from hyperscale.terminal.config.mode import TerminalMode
from hyperscale.terminal.styling import stylize

from .component import Component
from .section_config import SectionConfig, SectionSize


class Section:
    def __init__(
        self,
        config: SectionConfig,
        components: List[Component] | None = None,
    ) -> None:
        if components is None:
            components = []

        self.config = config
        self.components = components
        self._blocks: List[str] = []

        self._mode = TerminalMode.to_mode(self.config.mode)

        self._actual_width = 0
        self._actual_height = 0
        self._inner_width = 0
        self._inner_height = 0

        self._render_event: asyncio.Event = None

        self._scale: Dict[SectionSize, float] = {
            "smallest": 0.05,
            "xx-small": 0.1,
            "x-small": 0.25,
            "small": 0.33,
            "medium": 0.5,
            "large": 0.66,
            "x-large": 0.75,
            "xx-large": 0.85,
            "full": 0.99,
        }

        self._canvas = ""
        self._left_offset = 0
        self._top_offset = 0
        self._border_pad_offset_left = 0
        self._border_pad_offset_right = 0

        self._center_width = 0
        self._center_height = 0

        self.left_offset = 0
        self.top_offset = 0

    @property
    def width(self):
        return self._actual_width

    @property
    def height(self):
        return self._actual_height

    def initialize(
        self,
        canvas_width: int,
        canvans_height: int,
    ):
        width_scale = self._scale[self.config.width]
        height_scale = self._scale[self.config.height]

        self._actual_width = math.floor(width_scale * canvas_width)
        self._actual_height = math.floor(height_scale * canvans_height)

        self._inner_width = self._actual_width
        self._inner_height = self._actual_height

        horizontal_padding = self.config.left_padding + self.config.right_padding
        vertical_padding = self.config.top_padding + self.config.bottom_padding

        if self.config.left_border:
            border_size = len(self.config.left_border)
            horizontal_padding += border_size

        if self.config.right_border:
            border_size = len(self.config.right_border)
            horizontal_padding += border_size

        self._inner_width = self._inner_width - horizontal_padding

        if self.config.top_border:
            border_size = len(self.config.top_border.split("\n"))
            vertical_padding += border_size

        if self.config.bottom_border:
            border_size = len(self.config.bottom_border.split("\n"))
            vertical_padding += border_size

        self._inner_height = self._inner_height - vertical_padding

        self._center_width = math.floor(self._inner_width / 2)
        self._center_height = math.floor(self._inner_height / 2)

        self._render_event = asyncio.Event()

        return self

    async def _to_row(self):
        row = " " * self._inner_width

        if self.config.left_padding:
            row = (" " * self.config.left_padding) + row

        if self.config.right_padding:
            row += " " * self.config.right_padding

        if self.config.left_border:
            left_border = await stylize(
                self.config.left_border,
                color=self.config.border_color,
                mode=self._mode,
            )

            self._border_pad_offset_left = len(left_border)

            row = left_border + row

        if self.config.right_border:
            right_border = await stylize(
                self.config.right_border,
                color=self.config.border_color,
                mode=self._mode,
            )

            self._border_pad_offset_right = len(right_border)

            row += right_border

        return row

    def set_section_position(self, left_offset: int, top_offset: int):
        self.left_offset = left_offset
        self.top_offset = top_offset

    async def create_blocks(self):
        self._blocks = await asyncio.gather(
            *[self._to_row() for _ in range(self._inner_height)]
        )

        if self.config.top_padding:
            for _ in range(self.config.top_padding):
                self._blocks.insert(0, await self._to_row())

        if self.config.bottom_padding:
            for _ in range(self.config.bottom_padding):
                self._blocks.append(await self._to_row())

        if self.config.top_border:
            top_border = await stylize(
                self.config.top_border * self._actual_width,
                color=self.config.border_color,
                mode=self._mode,
            )

            self._blocks.insert(0, top_border)

        if self.config.bottom_border:
            bottom_border = await stylize(
                self.config.bottom_border * self._actual_width,
                color=self.config.border_color,
                mode=self._mode,
            )

            self._blocks.append(bottom_border)

    async def render(self):
        components = await asyncio.gather(
            *[
                component.render(
                    self._inner_width,
                    self._inner_height,
                    self._center_width,
                    self._center_height,
                )
                for component in self.components
            ]
        )

        x_start_offset = self.config.left_padding
        if self.config.left_border:
            x_start_offset += self._border_pad_offset_left

        y_start_offset = self.config.top_padding
        if self.config.top_border:
            y_start_offset += len(self.config.top_border.split("\n"))

        for frame, y_pos, raw_size, horizontal_alignment in components:
            y_start = y_pos + y_start_offset

            print(raw_size, self._inner_width)

            if horizontal_alignment == "left":
                left_border = await self._recreate_left_border()
                left_pad = self.config.left_padding * " "

                right_border = await self._recreate_right_border()
                right_pad = self.config.right_padding * " "

                line = "".join(
                    [
                        left_border,
                        left_pad,
                        frame,
                        right_pad,
                        right_border,
                    ]
                )

            elif horizontal_alignment == "center":
                pad_size = (self._inner_width - raw_size) / 2
                left_pad_adjust = math.floor(pad_size)
                right_pad_adjust = math.ceil(pad_size)

                left_border = await self._recreate_left_border()
                left_pad = (
                    self.config.left_padding
                    + left_pad_adjust
                    + len(self.config.left_border)
                ) * " "

                right_border = await self._recreate_right_border()
                right_pad = (
                    self.config.right_padding
                    + right_pad_adjust
                    + len(self.config.right_border)
                ) * " "

                line = "".join(
                    [
                        left_border,
                        left_pad,
                        frame,
                        right_pad,
                        right_border,
                    ]
                )

            else:
                left_border = await self._recreate_left_border()
                left_pad = self.config.left_padding * " "

                right_border = await self._recreate_right_border()
                right_pad = self.config.right_padding * " "

                line = "".join(
                    [
                        left_border,
                        left_pad,
                        frame,
                        right_pad,
                        right_border,
                    ]
                )

            # line = self._blocks[y_start]

            # left_pad = line[:x_start]
            # right_border = await self._recreate_right_border()
            # right_pad = self.config.right_padding * " " + right_border

            # left_pad, right_pad = self._adjust_line_pad(
            #     horizontal_alignment,
            #     left_pad,
            #     right_pad,
            # )

            self._blocks[y_start] = line

        return self._blocks

    def fit_width(self, remainder: int):
        self._inner_width += remainder
        self._actual_width += remainder
        self._center_width = math.floor(self._inner_width / 2)

    def fit_height(self, height: int):
        if self._actual_height >= height:
            return

        delta = height - self._actual_height

        for _ in range(delta):
            self._blocks.append("".join([" " for _ in range(self._actual_width)]))

        self._actual_height = height

    async def _recreate_left_border(self):
        if self.config.left_border:
            return await stylize(
                self.config.left_border,
                color=self.config.border_color,
                mode=TerminalMode.to_mode(self.config.mode),
            )

        return ""

    async def _recreate_right_border(self):
        if self.config.right_border:
            return await stylize(
                self.config.right_border,
                color=self.config.border_color,
                mode=TerminalMode.to_mode(self.config.mode),
            )

        return ""
