import asyncio
import math
from typing import Dict, List

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

        self._actual_width = 0
        self._actual_height = 0
        self._inner_width = 0
        self._inner_height = 0

        self._render_event: asyncio.Event = None

        self._scale: Dict[SectionSize, float] = {
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

        self._blocks = [self._to_row() for _ in range(self._inner_height)]

        if self.config.top_padding:
            for _ in range(self.config.top_padding):
                self._blocks.insert(0, self._to_row())

        if self.config.bottom_padding:
            for _ in range(self.config.bottom_padding):
                self._blocks.append(self._to_row())

        if self.config.top_border:
            self._blocks.insert(0, self.config.top_border * self._actual_width)

        if self.config.bottom_border:
            self._blocks.append(self.config.bottom_border * self._actual_width)

        return self

    def _to_row(self):
        row = " " * self._inner_width

        if self.config.left_padding:
            row = (" " * self.config.left_padding) + row

        if self.config.right_padding:
            row += " " * self.config.right_padding

        if self.config.left_border:
            row = self.config.left_border + row

        if self.config.right_border:
            row += self.config.right_border

        return row

    def set_section_position(self, left_offset: int, top_offset: int):
        self.left_offset = left_offset
        self.top_offset = top_offset

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
            x_start_offset += len(self.config.left_border)

        y_start_offset = self.config.top_padding
        if self.config.top_border:
            y_start_offset += len(self.config.top_border.split("\n"))

        for frame, x_pos, y_pos, frame_size in components:
            x_start = x_pos + x_start_offset
            y_start = y_pos + y_start_offset

            line = self._blocks[y_start]

            self._blocks[y_start] = "".join(
                [
                    line[:x_start],
                    frame,
                    line[x_start + frame_size :],
                ]
            )

        return self._blocks

    def fit_height(self, height: int):
        if self._actual_height >= height:
            return

        delta = height - self._actual_height

        for _ in range(delta):
            self._blocks.append("".join([" " for _ in range(self._actual_width)]))
