import asyncio
import math
from typing import Any, Dict, List

from hyperscale.terminal.config.mode import TerminalMode
from hyperscale.terminal.styling import stylize

from .alignment import AlignmentPriority, HorizontalAlignment, VerticalAlignment
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
        self._alignment_remainder = 0

        self._render_event: asyncio.Event = None

        self._vertical_alignments: List[VerticalAlignment] = [
            "top",
            "center",
            "bottom",
        ]

        self._horizontal_alignments: List[HorizontalAlignment] = [
            "left",
            "center",
            "right",
        ]

        self._horizontal_alignment_priority_map: Dict[HorizontalAlignment, int] = {
            "left": 0,
            "center": 1,
            "right": 2,
        }

        self._scale: Dict[SectionSize, float] = {
            "smallest": 0.1,
            "xx-small": 0.15,
            "x-small": 0.25,
            "small": 1 / 3,
            "medium": 0.5,
            "large": 2 / 3,
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

        self._insert_offset = 0

        self._alignment_priority_map: Dict[AlignmentPriority, int] = {
            "auto": -1,
            "low": 0,
            "medium": 1,
            "high": 2,
            "exclusive": 3,
        }

        self._alignment_adjust_map: Dict[AlignmentPriority, float] = {
            "auto": 1,
            "low": 0.25,
            "medium": 0.5,
            "high": 0.75,
            "exclusive": 1,
        }

        self._components_map = {component.name: component for component in components}

        self._bottom_padding: str | None = None
        self._bottom_border: str | None = None
        self._last_render: List[str] | None = None
        self._last_component_render: List[str] | None = None
        self._render_offset: Dict[int, tuple[int, int]] = {}

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
        
        if self._last_render:
            self._last_render = None

        if self._last_component_render:
            self._last_component_render = None

        self._render_offset.clear()

        width_scale = self._scale[self.config.width]
        height_scale = self._scale[self.config.height]

        self._actual_width = math.floor(width_scale * canvas_width)
        self._actual_height = math.floor(height_scale * canvans_height)

        horizontal_padding = self.config.left_padding + self.config.right_padding
        vertical_padding = self.config.top_padding + self.config.bottom_padding

        if self.config.left_border:
            border_size = len(self.config.left_border)
            horizontal_padding += border_size

        if self.config.inside_border:
            border_size = len(self.config.inside_border)
            horizontal_padding += (len(self.components) - 1) * border_size

        if self.config.right_border:
            border_size = len(self.config.right_border)
            horizontal_padding += border_size

        self._horizontal_padding = horizontal_padding
        self._inner_width = self._actual_width - horizontal_padding

        if self.config.top_border:
            border_size = len(self.config.top_border.split("\n"))
            vertical_padding += border_size

        if self.config.bottom_border:
            border_size = len(self.config.bottom_border.split("\n"))
            vertical_padding += border_size

        self._inner_height = self._actual_height - vertical_padding

        self._center_width = math.floor(self._inner_width / 2)
        self._center_height = math.floor(self._inner_height / 2)

        self._render_event = asyncio.Event()

        return self

    async def update(self, component_name: str, data: Any):
        await self._components_map[component_name].component.update(data)

    async def create_blocks(self):
        prioritized_components = sorted(
            self.components,
            key=lambda component: self._alignment_priority_map[
                component.alignment.horizontal_priority
            ]
            + self._alignment_priority_map[component.alignment.vertical_priority],
            reverse=True,
        )

        if len(self._blocks) > 0:
            self._blocks.clear()

        top_border = await self._create_border_row(self.config.top_border)
        if top_border:
            self._blocks.append(top_border)

        top_padding = await self._create_padding_row(
            self.config.top_padding,
            prioritized_components,
        )
        if top_padding:
            self._blocks.extend(top_padding)


        if self.config.bottom_padding:
            self._bottom_padding = await self._create_padding_row(
                    self.config.bottom_padding,
                    self.components,
            )

        if self.config.bottom_border:
            self._bottom_border = await self._create_border_row(self.config.bottom_border)

        remaining_width = self._inner_width
        remaining_height = self._inner_height

        remaining_components = len(prioritized_components)

        for idx, component in enumerate(prioritized_components):
            (
                aligned_component,
                remaining_width,
                remaining_height,
                remaining_components,
            ) = await self._align_component(
                component,
                remaining_width,
                remaining_height,
                remaining_components,
            )

            prioritized_components[idx] = aligned_component

        self.components = prioritized_components

    async def _align_component(
        self,
        component: Component,
        remaining_width: int,
        remaining_height: int,
        remaining_components: int,
    ):
        component_width = self._calculate_component_width(
            component,
            remaining_width,
            remaining_components,
        )

        remaining_width -= component_width

        component_height = self._calculate_component_height(
            component,
            remaining_height,
        )

        if remaining_width <= 0:
            remaining_height -= component_height

        remaining_components -= 1

        await component.fit(component_width, component_height)

        return (
            component,
            remaining_width,
            remaining_height,
            remaining_components,
        )

    def _calculate_component_width(
        self,
        component: Component,
        remaining_width: int,
        remaining_components: int,
    ):
        horizontal_priority_adjustment = self._alignment_adjust_map[
            component.alignment.horizontal_priority
        ]

        if component.alignment.horizontal_priority == "exclusive":
            return self._inner_width

        elif component.alignment.horizontal_priority == "auto":
            return int(remaining_width / remaining_components)

        else:
            return int(self._inner_width * horizontal_priority_adjustment)

    def _calculate_component_height(
        self,
        component: Component,
        remaining_height: int,
    ):
        vertical_priority_adjustment = self._alignment_adjust_map[
            component.alignment.vertical_priority
        ]

        if component.alignment.vertical_priority == "exclusive":
            return self._inner_height

        elif component.alignment.vertical_priority == "auto":
            return remaining_height

        else:
            return int(self._inner_height * vertical_priority_adjustment)

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

    async def _create_padding_row(
        self,
        padding_rows: int,
        components: list[Component],
    ):
        if padding_rows < 1:
            return None
        
        padding_row: str = " "

        if self.config.left_border:
            padding_row += await stylize(
                self.config.left_border,
                color=self.config.border_color,
                mode=self._mode,
            )

        for component in components:
            padding_row += " " * component.raw_size

            if self.config.inside_border:
                padding_row += self.config.inside_border

        if self.config.right_border:
            padding_row += self.config.right_border

        return [padding_row for _ in range(padding_rows)]

    async def render(self):

        components_count = len(self.components) 

        if components_count > 0:
            return await self._render_with_components()
        elif self._last_render is None:
            render = await self._render_without_components()
            self._last_render = render

            return render
        
        else:
            return self._last_render

    async def _render_without_components(self):
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

    async def _render_with_components(self):
        components: list[tuple[int, list[str]]] = await asyncio.gather(
            *[
                component.render(render_idx)
                for render_idx, component in enumerate(self.components)
            ]
        )

        if len([rerender for _, _, rerender in components if rerender is False]) == len(self.components) and self._last_render:
            return self._last_render
        
        if self._last_component_render is None:
            self._last_component_render = []

        ordered_lines = sorted(components, key=lambda render: render[0])

        last_component_idx = len(ordered_lines) - 1

        lines: list[str] = []
        line_widths: list[int] = []

        for render_idx, rendered_lines, rerender in ordered_lines:
            component = self.components[render_idx]

            if rerender or self._render_offset.get(render_idx) is None:

                self._last_component_render.clear()

                left_border = ""
                if self.config.left_border:
                    left_border = await stylize(
                        self.config.left_border,
                        color=self.config.border_color,
                        mode=TerminalMode.to_mode(self.config.mode),
                    )

                inside_border = ""
                if self.config.inside_border:
                    inside_border = await stylize(
                        self.config.inside_border,
                        color=self.config.border_color,
                        mode=TerminalMode.to_mode(self.config.mode),
                    )

                right_border = ""
                if self.config.right_border:
                    right_border = await stylize(
                        self.config.right_border,
                        color=self.config.border_color,
                        mode=TerminalMode.to_mode(self.config.mode),
                    )

                for idx, line in enumerate(rendered_lines):
                    assembled_line = await self._assemble_line(line)

                    if render_idx == 0 and self.config.left_border:
                        assembled_line = left_border + assembled_line

                    if render_idx < last_component_idx and self.config.inside_border:
                        assembled_line += inside_border

                    if render_idx == last_component_idx and self.config.right_border:
                        assembled_line = assembled_line + right_border

                    if len(lines) <= idx:
                        self._render_offset[render_idx] = (0, len(assembled_line))
                        lines.append(assembled_line)
                        self._last_component_render.append(assembled_line)
                        line_widths.append(component.raw_size)

                    else:
                        line_length = len(lines[idx])
                        self._render_offset[render_idx] = (line_length, line_length + len(assembled_line))

                        lines[idx] += assembled_line
                        self._last_component_render[idx] += assembled_line
                        line_widths[idx] += component.raw_size

            else:
                start, end = self._render_offset[render_idx]
                for idx, line in enumerate(self._last_component_render):
                    lines[idx] += line[start: end]

        blocks = list(self._blocks)
        blocks.extend(lines)

        if self._bottom_padding:
            blocks.extend(self._bottom_padding)

        if self._bottom_border:
            blocks.append(self._bottom_border)

        self._last_render = blocks

        return blocks
    
    def _rerender_component_lines(self):
        pass

    async def _assemble_line(self, line: str):
        assembled_line: str = ""

        assembled_line += self.config.left_padding * " "
        assembled_line += line
        assembled_line += self.config.right_padding * " "

        return assembled_line

    def fit_width(self, remainder: int):
        self._inner_width += remainder
        self._actual_width += remainder
        self._center_width = math.floor(self._inner_width / 2)

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
        await asyncio.gather(*[component.pause() for component in self.components])

    async def resume(self):
        await asyncio.gather(*[component.resume() for component in self.components])

    async def stop(self):
        await asyncio.gather(*[component.stop() for component in self.components])

    async def abort(self):
        await asyncio.gather(*[component.abort() for component in self.components])
