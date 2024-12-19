import asyncio
import math
from collections import defaultdict
from typing import Dict, List

from hyperscale.terminal.config.mode import TerminalMode
from hyperscale.terminal.config.widget_fit_dimensions import WidgetFitDimensions
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

        self._alignment_priotity_map: Dict[AlignmentPriority, int] = {
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

        self.component_alignments: Dict[
            VerticalAlignment,
            List[Component],
        ] = defaultdict(list)

        for component in self.components:
            self.component_alignments[component.alignment.vertical].append(component)

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

        self._horizontal_padding = horizontal_padding
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
        aligned_components: List[Component] = []

        if self._inner_height <= 1:
            aligned_components = await self._align_row(
                [
                    component
                    for vertical_alignment in self.component_alignments
                    for component in self.component_alignments[vertical_alignment]
                ]
            )

        else:
            for vertical_alignment in self.component_alignments:
                aligned_components.extend(
                    await self._align_row(
                        self.component_alignments[vertical_alignment],
                        vertical_alignment=vertical_alignment,
                    )
                )

        self.components = aligned_components

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

    async def _align_row(
        self,
        components: List[Component],
        vertical_alignment: VerticalAlignment | None = None,
    ):
        row_components_count = len(components)
        max_components_per_row = len(self._horizontal_alignments)

        assert (
            row_components_count <= max_components_per_row
        ), f"Err. - too many components for vertical alignment position. Encountered - {row_components_count} - but maximum supported is - {max_components_per_row}"

        (
            aligned_components,
            unaligned_components,
        ) = await self._align_components(components)

        remaining = (
            sum([component.raw_size for component in unaligned_components])
            + sum([component.raw_size for component in aligned_components])
        ) - self._inner_width

        assert (
            len(unaligned_components) == 0
        ), f"Err. - Exceeded max width of section by {remaining} - please increase the canvas width, reduce the alignment priority, or reduce the number of components for the vertical position - {vertical_alignment}"

        return aligned_components

    async def _align_components(
        self,
        unprioritized_components: List[Component],
    ):
        aligned_components: List[Component] = []
        unaligned_components: List[Component] = []

        prioritized_components = sorted(
            unprioritized_components,
            key=lambda component: self._alignment_priotity_map.get(
                component.alignment.priority, -1
            ),
            reverse=True,
        )

        horizontal_positions_count = len(self._horizontal_alignments)
        vertical_positions_count = len(self._vertical_alignments)

        max_components = horizontal_positions_count * vertical_positions_count

        if len(prioritized_components) > max_components:
            prioritized_components = prioritized_components[:max_components]

        remaining_width = self._inner_width

        remaining_rows = self._inner_height

        remaining_components = len(prioritized_components)

        for component in prioritized_components:
            (
                consumed_width,
                consumed_rows,
                remaining_width,
                remaining_rows,
                remaining_components,
            ) = self._calculate_component_alignment(
                component,
                remaining_width,
                remaining_rows,
                remaining_components,
            )

            if consumed_width and consumed_rows:
                await component.fit(
                    max_width=consumed_width,
                    max_height=consumed_rows,
                )

                aligned_components.append(component)

            else:
                unaligned_components.append(component)

        return (
            aligned_components,
            unaligned_components,
        )

    def _calculate_component_alignment(
        self,
        component: Component,
        remaining_width: int,
        remaining_rows: int,
        remaining_components: int,
    ):
        if remaining_rows <= 0:
            return (
                None,
                None,
                None,
                None,
                None,
            )

        if component.alignment.priority == "auto":
            consumed_width = math.floor(remaining_width / remaining_components)

        else:
            consumed_width = math.floor(
                remaining_width
                * self._alignment_adjust_map[component.alignment.priority]
            )

        next_remaining_width = remaining_width - consumed_width

        if (
            remaining_rows > 0
            and remaining_width <= 0
            and component.fit_type != WidgetFitDimensions.X_Y_AXIS
        ):
            next_remaining_width = self._inner_width - consumed_width

        consumed_rows = (
            self._inner_height
            if component.fit_type == WidgetFitDimensions.X_Y_AXIS
            else 1
        )

        next_remaining_rows = remaining_rows - consumed_rows

        return (
            consumed_width,
            consumed_rows,
            next_remaining_width,
            next_remaining_rows,
            remaining_components - 1,
        )

    async def render(self):
        try:
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

            y_start_offset = self.config.top_padding
            if self.config.top_border:
                y_start_offset += len(self.config.top_border.split("\n"))

            lines: Dict[int, List[str]] = defaultdict(list)

            left_border = await self._recreate_left_border()
            right_border = await self._recreate_right_border()

            left_pad = self.config.left_padding * " "
            right_pad = self.config.right_padding * " "

            pad_size = (
                self._inner_width - sum([raw_size for _, _, raw_size, _ in components])
            ) / 2

            left_pad_adjust = math.floor(pad_size)
            right_pad_adjust = math.ceil(pad_size)

            horizontal_prioritized_components = sorted(
                components,
                key=lambda component_config: self._horizontal_alignment_priority_map.get(
                    component_config[3]
                ),
            )

            consumed_widths: List[int] = []

            for (
                frame,
                y_pos,
                raw_size,
                horizontal_alignment,
            ) in horizontal_prioritized_components:
                y_start = y_pos + y_start_offset

                if isinstance(frame, str):
                    (lines, consumed_widths) = self._render_frame_string(
                        frame,
                        y_pos,
                        raw_size,
                        y_start_offset,
                        left_pad_adjust,
                        right_pad_adjust,
                        horizontal_alignment,
                        lines,
                        consumed_widths,
                    )

                else:
                    (lines, consumed_widths) = self._render_frames_from_list(
                        frame,
                        y_pos,
                        raw_size,
                        y_start_offset,
                        left_pad_adjust,
                        right_pad_adjust,
                        horizontal_alignment,
                        lines,
                        consumed_widths,
                    )

        except Exception:
            import traceback

            print(traceback.format_exc())
            exit(0)

        consumed_width = (
            sum(consumed_widths) + self.config.left_padding + self.config.right_padding
        )

        if self.config.left_border is None and consumed_width < self._actual_width:
            for y_start, line in lines.items():
                line_length = len(line)
                line.insert(line_length - 1, " ")

                lines[y_start] = line

        if self.config.right_border is None and consumed_width < self._actual_width:
            for y_start, line in lines.items():
                line_length = len(line)

                line.insert(line_length - 1, " ")

                lines[y_start] = line

        for y_start, line in lines.items():
            joined_line = "".join(line)

            self._blocks[y_start] = "".join(
                [
                    left_border,
                    left_pad,
                    joined_line,
                    right_pad,
                    right_border,
                ]
            )

        return self._blocks

    def _render_frames_from_list(
        self,
        frames: list[str],
        y_pos: int,
        raw_size: int,
        y_start_offset: int,
        left_pad_adjust: int,
        right_pad_adjust: int,
        horizontal_alignment: HorizontalAlignment,
        lines: Dict[int, List[str]],
        consumed_widths: List[int],
    ):
        for frame in frames:
            (lines, consumed_widths) = self._render_frame_string(
                frame,
                y_pos,
                raw_size,
                y_start_offset,
                left_pad_adjust,
                right_pad_adjust,
                horizontal_alignment,
                lines,
                consumed_widths,
            )

            y_pos += 1

        return (
            lines,
            consumed_widths,
        )

    def _render_frame_string(
        self,
        frame: str,
        y_pos: int,
        raw_size: int,
        y_start_offset: int,
        left_pad_adjust: int,
        right_pad_adjust: int,
        horizontal_alignment: HorizontalAlignment,
        lines: Dict[int, List[str]],
        consumed_widths: List[int],
    ):
        y_start = y_pos + y_start_offset

        if horizontal_alignment == "left":
            lines[y_start].append(frame)

            consumed_widths.append(raw_size)

        elif horizontal_alignment == "center":
            lines[y_start].append(
                (left_pad_adjust * " ") + frame + (right_pad_adjust * " ")
            )

            consumed_widths.extend(
                [
                    left_pad_adjust,
                    raw_size,
                    right_pad_adjust,
                ]
            )

        else:
            right_align_adjust = self._inner_width - sum(consumed_widths) - raw_size

            right_align_pad = right_align_adjust * " "

            lines[y_start].append(right_align_pad + frame)
            consumed_widths.extend(
                [
                    right_align_adjust,
                    raw_size,
                ]
            )

        return (
            lines,
            consumed_widths,
        )

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

    async def stop(self):
        await asyncio.gather(*[component.stop() for component in self.components])

    async def abort(self):
        await asyncio.gather(*[component.abort() for component in self.components])
