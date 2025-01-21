import asyncio
import math
from typing import List

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

from .section import Section


class Canvas:
    def __init__(self, sections: List[Section]) -> None:
        self.width = 0
        self.height = 0
        self._max_width = 0
        self._max_height = 0
        self._sections = sections

        self._section_rows: List[List[Section]] = []
        self._total_size: int = 0
        self._loop: asyncio.AbstractEventLoop | None = None
        self._horizontal_padding: int = 0
        self._vertical_padding: int = 0
        self._top_pad: list[str] = []
        self._bottom_pad: list[str] = []

    @property
    def total_width(self):
        return self.width + self._horizontal_padding

    @property
    def total_height(self):
        return self.height + self._vertical_padding

    @property
    def size(self):
        return self._total_size

    def get_section(self, component_name: str) -> Section | None:
        for section in self._sections:
            if component_name in section.component_names:
                return section

    def get_component(
        self, component_name: str
    ) -> (
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
        | None
    ):
        for section in self._sections:
            if component_name in section.component_names:
                return section.components.get(component_name)

    async def initialize(
        self,
        width: int | None = None,
        height: int | None = None,
        horizontal_padding: int = 0,
        vertical_padding: int = 0,
    ):
        self._horizontal_padding = horizontal_padding
        self._vertical_padding = vertical_padding

        if self._loop is None:
            self._loop = asyncio.get_event_loop()

        self.width = width
        self.height = height

        await asyncio.gather(
            *[section.resize(width, height) for section in self._sections]
        )

        section_rows: list[list[Section]] = []
        section_row: list[Section] = []
        row_width = 0

        auto_sections: list[Section] = []

        for section in self._sections:
            remainder = self.width - row_width
            if section.config.width == "auto" and remainder > 0:
                await section.resize(
                    remainder,
                    height,
                )

            row_width += section.width

            if row_width <= self.width:
                section_row.append(section)

            else:
                section_rows.append(list(section_row))
                auto_sections.clear()
                section_row = [section]
                row_width = section.width

        if len(section_row) > 0:
            section_rows.append(section_row)

        for row in section_rows:
            remainder = self.width - sum([section.width for section in row])

            row[-1].fit_width(remainder)

            row_height = max([section.height for section in row])

            await asyncio.gather(*[section.fit_height(row_height) for section in row])

        await asyncio.gather(*[section.create_blocks() for section in self._sections])

        self._section_rows = section_rows

        total_line_width = sum(
            [self._horizontal_padding, self.width, self._horizontal_padding]
        )

        self._top_pad.clear()
        for _ in range(self._vertical_padding):
            padding_line = " " * total_line_width + "\r"
            self._top_pad.append(padding_line)

        self._bottom_pad.clear()
        self._bottom_pad.extend(
            [" " * total_line_width + "\r" for _ in range(self._vertical_padding)]
        )

    async def replace(
        self,
        components: List[
            Counter
            | Header
            | Text
            | Spinner
            | Link
            | ProgressBar
            | ScatterPlot
            | TotalRate
            | WindowedRate
        ],
    ):
        replacements: list[
            tuple[
                Section,
                Counter
                | Header
                | Text
                | Spinner
                | MultilineText
                | Link
                | ProgressBar
                | ScatterPlot
                | TotalRate
                | WindowedRate,
            ]
        ] = []

        for component in components:
            if section := self.get_section(component.name):
                replacements.append(
                    (
                        section,
                        component,
                    )
                )

        if len(replacements) > 0:
            await asyncio.gather(
                *[section.replace(component) for section, component in replacements]
            )

    async def render(self):
        section_row_sets: list[list[str]] = []
        for section_row in self._section_rows:
            row_lines: list[str] = []

            for section in section_row:
                section_lines = await section.render()

                for idx, line in enumerate(section_lines):
                    if idx >= len(row_lines):
                        row_lines.append(line)

                    else:
                        row_lines[idx] += line

            section_row_sets.append(row_lines)

        rows: list[str] = list(self._top_pad)
        rows.extend(
            [
                " " * self._horizontal_padding
                + row
                + " " * self._horizontal_padding
                + "\r"
                for section_row_set in section_row_sets
                for row in section_row_set
            ]
        )

        rows.extend(self._bottom_pad)

        return "\n".join(rows)

    def _apply_horizontal_padding(self, line: str):
        if self._horizontal_padding > 0:
            line = (
                " " * self._horizontal_padding + line + " " * self._horizontal_padding
            )

        return line

    def _apply_vertical_padding(self, lines: list[str]):
        if self._vertical_padding > 0:
            for _ in range(self._vertical_padding):
                padding_line = " " * self.width

                lines.insert(0, padding_line)

            lines.extend([" " * self.width for _ in range(self._vertical_padding)])

        return lines

    async def pause(self):
        await asyncio.gather(*[section.pause() for section in self._sections])

    async def resume(self):
        await asyncio.gather(*[section.resume() for section in self._sections])

    async def stop(self):
        await asyncio.gather(*[section.stop() for section in self._sections])

    async def abort(self):
        await asyncio.gather(*[section.abort() for section in self._sections])
