import asyncio
import math
import shutil
from typing import List

from .section import Section


class Canvas:
    def __init__(self) -> None:
        self.width = 0
        self.height = 0
        self._max_width = 0
        self._max_height = 0
        self._sections: List[List[Section]] = [[]]
        self._total_size: int = 0
        self._loop: asyncio.AbstractEventLoop | None = None

    @property
    def size(self):
        return self._total_size

    async def initialize(
        self,
        sections: List[Section],
        width: int | None = None,
        height: int | None = None,
    ):
        if self._loop is None:
            self._loop = asyncio.get_event_loop()

        terminal_size = await self._loop.run_in_executor(None, shutil.get_terminal_size)

        self._max_width = terminal_size.columns
        self._max_height = int(math.ceil(terminal_size.lines / 10.0)) * 10

        if width is None:
            width = math.floor(self._max_width / 2)

        if width > self._max_width:
            width = self._max_width

        if height is None:
            height = self._max_height

        self.width = width
        self.height = height

        sections = [
            section.initialize(
                self.width,
                self.height,
            )
            for section in sections
        ]

        current_row_width = 0
        row_idx = 0
        for section in sections:
            next_width = current_row_width + section.width
            remainder = self.width - current_row_width

            if next_width > self.width:
                self._sections[row_idx][-1].fit_width(remainder)
                self._sections.append([section])
                current_row_width = section.width
                row_idx += 1

            else:
                current_row_width += section.width
                self._sections[row_idx].append(section)

        remainder = self.width - current_row_width
        self._sections[-1][-1].fit_width(remainder)

        for row in self._sections:
            row_height = max([section.height for section in row])

            await asyncio.gather(*[section.create_blocks() for section in sections])

            for section in row:
                section.fit_height(row_height)

    async def render(self):
        self._total_size = len(self._sections)
        terminal_size = await self._loop.run_in_executor(None, shutil.get_terminal_size)

        width = terminal_size.columns
        height = terminal_size.lines - 1

        canvas_sections = await asyncio.gather(
            *[
                self._join_row(
                    row,
                    width,
                )
                for row in self._sections
            ]
        )

        self._total_size += sum(
            [len(canvas_section) for canvas_section in canvas_sections]
        )

        canvas = [line for canvas_section in canvas_sections for line in canvas_section]

        self._total_size += len(canvas_sections)
        return "\n".join(canvas[:height])

    async def _join_row(
        self,
        row: List[Section],
        width: int,
    ):
        rendered_blocks = await asyncio.gather(*[section.render() for section in row])

        segments_count = len(rendered_blocks[0])

        segments = ["" for _ in range(segments_count)]

        for base_idx, segment in enumerate(rendered_blocks):
            for segment_idx, segement_row in enumerate(segment):
                segments[segment_idx] += segement_row

        for idx, segment_row in enumerate(segments):
            segments[idx] = segment_row[:width]

        return segments

    async def stop(self):
        await asyncio.gather(
            *[section.stop() for row in self._sections for section in row]
        )

    async def abort(self):
        await asyncio.gather(
            *[section.abort() for row in self._sections for section in row]
        )
