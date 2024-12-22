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
        self._sections: List[Section] = []
        self._section_rows: List[List[Section]] = []
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

        self._max_width = int(terminal_size.columns * 1.25)
        self._max_height = int(math.ceil(terminal_size.lines / 10.0)) * 10

        if width is None:
            width = math.floor(self._max_width / 2)

        if width > self._max_width:
            width = self._max_width

        if height is None:
            height = self._max_height

        self.width = width
        self.height = height

        self._sections = sections

        for section in self._sections:
            section.initialize(
                width,
                height,
            )

        section_rows: list[list[Section]] = []
        section_row: list[Section] = []
        row_width = 0

        for section in self._sections:
            row_width += section.width

            if row_width <= self.width:
                section_row.append(section)

            else:
                section_rows.append(list(section_row))
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

        rows: list[str] = []
        for section_row_set in section_row_sets:
            for row in section_row_set:
                rows.append(row)

        return "\n".join(rows)

    async def stop(self):
        await asyncio.gather(*[section.stop() for section in self._sections])

    async def abort(self):
        await asyncio.gather(*[section.abort() for section in self._sections])
