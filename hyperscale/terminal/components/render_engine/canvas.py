import asyncio
import math
import os
from typing import List

from .section import Section


class Canvas:
    def __init__(self) -> None:
        self.width = 0
        self.height = 0
        self._max_width = 0
        self._max_height = 0
        self._sections: List[List[Section]] = [[]]

    async def initialize(
        self,
        sections: List[Section],
        width: int | None = None,
        height: int | None = None,
    ):
        terminal_size = await asyncio.to_thread(os.get_terminal_size)

        self._max_width = terminal_size[0]
        self._max_height = terminal_size[1]

        if width is None:
            width = math.floor(self._max_width / 2)

        if width > self._max_width:
            width = self._max_width

        if height is None or height > self._max_height:
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

            if next_width > self.width:
                self._sections.append([section])
                current_row_width = section.width
                row_idx += 1

            else:
                current_row_width += section.width
                self._sections[row_idx].append(section)

        for row in self._sections:
            row_height = max([section.height for section in row])

            for section in row:
                section.fit_height(row_height)

    async def render(self):
        canvas = await asyncio.gather(*[self._join_row(row) for row in self._sections])

        return "\n".join(canvas)

    async def _join_row(self, row: List[Section]):
        rendered_blocks = await asyncio.gather(*[section.render() for section in row])

        base = rendered_blocks[0]

        for segment in rendered_blocks[1:]:
            for idx, segment_row in enumerate(segment):
                base[idx] += segment_row

        return "\n".join(base)
