from hyperscale.terminal.config.mode import TerminalMode
from hyperscale.terminal.styling import stylize
from typing import Any
from .header_text import (
    MEDI,
    LARG,
    XL,
)

from .header_config import HeaderConfig


class Header:
    def __init__(
        self,
        config: HeaderConfig,
    ):
        self._config = config
        self._headers: dict[tuple[int, int], str] = {
            (1, 3): "\033[1m\033[3m\033[4m hyperscale",
            (3, 5): MEDI,
            (5, 8): LARG,
            (8, 100): XL,
        }

        self._styled_header: str = ""
        self._max_width = 0
        self._max_height = 0
        self._mode = TerminalMode.to_mode(self._config.terminal_mode)

    @property
    def raw_size(self):
        return self._max_width

    @property
    def size(self):
        return self._max_width

    async def fit(
        self,
        max_width: int,
        max_height: int,
    ):
        selected_header: str = ""

        for size_range, Header in self._headers.items():
            min_size, max_size = size_range
            if max_height >= min_size and max_height < max_size:
                selected_header = Header

                break

        header_lines = [
            line[:max_width]
            for line in selected_header.split("\n")
            if len(line) > max_width
        ]

        header_lines = [
            line + " " * (max_width - len(line))
            for line in selected_header.split("\n")
            if len(line) < max_width
        ]

        self._styled_header = await stylize(
            "\n".join(header_lines), color=self._config.color, mode=self._mode
        )

        self._max_width = max_width
        self._max_height = max_height

    async def update(self, _: Any):
        pass

    async def get_next_frame(self):
        return self._styled_header

    async def pause(self):
        pass

    async def resume(self):
        pass

    async def stop(self):
        pass

    async def abort(self):
        pass
