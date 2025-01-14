import asyncio
import math
from hyperscale.ui.config.mode import TerminalMode
from hyperscale.ui.config.widget_fit_dimensions import WidgetFitDimensions
from hyperscale.ui.styling import stylize, get_style
from typing import Any

from .header_config import HeaderConfig
from .font import (
    FormattedWord,
    Word,
)


class Header:
    def __init__(
        self,
        name: str,
        config: HeaderConfig,
        subscriptions: list[str] | None = None,
    ):
        self.fit_type = WidgetFitDimensions.X_Y_AXIS
        self.name = name

        if subscriptions is None:
            subscriptions = []

        self._config = config
        self.subscriptions = subscriptions

        self._word = Word(self._config.header_text)

        self._styled_header_lines: list[str] | None = None
        self._formatted_word: FormattedWord | None = None
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
        max_width: int | None = None,
        max_height: int | None = None,
    ):
        self._styled_header_lines = None

        self._formatted_word = self._word.to_ascii(
            formatter_set=self._config.formatters,
            max_width=max_width,
            max_height=max_height,
        )

        if max_height is None:
            max_height = self._formatted_word.height

        if max_width is None:
            max_width = self._formatted_word.width

        self._max_width = max_width
        self._max_height = max_height

    def _pad_word_width(self):
        padded_lines: list[str] = []
        for line in self._formatted_word.ascii_lines:
            difference = max(self._max_width - len(line), 0)

            if self._config.horizontal_alignment == "left":
                padded_lines.append(line + " " * difference)

            elif self._config.horizontal_alignment == "center":
                left_pad = " " * math.floor(difference / 2)
                right_pad = " " * math.floor(difference / 2)

                padded_lines.append(left_pad + line + right_pad)

            elif self._config.horizontal_alignment == "right":
                padded_lines.append(" " * difference + line)

        return FormattedWord(
            plaintext_word=self._formatted_word.plaintext_word,
            ascii="\n".join(padded_lines),
            ascii_lines=padded_lines,
            height=self._formatted_word.height,
            width=self._max_width,
        )

    def _pad_word_height(self):
        word_lines = self._formatted_word.ascii_lines

        difference = max(self._max_height - self._formatted_word.height, 0)

        if self._config.vertical_alignment == "top":
            word_lines = self._pad_bottom(word_lines, difference)

        elif self._config.vertical_alignment == "center":
            top_pad = math.floor(difference / 2)
            bottom_pad = math.floor(difference / 2)

            word_lines = self._pad_top(word_lines, top_pad)
            word_lines = self._pad_bottom(word_lines, bottom_pad)

        elif self._config.vertical_alignment == "bottom":
            word_lines = self._pad_top(word_lines, difference)

        return FormattedWord(
            plaintext_word=self._formatted_word.plaintext_word,
            ascii="\n".join(word_lines),
            ascii_lines=word_lines,
            height=self._max_height,
            width=self._formatted_word.width,
        )

    def _pad_top(
        self,
        word_lines: list[str],
        amount: int,
    ):
        for _ in range(amount):
            word_lines.insert(0, " " * self._max_width)

        return word_lines

    def _pad_bottom(
        self,
        word_lines: list[str],
        amount: int,
    ):
        word_lines.extend([" " * self._max_width for _ in range(amount)])

        return word_lines

    async def update(self, _: Any):
        pass

    async def get_next_frame(self):
        if self._styled_header_lines is None:
            self._styled_header_lines = await self._render()

        return self._styled_header_lines, False

    async def _render(self):
        if self._max_width > self._formatted_word.width:
            self._formatted_word = self._pad_word_width()

        if self._max_height > self._formatted_word.height:
            self._formatted_word = self._pad_word_height()

        styled_header_lines: list[str] = []
        for line in self._formatted_word.ascii_lines:
            styled_header_lines.append(
                await stylize(
                    line,
                    color=get_style(self._config.color),
                    highlight=get_style(self._config.highlight),
                    attrs=[get_style(attr) for attr in self._config.attributes]
                    if self._config.attributes
                    else None,
                    mode=self._mode,
                )
            )

        return styled_header_lines

    async def pause(self):
        pass

    async def resume(self):
        pass

    async def stop(self):
        pass

    async def abort(self):
        pass
