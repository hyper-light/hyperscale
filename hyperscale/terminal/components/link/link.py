import asyncio
from os import get_terminal_size
from typing import List, Literal, Sequence

from hyperscale.terminal.config.mode import TerminalMode
from hyperscale.terminal.config.widget_fit_dimensions import WidgetFitDimensions
from hyperscale.terminal.styling import stylize
from hyperscale.terminal.styling.attributes import (
    Attribute,
    AttributeName,
)
from hyperscale.terminal.styling.colors import (
    Color,
    ColorName,
    ExtendedColorName,
    Highlight,
    HighlightName,
)

from .link_validator import LinkValidator


class Link:
    def __init__(
        self,
        text: str,
        url: str,
        color: ColorName | ExtendedColorName | None = None,
        highlight: HighlightName | ExtendedColorName | None = None,
        attributes: List[AttributeName] | None = None,
        mode: Literal["extended", "compatability"] = "compatability",
    ) -> None:
        self.fit_type = WidgetFitDimensions.X_AXIS

        self._text = text
        self._link = LinkValidator(url=url)
        self._max_size: int | None = None
        self._styled: str | None = None
        self._color = color
        self._highlight = highlight
        self._mode = mode
        self._attrs = self._set_attrs(attributes) if attributes else set()
        self._loop = asyncio.get_event_loop()
        self._base_size = len(text)

    def __str__(self):
        return self._styled or "\x1b]8;;%s\x1b\\%s\x1b]8;;\x1b\\" % (
            str(self._link.url),
            self._text,
        )

    @property
    def raw_size(self):
        return self._base_size

    @property
    def size(self):
        return self._base_size

    async def fit(
        self,
        max_size: int | None = None,
    ):
        if max_size is None:
            terminal_size = await self._loop.run_in_executor(None, get_terminal_size)
            max_size = terminal_size[0]

        self._text = self._text[:max_size]
        self._max_size = max_size
        self._base_size = len(self._text)

    async def get_next_frame(self) -> str:
        return await self.style()

    async def style(
        self,
        color: ColorName | ExtendedColorName | None = None,
        highlight: HighlightName | ExtendedColorName | None = None,
        attrs: Sequence[str] | None = None,
        mode: Literal["extended", "compatability"] | None = None,
    ):
        if color is None:
            color = self._color

        if highlight is None:
            highlight = self._highlight

        if attrs is None:
            attrs = self._attrs

        if mode is None:
            mode = self._mode

        link_fmt_str = "\x1b]8;;%s\x1b\\%s\x1b]8;;\x1b\\"

        if color or highlight:
            styled = await stylize(
                self._text,
                color=color,
                highlight=highlight,
                attrs=attrs,
                mode=TerminalMode.to_mode(mode),
            )

            self._styled = link_fmt_str % (str(self._link.url), styled)

        return self._styled or link_fmt_str % (str(self._link.url), self._text)

    @staticmethod
    def _set_color(
        value: str,
        default: int | None = None,
        mode: TerminalMode = TerminalMode.COMPATIBILITY,
    ) -> str:
        if (
            value not in Color.names
            or (value in Color.names and mode == TerminalMode.EXTENDED)
            and default is None
        ):
            raise ValueError(
                "'{0}': unsupported color value. Use one of the: {1}".format(  # pylint: disable=consider-using-f-string
                    value, ", ".join(Color.names.keys())
                )
            )
        return Color.by_name(value, default=default)

    @staticmethod
    def _set_highlight(
        value: str,
        default: int | None = None,
        mode: TerminalMode = TerminalMode.COMPATIBILITY,
    ) -> str:
        if (
            value not in Highlight.names
            or (value not in Color.extended_names and mode == TerminalMode.EXTENDED)
            and default is None
        ):
            raise ValueError(
                "'{0}': unsupported highlight value. "  # pylint: disable=consider-using-f-string
                "Use one of the: {1}".format(value, ", ".join(Highlight.names.keys()))
            )
        return Highlight.by_name(value, default=default)

    @staticmethod
    def _set_attrs(attrs: Sequence[str]) -> set[str]:
        for attr in attrs:
            if attr not in Attribute.names:
                raise ValueError(
                    "'{0}': unsupported attribute value. "  # pylint: disable=consider-using-f-string
                    "Use one of the: {1}".format(
                        attr, ", ".join(Attribute.names.keys())
                    )
                )
        return set(attrs)

    @property
    def color(self) -> str | None:
        return self._color

    @color.setter
    def color(self, value: str) -> None:
        self._color = self._set_color(value, mode=self._mode) if value else value

    @property
    def highlight(self) -> str | None:
        return self._highlight

    @highlight.setter
    def highlight(self, value: str) -> None:
        self._highlight = (
            self._set_highlight(value, mode=self._mode) if value else value
        )

    @property
    def attrs(self) -> Sequence[str]:
        return list(self._attrs)

    @attrs.setter
    def attrs(self, value: Sequence[str]) -> None:
        new_attrs = self._set_attrs(value) if value else set()
        self._attrs = self._attrs.union(new_attrs)

    async def stop(self):
        pass

    async def abort(self):
        pass
