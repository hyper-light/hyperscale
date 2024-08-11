import functools
from typing import List, Sequence

from hyperscale.terminal.colors import (
    Attribute,
    AttributeName,
    Color,
    ColorName,
    Highlight,
    HighlightName,
    colorize,
)


class Text:
    def __init__(
        self,
        text: str,
        color: ColorName | None = None,
        highlight: HighlightName | None = None,
        attributes: List[AttributeName] | None = None,
    ) -> None:
        self._text = text
        self._styled: str | None = None
        self._color = color
        self._highlight = highlight
        self._attrs = self._set_attrs(attributes) if attributes else set()
        self._color_func = self._compose_color_func()

    def __str__(self):
        return self._styled or self._text

    async def style(self):
        if self._color or self._highlight:
            self._styled = await self._color_func(self._text)

        return self._styled or self._text

    @staticmethod
    def _set_color(value: str, default: int | None = None) -> str:
        if value not in Color.names and default is None:
            raise ValueError(
                "'{0}': unsupported color value. Use one of the: {1}".format(  # pylint: disable=consider-using-f-string
                    value, ", ".join(Color.names.keys())
                )
            )
        return Color.by_name(value, default=default)

    @staticmethod
    def _set_highlight(value: str, default: int | None = None) -> str:
        if value not in Highlight.names and default is None:
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
        self._color = self._set_color(value) if value else value
        self._color_func = self._compose_color_func()  # update

    @property
    def highlight(self) -> str | None:
        return self._highlight

    @highlight.setter
    def highlight(self, value: str) -> None:
        self._highlight = self._set_highlight(value) if value else value
        self._color_func = self._compose_color_func()  # update

    @property
    def attrs(self) -> Sequence[str]:
        return list(self._attrs)

    @attrs.setter
    def attrs(self, value: Sequence[str]) -> None:
        new_attrs = self._set_attrs(value) if value else set()
        self._attrs = self._attrs.union(new_attrs)
        self._color_func = self._compose_color_func()  # update

    def _compose_color_func(self):
        return functools.partial(
            colorize,
            color=self._color,
            highlight=self._highlight,
            attrs=list(self._attrs),
        )
