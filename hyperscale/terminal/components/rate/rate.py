import asyncio
from os import get_terminal_size
from typing import List, Literal, Sequence

from hyperscale.core_rewrite.engines.client.time_parser import TimeParser
from hyperscale.terminal.config.mode import TerminalMode
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

from .rate_config import RateConfig


class Rate:
    def __init__(
        self,
        config: RateConfig,
        color: ColorName | ExtendedColorName | None = None,
        highlight: HighlightName | ExtendedColorName | None = None,
        attributes: List[AttributeName] | None = None,
        mode: Literal["extended", "compatability"] = "compatability",
    ) -> None:
        self._count = 0
        self._rate_lock = asyncio.Lock()

        self._precision = config.precision
        self._unit = config.unit
        self._rate_unit = config.to_rate()
        self._rate_as_seconds = TimeParser(self._rate_unit).time
        self._last_elapsed: float | None = None

        self._styled: str | None = None
        self._color = color
        self._highlight = highlight
        self._mode = mode
        self._max_size: int | None = None
        self._base_size = self._precision + 1

        self._places_map = {"t": 1e12, "b": 1e9, "m": 1e6, "k": 1e3}

        if config.unit:
            self._base_size += len(config.unit) + 1

        self._attrs = self._set_attrs(attributes) if attributes else set()
        self._loop = asyncio.get_event_loop()

    def __str__(self):
        return self._styled or self._format_count()

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
        count_size = self._precision + 2
        max_size -= count_size

        if max_size is None:
            terminal_size = await self._loop.run_in_executor(None, get_terminal_size)
            max_size = terminal_size[0]

        self._max_size = count_size
        self._base_size = count_size

        if self._unit:
            max_size -= 1

            self._unit = self._unit[:max_size]

            unit_size = len(self._unit) + 1

            self._max_size += unit_size
            self._base_size += unit_size

    async def update(
        self,
        amount: int = 1,
    ):
        await self._rate_lock.acquire()

        self._count += amount

        self._rate_lock.release()

    async def get(self):
        return await self.get_next_frame()

    async def get_next_frame(self) -> str:
        await self._rate_lock.acquire()
        count = await self.style()
        self._rate_lock.release()

        return count

    async def style(
        self,
        color: ColorName | ExtendedColorName | None = None,
        highlight: HighlightName | ExtendedColorName | None = None,
        attrs: Sequence[str] | None = None,
        mode: Literal["extended", "compatability"] | None = None,
    ):
        count = self._format_count()

        if color is None:
            color = self._color

        if highlight is None:
            highlight = self._highlight

        if attrs is None:
            attrs = self._attrs

        if mode is None:
            mode = self._mode

        if self._unit:
            count = f"{count} {self._unit}"

        if color or highlight:
            self._styled = await stylize(
                count,
                color=color,
                highlight=highlight,
                attrs=attrs,
                mode=TerminalMode.to_mode(mode),
            )

        return self._styled or count

    def _format_count(self):
        selected_place_adjustment: int | None = None
        selected_place_unit: str | None = None

        sorted_places = sorted(
            self._places_map.items(),
            key=lambda adjustment: adjustment[1],
            reverse=True,
        )

        for place_unit, adjustment in sorted_places:
            if self._count / adjustment >= 1:
                selected_place_adjustment = adjustment
                selected_place_unit = place_unit

                break

        count = str(self._count)
        if selected_place_adjustment:
            count = f"%.{self._precision}g" % (self._count / selected_place_adjustment)

        count_size = len(count)
        precision_diff = self._precision - count_size

        if count_size < self._precision and selected_place_adjustment is not None:
            count = f"%.{precision_diff}f" % float(count)

        elif selected_place_adjustment is None:
            precision_diff = (self._precision + 1) - count_size
            count = f"%.{precision_diff}f" % float(count)

        if "." not in count:
            count += "."

        if selected_place_unit:
            count += selected_place_unit

        return str(count)

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
