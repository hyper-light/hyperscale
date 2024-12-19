import asyncio
import time
from os import get_terminal_size
from typing import List, Literal, Sequence, Tuple

from hyperscale.core_rewrite.engines.client.time_parser import TimeParser
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

from .windowed_rate_config import WindowedRateConfig


class WindowedRate:
    def __init__(
        self,
        config: WindowedRateConfig,
        color: ColorName | ExtendedColorName | None = None,
        highlight: HighlightName | ExtendedColorName | None = None,
        attributes: List[AttributeName] | None = None,
        mode: Literal["extended", "compatability"] = "compatability",
    ) -> None:
        self.fit_type = WidgetFitDimensions.X_AXIS

        self._counts: List[Tuple[int | float, float]] = []
        self._rate_lock = asyncio.Lock()

        self._precision = config.precision
        self._unit = config.unit
        self._rate_period_string = f"{config.rate_period}{config.rate_unit}"
        self._rate_as_seconds = TimeParser(self._rate_period_string).time
        self._start: float | None = None

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

        self._next_time: float | None = None
        self._start: float | None = None
        self._last_elapsed = 1
        self._last_count = 0

    def __str__(self):
        return self._styled or self._format_rate()

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
        numerator_units_length = 2
        denominator_units_lenght = 2
        count_size = (
            self._precision + numerator_units_length + denominator_units_lenght + 1
        )

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
        amount: int,
    ):
        await self._rate_lock.acquire()

        if self._start is None:
            self._start = time.monotonic()

        self._counts.append((amount, time.monotonic()))

        self._rate_lock.release()

    async def get(self):
        return await self.get_next_frame()

    async def get_next_frame(self) -> str:
        await self._rate_lock.acquire()

        if self._start is None:
            self._start = time.monotonic()

        current_time = time.monotonic()
        window = current_time - self._rate_as_seconds

        counts = [
            amount
            for amount, timestamp in self._counts
            if timestamp >= window and timestamp <= current_time
        ]

        if len(counts) > 0:
            self._last_count = sum(counts)

        else:
            self._last_count = 0

        self._last_elapsed = time.monotonic() - self._start
        self._start = time.monotonic()

        self._counts = [
            (amount, timestamp)
            for amount, timestamp in self._counts
            if timestamp > window
        ]

        rate = await self.style()
        self._rate_lock.release()

        return rate

    async def style(
        self,
        color: ColorName | ExtendedColorName | None = None,
        highlight: HighlightName | ExtendedColorName | None = None,
        attrs: Sequence[str] | None = None,
        mode: Literal["extended", "compatability"] | None = None,
    ):
        rate = self._format_rate()

        if color is None:
            color = self._color

        if highlight is None:
            highlight = self._highlight

        if attrs is None:
            attrs = self._attrs

        if mode is None:
            mode = self._mode

        if self._unit:
            rate = f"{rate} {self._unit}"

        rate = f"{rate}/{self._rate_period_string}"

        if color or highlight:
            self._styled = await stylize(
                rate,
                color=color,
                highlight=highlight,
                attrs=attrs,
                mode=TerminalMode.to_mode(mode),
            )

        return self._styled or rate

    def _format_rate(self):
        selected_place_adjustment: int | None = None
        selected_place_unit: str | None = None

        sorted_places = sorted(
            self._places_map.items(),
            key=lambda adjustment: adjustment[1],
            reverse=True,
        )

        if self._last_elapsed < 1:
            last_rate = self._last_count / (
                self._rate_as_seconds * (1 - self._last_elapsed)
            )

        else:
            last_rate = self._last_count / (self._rate_as_seconds * self._last_elapsed)

        adjustment_idx = 0
        for place_unit, adjustment in sorted_places:
            if last_rate / adjustment >= 1:
                selected_place_adjustment = adjustment
                selected_place_unit = place_unit
                adjustment_idx += 1
                break

        if selected_place_adjustment is None:
            selected_place_adjustment = 1

        full_rate = str(last_rate)
        full_rate_size = len(full_rate)
        rate = f"%.{self._precision}g" % (last_rate / selected_place_adjustment)
        rate_size = len(rate)

        if "." not in rate:
            rate += "."

        formatted_rate_size = len(rate)
        max_size = self._precision + 1

        if formatted_rate_size > max_size:
            rate = rate[: self._precision + 1]

        elif formatted_rate_size < max_size:
            current_digit = max_size
            while len(rate) < max_size:
                if current_digit < rate_size:
                    rate += full_rate[current_digit]

                else:
                    rate += "0"

                current_digit += 1

        if selected_place_unit:
            rate += selected_place_unit

        elif rate_size - 1 < full_rate_size:
            rate += full_rate[rate_size + 1]

        else:
            rate += "0"

        return rate

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
