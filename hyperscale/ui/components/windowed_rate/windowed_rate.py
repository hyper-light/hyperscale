import asyncio
import time
from typing import List

from hyperscale.core.engines.client.time_parser import TimeParser
from hyperscale.ui.config.mode import TerminalMode
from hyperscale.ui.config.widget_fit_dimensions import WidgetFitDimensions
from hyperscale.ui.styling import stylize, get_style

from .windowed_rate_config import WindowedRateConfig

Sample = tuple[int | float, float]


class WindowedRate:
    def __init__(
        self,
        name: str,
        config: WindowedRateConfig,
        subscriptions: list[str] | None = None,
    ) -> None:
        self.fit_type = WidgetFitDimensions.X_AXIS
        self.name = name

        if subscriptions is None:
            subscriptions = []

        self._config = config
        self.subscriptions = subscriptions

        self._precision = config.precision
        self._unit = config.unit
        self._last_samples: list[tuple[int | float, float]] = []

        self._rate_period_string = f"{config.rate_period}{config.rate_unit}"
        self._rate_as_seconds = TimeParser(self._rate_period_string).time

        self._places_map = {"t": 1e12, "b": 1e9, "m": 1e6, "k": 1e3}

        self._max_width: int | None = None
        self._windowed_rate_width = 0

        self._update_lock: asyncio.Lock | None = None
        self._updates: asyncio.Queue[Sample] | None = None

        self._refresh_start: float | None = None
        self._refresh_elapsed = 0

        self._mode = TerminalMode.to_mode(config.terminal_mode)
        self._last_frame: list[str] | None = None

    @property
    def raw_size(self):
        return self._windowed_rate_width

    @property
    def size(self):
        return self._windowed_rate_width

    async def fit(
        self,
        max_width: int | None = None,
    ):
        if self._update_lock is None:
            self._update_lock = asyncio.Lock()

        if self._updates is None:
            self._updates = asyncio.Queue()

        numerator_units_length = 2
        denominator_units_length = len(self._rate_period_string)
        count_size = (
            self._precision + numerator_units_length + denominator_units_length + 1
        )

        self._max_width = max_width
        windowed_rate_width = count_size

        if self._unit:
            space_between = 1
            unit_width = max_width - count_size - space_between

            self._unit = self._unit[:unit_width]
            unit_size = len(self._unit) + space_between

            windowed_rate_width += unit_size

        self._windowed_rate_width = windowed_rate_width
        self._updates.put_nowait([(0, time.monotonic())])

    async def update(
        self,
        samples: list[tuple[int | float, float]],
    ):
        await self._update_lock.acquire()

        self._updates.put_nowait(samples)

        self._update_lock.release()

    async def get_next_frame(self):
        if self._refresh_start is None:
            self._refresh_start = time.monotonic()

        samples = await self._check_if_should_rerender()

        rerender = False

        if samples:
            frame = await self._render(samples)
            self._last_frame = [frame]

            self._last_samples = samples
            rerender = True

        elif self._refresh_elapsed > self._rate_as_seconds or self._last_frame is None:
            frame = await self._render(self._last_samples)
            self._last_frame = [frame]
            rerender = True

            self._refresh_start = time.monotonic()
            self._refresh_elapsed = 0

        self._refresh_elapsed = time.monotonic() - self._refresh_start

        return self._last_frame, rerender

    async def _render(self, samples: List[Sample]):
        current_time = time.monotonic()
        window = current_time - self._rate_as_seconds

        count = sum(
            [
                amount
                for amount, timestamp in samples
                if timestamp >= window and timestamp <= current_time
            ]
        )

        rate = self._format_rate(count)

        rate = rate + "/" + self._rate_period_string

        formatted_rate = await stylize(
            rate,
            color=get_style(
                self._config.color,
                count,
                window,
            ),
            highlight=get_style(
                self._config.highlight,
                count,
                window,
            ),
            attrs=[
                get_style(
                    attr,
                    count,
                    window,
                )
                for attr in self._config.attributes
            ]
            if self._config.attributes
            else None,
        )

        return formatted_rate

    def _format_rate(self, count: int):
        selected_place_adjustment: int | None = None
        selected_place_unit: str | None = None

        sorted_places = sorted(
            self._places_map.items(),
            key=lambda adjustment: adjustment[1],
            reverse=True,
        )

        rate_amount = float(count)

        adjustment_idx = 0
        for place_unit, adjustment in sorted_places:
            if rate_amount / adjustment >= 1:
                selected_place_adjustment = adjustment
                selected_place_unit = place_unit
                adjustment_idx += 1
                break

        if selected_place_adjustment is None:
            selected_place_adjustment = 1

        full_rate = str(rate_amount)
        full_rate_size = len(full_rate)
        rate = f"%.{self._precision}g" % (rate_amount / selected_place_adjustment)
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

        elif rate_size + 1 < full_rate_size:
            rate += full_rate[rate_size + 1]

        else:
            rate += "0"

        if self._unit:
            rate = f"{rate} {self._unit}"

        return rate

    async def _check_if_should_rerender(self):
        await self._update_lock.acquire()

        data: List[Sample] | None = None

        if self._updates.empty() is False:
            data = await self._updates.get()

        self._update_lock.release()

        return data

    async def pause(self):
        pass

    async def resume(self):
        pass

    async def stop(self):
        if self._update_lock.locked():
            self._update_lock.release()

    async def abort(self):
        if self._update_lock.locked():
            self._update_lock.release()
